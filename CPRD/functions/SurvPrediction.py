import random
from CPRD.functions import tables, merge
import pyspark.sql.functions as F
from CPRD.config.utils import *
from CPRD.functions.modalities import *
from pyspark.sql import Window
from CPRD.config.spark import read_parquet
from typing import Any
import datetime
from pyspark.sql.types import IntegerType
# many thanks to YLi for this writeup

class SurvRiskPredictionBase:
    def __init__(self, follow_up_duration_month):
        self.follow_up_duration = follow_up_duration_month

    def setupEventAndTime(self, demographics, source, condition, column, death=None):
        """
        demographics: study_entry (baseline), startdate (earliest available date), enddate (last collect/dod)
        source: patid, eventdate, code
        """
        # retrive the death condition if not None
        if death is not None:
            death = self.prepareDeathCause(death, condition, column)
            source = source.union(death)

        # keep records that belongs to a condtion provided by condition list
        source = source.filter(F.col(column).isin(*condition)).select(['patid', 'eventdate', column])

        # take first of the eventdate by patid
        w = Window.partitionBy('patid').orderBy('eventdate')
        source = source.withColumn(column, F.first(column).over(w)).groupBy('patid').agg(
            F.min('eventdate').alias('eventdate'),
            F.first(column).alias(column)
        )

        # demographic has ['startdate', 'enddate', 'eventdate', 'endfollowupdate']
        demographics = demographics.join(source, 'patid', 'left').drop(column)
        demographics = demographics.withColumn('endfollowupdate', demographics.study_entry +
                                               F.expr('INTERVAL {} MONTHS'.format(self.follow_up_duration))).cache()

        # add event and time
        # 1. separate into patient_with_event and patient_no_event
        # 2. patient no event, event = 0 time = F.least(enddate, endfollowupdate)
        # 3. oatient with event:
        #   (1) event before study entry -> excluded
        #   (2) event after endfollowup -> event = 0, time = endfollowupdate
        #   (3) event between study entry and endfollowup -> event = 1, time = eventdate
        demographics_no_event = demographics.filter(F.col('eventdate').isNull())\
            .withColumn('label', F.lit(0)).withColumn('time2event', F.least(F.col('enddate'), F.col('endfollowupdate')))

        demographics_with_event = demographics.filter(F.col('eventdate').isNotNull())\
            .filter(F.col('eventdate') > F.col('study_entry')).cache()
        demographics_with_event_a = demographics_with_event.filter(F.col('eventdate') > F.col('endfollowupdate'))\
            .withColumn('label', F.lit(0)).withColumn('time2event', F.col('eventdate'))
        demographics_with_event_b = demographics_with_event.\
            filter((F.col('eventdate') < F.col('endfollowupdate')) & (F.col('eventdate') > F.col('study_entry')))\
            .withColumn('label', F.lit(1)).withColumn('time2event', F.col('eventdate'))

        # 'startdate', 'enddate', 'eventdate', 'label', 'time2event'
        demographics = demographics_no_event.union(demographics_with_event_a).union(demographics_with_event_b)\
            .drop('endfollowupdate')

        # transform time2event from 'date' to 'months'
        time2eventdiff = F.unix_timestamp('time2event', "yyyy-MM-dd") - F.unix_timestamp('study_entry', "yyyy-MM-dd")
        demographics = demographics.withColumn('time2event', time2eventdiff)\
            .withColumn('time2event', (F.col('time2event') / 3600 / 24 / 30).cast('integer'))
        return demographics

    def prepareDeathCause(self, death, condition, column):
        cause_cols = ['cause', 'cause1', 'cause2', 'cause3', 'cause4', 'cause5', 'cause6', 'cause7',
                      'cause8', 'cause9', 'cause10', 'cause11', 'cause12', 'cause13', 'cause14', 'cause15']
        cause_cols = [F.col(each) for each in cause_cols]
        death = death.withColumn("cause", F.array(cause_cols)).select(['patid', 'cause', 'dod'])
        rm_dot = F.udf(lambda x: x.replace(".", ""))
        death = death.withColumn('cause', F.explode('cause')) \
            .withColumn('cause', rm_dot('cause')) \
            .filter(F.col('cause').isin(*condition))
        death = death.groupBy('patid').agg(F.first('dod').alias('eventdate'), F.first('cause').alias(column))
        return death