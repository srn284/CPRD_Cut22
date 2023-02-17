import random
from CPRD.functions import tables, merge
import pyspark.sql.functions as F
from CPRD.config.utils import *
from CPRD.functions.modalities import *
from pyspark.sql import Window
from CPRD.config.spark import read_parquet
from typing import Any
import datetime, shutil
import os
from pyspark.sql.types import IntegerType


class PredBase:
    def __init__(self, follow_up_duration_month, time_to_event_mark_default=-1):
        self.follow_up_duration = follow_up_duration_month
        self.time2eventMarkDefault = time_to_event_mark_default

    def define_label(self, demographics, source, condition, column, death):
        """
        this function captures label
        demographics is the patient eligible for analyses
        source - the records of the patients - this can be medications /diagnoses/etc
        column - the column of the source that has the codes (e.g., might be "code" or "ICD")
        death - the death dataframe
        """
        demographics = self.get_label_from_records(demographics, source=source, condition=condition, column=column)
        label_defined = demographics.filter(F.col('label').isin(*[0, 1]))
        label_unclear = demographics.filter(F.col('label').isin(*[0, 1]) == False)
        label_mark = self.get_label_from_death_registration(label_unclear, death, condition)
        demographics = label_defined.union(label_mark)
        return demographics

    def exclusion_inclusion_record(self, demographics, criteria, source, column, exclusion=True):
        """
        this function is to check the if patient should be included or excluded because contains certain
        condition in the history

        demographics provided patient id, study entry date
        criteria will indicate the condition (codes) to be excluded
        source is the data modalities been used for checking the exclusion criteria
        column is the column name in the source for checking the inclusion exclusion criteria
        exclusion is ture means we do exclusion for patients who satisfy the criteria
        """

        # select code in the source that is in the criteria list
        source = source.filter(F.col(column).isin(*criteria)).withColumnRenamed(column, "code").select(
            ['patid', 'eventdate', 'code'])

        # inner join demographics and source to include patients who has those code in the criteria
        subset = demographics.join(source, 'patid', 'inner')

        # keep codes that before the study_entry and keep the patid
        subset = subset.where(F.col('eventdate') <= F.col('study_entry')).withColumn('redundant', F.lit(1)) \
            .select(['patid', 'redundant']).dropDuplicates()

        if exclusion is False:
            demographics = demographics.join(subset, 'patid', 'inner').drop('redundant')
        else:
            demographics = demographics.join(subset, 'patid', 'left')
            demographics = demographics.filter(F.col('redundant').isNull()).drop('redundant')

        return demographics, subset

    def _check_follow_up_duration(self, demographics):
        """
        1. identify positive cases before the end of follow up, and calculate time to event
        2. identify negative cases that has end date after the end of follow up, assign time to event to a default value
        """
        # set up the end of follow up date in theory
        demographics = demographics.withColumn('endFollowUp', demographics.study_entry +
                                               F.expr('INTERVAL {} MONTHS'.format(self.follow_up_duration)))

        # if an event happens before the end of follow up, then a positive case is identified
        positivePOSSIBLE = demographics.where(F.col('eventdate') <= F.col('endFollowUp'))

        positive = positivePOSSIBLE.where(F.col('eventdate') <= F.col('enddate')).withColumn('label', F.lit(1))
        moreNEG = positivePOSSIBLE.where(F.col('eventdate') > F.col('enddate')).withColumn('label', F.lit(0))
        time2eventdiff2 = F.unix_timestamp('enddate', "yyyy-MM-dd") - F.unix_timestamp('study_entry', "yyyy-MM-dd")

        time2eventdiff = F.unix_timestamp('eventdate', "yyyy-MM-dd") - F.unix_timestamp('study_entry', "yyyy-MM-dd")
        positive = positive.withColumn('time2event', time2eventdiff) \
            .withColumn('time2event', (F.col('time2event') / 3600 / 24 / 30).cast('integer')) \
            .select(['patid', 'label', 'time2event'])

        # if an event happens after the follow up, then it is a negative patient
        negtive_1 = demographics.where(F.col('eventdate') > F.col('endFollowUp')).withColumn('label', F.lit(0))
        negtive_1BEFORE = negtive_1.where(F.col('endFollowUp') <= F.col('enddate')).withColumn('time2event', F.lit(
            self.time2eventMarkDefault)).select(['patid', 'label', 'time2event'])
        negtive_1AFTER = negtive_1.where(F.col('endFollowUp') > F.col('enddate')).withColumn('time2event',
                                                                                             time2eventdiff2) \
            .withColumn('time2event', (F.col('time2event') / 3600 / 24 / 30).cast('integer')) \
            .select(['patid', 'label', 'time2event'])

        moreNEG = moreNEG.withColumn('time2event', time2eventdiff2) \
            .withColumn('time2event', (F.col('time2event') / 3600 / 24 / 30).cast('integer')) \
            .select(['patid', 'label', 'time2event'])

        # the identified patients are those that are definitely positive and negative (due to having event after follow-up)
        identified = positive.union(negtive_1AFTER)
        identified = identified.union(moreNEG)
        identified = identified.union(negtive_1BEFORE)

        # get anti left patients - unclear is the ones which are not pos or neg (due to event after followup)
        # more negative patients where the followup happens before enddate. technically censored in time2event framework
        # but not in our binary prediction framework
        unclear = demographics.alias('a').join(identified.alias('b'), F.col("a.patid") == F.col("b.patid"), 'left') \
            .filter(F.col("b.patid").isNull()).select('a.*')
        negative = unclear.where(F.col('endFollowUp') < F.col('enddate')).withColumn('label', F.lit(0)) \
            .withColumn('time2event', F.lit(self.time2eventMarkDefault)).select(['patid', 'label', 'time2event'])

        # combine both identified positive and negative patients
        # null label is patients who have records less than the follow up period, need to further identify if
        # the patient is dead or not - and if event happens in death (not HF but anything else is fair game)
        label = identified.union(negative)
        demographics = demographics.join(label, 'patid', 'left')
        return demographics

    def get_label_from_records(self, demographics, source, condition, column='code', incidence=True,
                               prevalent_conditions=None):
        """
        identify label for patients from the records using the source dataframe and condition list provided
        demographics includes study entry date and when records end
        source is the modality required to retrieve label (e.g. diagnoses)
        condition is a list of code for case identification
        column is the column in source for identifying cases
        incidence is a boolean where True means that incidence outcome is needed in the outcome ascertainment. And the prevalent outcome
            patients will be removed from dataset. False - allows prevalent condition ascertainment
        prevalent_conditions - while incidence is False only, this matters. In this case - the codes provided here that
            are subset of the codes in the condition parameter and are the codes for which prevalent disease outcome prediction
            are conducted.

        """

        # keep records that belongs to a condtion provided by condition list
        source = source.filter(F.col(column).isin(*condition)).select(['patid', 'eventdate', column])

        # take first of the eventdate by patid

        if incidence:
            w = Window.partitionBy('patid').orderBy('eventdate')
            source_first = source.withColumn(column, F.first(column).over(w)).groupBy('patid').agg(
                F.min('eventdate').alias('eventdate'),
                F.first(column).alias(column)
            )

            # remove patients that having incidence before the study entry
            # demographics includes column + event date
            demographics = demographics.join(source_first, 'patid', 'left')
            demographics = demographics.drop(column)
            #             print(demographics.show())
            exclude = demographics.where(F.col('eventdate') <= F.col('study_entry'))
            # anti left join
            demographics = demographics.alias('a').join(exclude.select(['patid', 'eventdate']).alias('b'),
                                                        F.col("a.patid") == F.col("b.patid"),
                                                        'left') \
                .filter(F.col("b.patid").isNull()).select('a.*')

        elif incidence== False:
            if prevalent_conditions is None:
                raise ValueError(
                    "Not fully incidence as stated in the parameter, prevalent_conditions='some' but no exceptions provided. please provide excpetions in prevalent_conditions")

            else:
                # exclude those with conditions in the past BUT not in the inncidence_exceptions
                #  if patients have any disease BUT prevalent_conditions, throw them out

                sourceOrig = source

                demorawcolumns = [colsdemo for colsdemo in demographics.columns if
                                  colsdemo not in ['eventdate', 'code']]
                originalDemo4Backup = demographics.select(demorawcolumns)

                demographics = demographics.join(source, 'patid', 'left')

                excludePotential = demographics.where(F.col('eventdate') <= F.col('study_entry'))
                includeterms = prevalent_conditions
                exclude = excludePotential.filter(~ F.col(column).isin(*includeterms))
                keep = excludePotential.alias('a').join(exclude.select(['patid', 'eventdate']).alias('b'),
                                                        F.col("a.patid") == F.col("b.patid"),
                                                        'left').filter(F.col("b.patid").isNull()).select('a.*')
                keep = keep.select(['patid', 'study_entry']).dropDuplicates()

                excludePotentialPats = excludePotential.select(['patid', 'study_entry']).dropDuplicates()
                demographics = demographics.alias('a').join(excludePotentialPats.select(['patid']).alias('b'),
                                                            F.col("a.patid") == F.col("b.patid"),
                                                            'left').filter(F.col("b.patid").isNull()).select('a.*')

                demographicsPats = demographics.select(['patid']).dropDuplicates()
                demographicsDates = demographics.select(['patid', 'eventdate', column, 'study_entry'])
                w = Window.partitionBy('patid').orderBy('eventdate')
                demographicsDates = demographicsDates.withColumn(column, F.first(column).over(w)).groupBy('patid').agg(
                    F.min('eventdate').alias('eventdate'),
                    F.first(column).alias(column),
                    F.first('study_entry').alias('study_entry'),

                )

                demographicsDates = demographicsDates.drop(column)
                # in case
                demographicsDates = demographicsDates.where((F.col('eventdate') > F.col('study_entry')))
                demographicsDates = demographicsDates.drop(column).drop('study_entry')
                demographics = demographicsPats.join(demographicsDates, 'patid', 'left')

                w = Window.partitionBy('patid').orderBy('eventdate')

                # in the keep patients, only look at records after study entry and see if they have label post study entry
                demographics_4keep_wSource = keep.join(sourceOrig, 'patid', 'left')
                demographics_4keep_wSourcePOS = demographics_4keep_wSource.where(
                    (F.col('eventdate') > F.col('study_entry')))
                demographics_4keep_wSourcePOS = demographics_4keep_wSourcePOS.withColumn(column, F.first(column).over(
                    w)).groupBy('patid').agg(
                    F.min('eventdate').alias('eventdate'),
                    F.first(column).alias(column),
                    F.first('study_entry').alias('study_entry')

                )

                demographics_4keep_wSourcePOS = demographics_4keep_wSourcePOS.drop(column)

                demographics_4keep_wSourceNEG = keep.alias('a').join(
                    demographics_4keep_wSourcePOS.select(['patid', 'eventdate']).alias('b'),
                    F.col("a.patid") == F.col("b.patid"),
                    'left').filter(F.col("b.patid").isNull()).select(
                    'a.*')

                demographics_4keep_wSource = demographics_4keep_wSourcePOS.select(['patid']).union(
                    demographics_4keep_wSourceNEG.select(['patid']))
                demographics_4keep_wSource = demographics_4keep_wSource.join(demographics_4keep_wSourcePOS, 'patid',
                                                                             'left').drop('study_entry')

                demographics = demographics.join(originalDemo4Backup, 'patid', 'left')
                demographics_4keep_wSource = demographics_4keep_wSource.join(originalDemo4Backup, 'patid', 'left')
                # union with larger demographics which was free of potentialyl exlcuded patients
                demographics_4keep_wSource = demographics_4keep_wSource.select(demographics.columns)

                demographics = demographics.union(demographics_4keep_wSource)
        # assign label based on the follow up records

        demographics = self._check_follow_up_duration(demographics)
        return demographics

    def get_label_from_death_registration(self, demographics, death, condition):
        """
                for patients who are not positive cases (identified from records), and do not have
                records long enough over the duration to be identified as negative patients,
                i.e. enddate < follow-up date in theory
                we check death registration, if death is caused by condition, we identify them as positive,
                otherwise, we exclude them
                """

        hfconditions = ['I110', 'I130', 'I50', 'I132']
        # no hf in death
        condition = [xx for xx in condition if xx not in hfconditions]
        # thus the removal of hf codes above^
        for each in ['label', 'time2event']:
            if each in demographics.columns:
                demographics = demographics.drop(each)

        # keep death record with patid in the demographics
        patid = demographics.select(['patid'])
        death = patid.join(death, 'patid', 'inner')

        # keep death cause that belongs to condition and set up label to positive
        cause_cols = ['cause', 'cause1', 'cause2', 'cause3', 'cause4', 'cause5', 'cause6', 'cause7',
                      'cause8', 'cause9', 'cause10', 'cause11', 'cause12', 'cause13', 'cause14', 'cause15']
        cause_cols = [F.col(each) for each in cause_cols]
        death = death.withColumn("cause", F.array(cause_cols)).select(['patid', 'cause'])
        rm_dot = F.udf(lambda x: x.replace(".", ""))
        death = death.withColumn('cause', F.explode('cause')) \
            .withColumn('cause', rm_dot('cause')) \
            .filter(F.col('cause').isin(*condition))
        death = death.groupBy('patid').agg(F.first('cause').alias('cause')) \
            .withColumn('label', F.lit(1)).select(['patid', 'label'])
        # join death with demographics and calculate time2event

        time2eventdiff = F.unix_timestamp('enddate', "yyyy-MM-dd") - F.unix_timestamp('study_entry', "yyyy-MM-dd")
        demographics = demographics.join(death, 'patid', 'left').withColumn('time2event', time2eventdiff) \
            .withColumn('time2event', (F.col('time2event') / 3600 / 24 / 30).cast('integer'))
        demographicsNULL = demographics.filter(F.col('label').isNull())

        demographicsNULL = demographicsNULL.withColumn('label', F.lit(0))

        demographicsNOTNULL = demographics.filter(F.col('label').isNotNull())

        demographics = demographicsNULL.union(demographicsNOTNULL)
        return demographics

    def set_label(self, demographics, source, condition, check_death=True, death=None, columns='code', incidence=True,
                  prevalent_conditions=None):
        demographics = self.get_label_from_records(demographics=demographics, source=source, condition=condition,
                                                   column=columns, incidence=incidence,
                                                   prevalent_conditions=prevalent_conditions)

        label_defined = demographics.filter(F.col('label').isin(*[0, 1]))

        if check_death:
            label_unclear = demographics.filter(F.col('label').isNull())
            label_define_death = self.get_label_from_death_registration(demographics=label_unclear, death=death,
                                                                        condition=condition)
            demographics = label_defined.union(label_define_death)
        else:
            label_unclear = demographics.filter(F.col('label').isNull())
            label_define_death = self.get_label_from_death_registration(demographics=label_unclear, death=death,
                                                                        condition=['_garbage'])
            demographics = label_defined.union(label_define_death)

        return demographics


class OutcomePrediction(PredBase):
    def __init__(self, label_condition, exclusion_codes=None, duration=(1985, 2021),
                 follow_up_duration_month=60, time_to_event_mark_default=-1):
        super().__init__(follow_up_duration_month, time_to_event_mark_default)
        """
        label_condition, exclusion_codes  are dictionary with key: condition name, value: condition code
        {"hypertension": ['I10']}
        duration is the duration of study,
        follow_up_duration_month is the maximum follow up duration in months
        time_to_event_mark_default is mark as -1 if no event and lasts till end of follow-up

        """
        self.label_condition = label_condition
        self.exclusion_codes = exclusion_codes
        self.duration = duration

    def exclusion_inclusion_diagnosis_medication(self, demographics, source=None, source_col='code',
                                                 exclusion_source=True):
        # exclude patients if having diagnosis the exclusion_diagnosis
        _ = None
        if self.exclusion_codes is not None:
            if source is None:
                raise ValueError("exclusion criteria for proper source is not None, provide source (i.e., diag, meds, etc) dataframe")
            else:
                for diag, code in self.exclusion_codes.items():
                    demographics, _ = self.exclusion_inclusion_record(demographics, code, source, source_col,
                                                                      exclusion=exclusion_source)



        return demographics, _

    def pipeline(self, demographics=None, source=None,  exclusion_source=True, check_death=True, death=None,
                 column_condition='code', incidence=True, prevalent_conditions=None):
        """
        :param demographics: the cohort file with sutdy entry etc
        :param source: the source table for modalitiies - e.g. the diag table
        :param exclusion_source: True if we want to exclude based on past diags
        :param check_death: True if we are to check death
        :param death: death table (none if above is false)
        :param column_condition: column that has the diags or meds or whatever modality we are wanting to look for label
        :param incidence: True if we are looking for incident lable
        :param prevalent_conditions: if incidence is false, then what are some prevalent conditions we are allowing to look for (a subset of the labels)
        :return: cohort file with baseline, and label, and time2event as well
        """


        demographics, _ = self.exclusion_inclusion_diagnosis_medication(demographics, source, column_condition,
                                                                        exclusion_source)
        # set up label for demographics
        demographics = self.set_label(demographics, source, self.label_condition, check_death, death=death,
                                      columns=column_condition, incidence=incidence,
                                      prevalent_conditions=prevalent_conditions)
        return demographics

    def forward(self):
        raise NotImplementedError
