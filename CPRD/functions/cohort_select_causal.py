import random
from CPRD.functions import tables, merge
import pyspark.sql.functions as F
from CPRD.config.utils import cvt_str2time
from typing import Any
import datetime
from pyspark.sql.types import IntegerType
from CPRD.functions.modalities import *
from pyspark.sql import Window
import shutil
from CPRD.config.spark import read_parquet

class CausalCohort:
    def __init__(self, least_year_register_gp, least_age, greatest_age, imdReq=True):
        self.least_year_register_gp = least_year_register_gp
        self.least_age = least_age
        self.greatest_age = greatest_age
        self.imdReq = imdReq

    def retrieve_eligible_patid(self, patient, practice, eligible, linkage = True, practiceLink=True):
        demographics = tables.retrieve_demographics(patient=patient, practice=practice, practiceLink=practiceLink)

        if linkage==True:
            eligible = eligible.select('patid')
            demographics = demographics.join(eligible, on='patid', how='inner')
        return demographics

    def date_least_year_register_gp(self, demographics, death):
        time = merge.retrieve_eligible_time(demographics=demographics, death=death, return_dod=True)
        time = time.withColumn('least_gp_register_date',
                               time.startdate + F.expr('INTERVAL {} YEAR'.format(self.least_year_register_gp)))
        demographics = demographics.join(time, on='patid', how='inner') \
            .drop(demographics.enddate).drop(demographics.startdate)
        return demographics

    def date_least_age(self, demographics):
        # set dob to be the middle of the year
        demographics = demographics.withColumn('dob', F.concat(F.col('yob'), F.lit('0701')))
        demographics = demographics.withColumn('dob', cvt_str2time(demographics, 'dob'))
        demographics = demographics.withColumn('{}_dob'.format(self.least_age),
                                               demographics.dob + F.expr('INTERVAL {} YEARS'.format(self.least_age)))
        return demographics

    def date_greatest_age(self, demographics):
        # set dob to be the middle of the year
        demographics = demographics.withColumn('dob', F.concat(F.col('yob'), F.lit('0701')))
        demographics = demographics.withColumn('dob', cvt_str2time(demographics, 'dob'))
        demographics = demographics.withColumn('{}_dob'.format(self.greatest_age),
                                               demographics.dob + F.expr('INTERVAL {} YEARS'.format(self.greatest_age)))
        return demographics

    def standard_prepare(self, file, spark, linkage=True, practiceLink=True):
        """
        return dataframe contains information about records start and end date, the date for at least N year after
        registration with GP, and the date for a patient at Y years old for futher cohort selection

        return least_gp_register_date, {}_dob {} is the age pre-defined
        """

        patient = tables.retrieve_patient(dir=file['patient'], spark=spark)
        practice = tables.retrieve_practice(dir=file['practice'], spark=spark)
        eligible = tables.retrieve_link_eligible(dir=file['eligible'], spark=spark)
        death = tables.retrieve_death(dir=file['death'], spark=spark)

        demographics = self.retrieve_eligible_patid(patient, practice, eligible, linkage, practiceLink)
        demographics = self.date_least_year_register_gp(demographics, death)
        demographics = self.date_least_age(demographics)
        demographics = self.date_greatest_age(demographics)
        if self.imdReq:
            print('Imd Processing...')
            imd = retrieve_imd(file, spark)
            demographics = demographics.join(imd, 'patid', 'inner')

        return demographics

    def pipeline(self, *args, **kwargs) -> Any:
        r"""
        Same as :meth:`torch.nn.Module.forward()`.

        Args:
            *args: Whatever you decide to pass into the forward method.
            **kwargs: Keyword arguments are also possible.

        Return:
            Your model's output
        """
        raise NotImplementedError



class CohortSoftCut(CausalCohort):
    def __init__(self, least_year_register_gp, least_age, greatest_age, exposure, imdReq=True, linkage=True, practiceLink=True):
        super().__init__(least_year_register_gp, least_age, greatest_age, imdReq)

        # exposure is shown as tuple: (format = 'prodcode','ICD10','medcode', 'OPCS')
        self.exposure = exposure
        self.linkage = linkage
        self.practiceLink = practiceLink
    def demoExtract(self, file, spark, duration=('1995-01-01', '2010-01-01')):

        demographics = self.standard_prepare(file, spark, self.linkage, self.practiceLink)


        # causal soft
        demographics = demographics.withColumn('study_entry', F.to_date(F.lit(duration[0])))
        # study entry is greatest of three as described above
        demographics = demographics.withColumn('study_entry_real',
                                               F.greatest('{}_dob'.format(self.least_age), 'least_gp_register_date',
                                                          'study_entry')) \
            .drop('study_entry').withColumnRenamed("study_entry_real", "study_entry")
        demographics = demographics.withColumn('exit_date', F.to_date(F.lit(duration[1])))

        demographics = demographics.withColumn('exit_datereal', F.least(F.col('exit_date'), F.to_date(F.lit(duration[1])),
                                                              F.col('{}_dob'.format(self.greatest_age)))).drop('exit_date').withColumnRenamed('exit_datereal', 'exit_date')

        # last start of study is the second element of the duration (the finish date)
        # requirement of the start of study before the last date (enddate)
        demographics = demographics.where(F.col('study_entry') < F.col('enddate'))
        demographics = demographics.where(F.col('study_entry') < F.col('exit_date'))


        return demographics

    def pipeline(self, file, spark, duration=('1995-01-01', '2010-01-01'), randomNeg=True, sourceT=None, sourceCol = None, rollingTW=-1):
        """
        random select baseline date between the start and end
        start: greatest of dob, gp registration, study start date (duration[0])
        end: least of end date, duration
        other criteria, the start date is smaller than the end date

        duration in (year-month-date) format
        """
        demographics = self.demoExtract(file, spark, duration)

        demographics = self.extractionExposure(file, spark, duration, demographics, sourceT=sourceT, sourceCol=sourceCol,rollingTW=rollingTW )

        demographics = self.randomizeNeg(file, spark, demographics, randomNeg)
        demographics = demographics.drop('study_entry'). withColumnRenamed('eventdate', 'study_entry')
        return demographics

    def randomizeNeg(self, file, spark, demographics, randomNeg=True):
        # random generate a date between start and end as baseline
        pos = demographics.filter(F.col('exp_label') == 1)
        neg = demographics.filter(F.col('exp_label') == 0)
        outputCols = ['patid', 'region', 'eventdate', 'gender', 'dob', 'yob', 'study_entry',
                      'startdate', 'enddate', 'exit_date'] + ['expCode', 'exp_label']
        if randomNeg:
            rand_generate = F.udf(lambda x: random.randrange(x), IntegerType())
            neg = neg.withColumn('minEndExitdate' , F.least('exit_date', 'enddate'))

            neg = neg.withColumn('diff', F.datediff(F.col('minEndExitdate'), F.col('study_entry'))) \
                .withColumn('diff', rand_generate('diff')) \
                .withColumn('eventdate', F.expr("date_add(study_entry, diff)")).drop('diff')


        if self.imdReq:
            outputCols.append('imd2015_5')

        pos = pos.select(outputCols)
        neg = neg.select(outputCols)
        demographics = pos.union(neg)
        return demographics

    def extractionExposure(self, file, spark, duration, demographics, sourceT=None, sourceCol=None,rollingTW=-1 ):

        sourceTable = sourceT
        tempcodes = list(self.exposure.values())[0]
        pos, neg = self.get_exp(demographics, sourceTable, tempcodes, sourceCol,rollingTW )
        outputCols = ['patid', 'region', 'eventdate', 'gender', 'dob', 'yob',  'study_entry',
                      'startdate', 'enddate', 'exit_date']+ ['expCode', 'exp_label']


        if self.imdReq:
            outputCols.append('imd2015_5')
        pos = pos.select(outputCols)
        neg = neg.select(outputCols)
        demographics = pos.union(neg)

        return demographics

    def get_exp(self, demographics, source, condition, column='code',rollingTW=-1):

        """
                identify label for patients from the records using the source dataframe and condition list provided
                demographics includes study entry date and when records end
                source is the modality required to retrieve label (e.g. diagnoses)
                condition is a list of code for case identification
                column is the column in source for identifying cases
                """

        # keep records that belongs to a condtion provided by condition list
        source = source.filter(F.col(column).isin(*condition)).select(['patid', 'eventdate', column])

        if rollingTW==-1:
            # take first of the eventdate by patid

            w = Window.partitionBy('patid').orderBy('eventdate')
            source_first = source.withColumn(column, F.first(column).over(w)).groupBy('patid').agg(
                F.min('eventdate').alias('eventdate'),
                F.first(column).alias(column)
            )
            # remove patients that having incidence before the study entry
            demographics = demographics.join(source_first, 'patid', 'left').withColumnRenamed(column, 'expCode')
            exclude = demographics.where(F.col('eventdate') <= F.col('study_entry'))
            # anti left join hack (some bug in pyspark hence the hacky way)
            demographics = demographics.alias('a').join(exclude.select(['patid', 'eventdate']).alias('b'),
                                                        F.col("a.patid") == F.col("b.patid"),
                                                        'left') \
                .filter(F.col("b.patid").isNull()).select('a.*')

        # no need to worry about this for now...
        elif rollingTW!=-1:
            source2 = source
            monthsLambda = lambda i: int(-i * 30 * 86400)

            # Considering the dataframe already created using code provided in question
            source2 = source2.withColumn('unix_time', F.unix_timestamp('eventdate', 'yyyy-MM-dd'))
            source2startdate = source2.groupBy("patid").agg(F.min("eventdate").alias('first_drugdate'))
            winSpec = Window.partitionBy('patid').orderBy('unix_time').rangeBetween(monthsLambda(rollingTW), 0)
            winSpec2 = Window.partitionBy('patid').orderBy('unix_time').rangeBetween(monthsLambda(rollingTW + 13), 0)

            source2 = source2.withColumn('maxdate', F.max('eventdate').over(winSpec))
            source2 = source2.withColumn('mindate', F.min('eventdate').over(winSpec))

            source2 = source2.withColumn('maxdate2', F.max('eventdate').over(winSpec2))
            source2 = source2.withColumn('mindate2', F.min('eventdate').over(winSpec2))

            rwDiff = F.unix_timestamp('maxdate', "yyyy-MM-dd") - F.unix_timestamp('mindate', "yyyy-MM-dd")
            source2 = source2.withColumn('rwDiff', rwDiff) \
                .withColumn('rwDiff', (F.col('rwDiff') / 3600 / 24 / 30).cast('integer'))
            #

            rwDiff = F.unix_timestamp('maxdate2', "yyyy-MM-dd") - F.unix_timestamp('mindate2', "yyyy-MM-dd")
            source2 = source2.withColumn('rwDiff2', rwDiff) \
                .withColumn('rwDiff2', (F.col('rwDiff2') / 3600 / 24 / 30 / (rollingTW + 12)).cast('integer'))
            #


            source2 = source2.select(['patid', 'eventdate', 'prodcode', 'rwDiff', 'rwDiff2', 'mindate'])
            source2del = source2.where(F.col('rwDiff') < rollingTW-1).select('patid').dropDuplicates()
            source2keep = source2.where(F.col('rwDiff') >= rollingTW-1).select('patid').dropDuplicates()

            source2del = source2del.alias('a').join(source2keep.select(['patid']).alias('b'),
                                                    F.col("a.patid") == F.col("b.patid"),
                                                    'left') \
                .filter(F.col("b.patid").isNull()).select('a.*').select('patid')

            source2keep2 = source2.where(F.col('rwDiff') >= rollingTW-1)
            source2keep2 = source2keep2.join(source2startdate, 'patid', 'left')
            source2keep2 = source2keep2.orderBy('patid', 'eventdate')

            w = Window.partitionBy('patid').orderBy('eventdate')

            source_first = source2keep2.withColumn(column, F.first(column).over(w)).groupBy('patid').agg(
                F.min('mindate').alias('eventdate'),
                F.first('prodcode').alias('prodcode'), F.first('rwDiff').alias('rwDiff'),
                F.min('first_drugdate').alias('first_drugdate'), F.first('rwDiff2').alias('rwDiff2')
            )

            source_first2keep = source_first.filter(
                (F.col('eventdate') == F.col('first_drugdate')) | (F.col('rwDiff2') > 0))




            source_first2del =  source_first.filter(
                (F.col('eventdate') != F.col('first_drugdate')) & (F.col('rwDiff2') <= 0))

            # remove patients that having incidence before the study entry
            # demographics includes column + event date
            source_first2keep = source_first2keep.select(['patid','eventdate',column])
            demographics = demographics.join(source_first2keep, 'patid', 'left').withColumnRenamed(column, 'expCode')

            exclude = demographics.where(F.col('eventdate') <= F.col('study_entry'))
            # anti left join
            demographics = demographics.alias('a').join(exclude.select(['patid', 'eventdate']).alias('b'),
                                                        F.col("a.patid") == F.col("b.patid"),
                                                        'left') \
                .filter(F.col("b.patid").isNull()).select('a.*')

            source2fulldel = source2del.withColumn('randoo', F.lit(0))

            demographics = demographics.alias('z').join(source2fulldel.select(['patid' ]).alias('b'),
                                                        F.col("z.patid") == F.col("b.patid"),
                                                        'left') \
                .filter(F.col("b.patid").isNull()).select('z.*')


            demographics = demographics.alias('z').join(source_first2del.select(['patid' ]).alias('b'),
                                                        F.col("z.patid") == F.col("b.patid"),
                                                        'left') \
                .filter(F.col("b.patid").isNull()).select('z.*')

        positive = demographics.where(F.col('eventdate') <= F.col('exit_date')).withColumn('exp_label', F.lit(1))

        positive = positive.withColumn('study_entry', positive['eventdate'])

        negative = demographics.alias('a').join(positive.select(['patid','eventdate']).alias('b'), F.col("a.patid") == F.col("b.patid"),
                                                'left').filter(F.col("b.patid").isNull()).select('a.*')
        negative = negative.withColumn('exp_label', F.lit(0))


        return positive, negative
