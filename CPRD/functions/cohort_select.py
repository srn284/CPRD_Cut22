import random
from CPRD.functions import tables, merge
import pyspark.sql.functions as F
from CPRD.config.utils import cvt_str2time
from typing import Any
import datetime
from pyspark.sql.types import IntegerType

class Cohort:
    def __init__(self, least_year_register_gp, least_age, greatest_age):
        self.least_year_register_gp = least_year_register_gp
        self.least_age = least_age
        self.greatest_age = greatest_age
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
        demographics = demographics.join(time, on='patid', how='inner')\
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

    def standard_prepare(self, file, spark):
        """
        return dataframe contains information about records start and end date, the date for at least N year after
        registration with GP, and the date for a patient at Y years old for futher cohort selection

        return least_gp_register_date, {}_dob {} is the age pre-defined
        """
        patient = tables.retrieve_patient(dir=file['patient'], spark=spark)
        practice = tables.retrieve_practice(dir=file['practice'], spark=spark)
        eligible = tables.retrieve_link_eligible(dir=file['eligible'], spark=spark)
        death = tables.retrieve_death(dir=file['death'], spark=spark)

        demographics = self.retrieve_eligible_patid(patient, practice, eligible)
        demographics = self.date_least_year_register_gp(demographics, death)
        demographics = self.date_least_age(demographics)
        demographics = self.date_greatest_age(demographics)

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


class CohortHardCut(Cohort):
    def __init__(self, least_year_register_gp, least_age, greatest_age):
        super().__init__(least_year_register_gp, least_age, greatest_age)

    def pipeline(self, file, spark, study_entry_date='1995-01-01'):
        """
        keep patients who are at least age X, and at least register with GP for Y years at study entry date
        """
        demographics = self.standard_prepare(file, spark)
        demographics = demographics.withColumn('study_entry', F.to_date(F.lit(study_entry_date)))

        # study entry date after the date of birth
        demographics = demographics.where(F.col('study_entry') > F.col('{}_dob'.format(self.least_age)))
        demographics = demographics.where(F.col('study_entry') <= F.col('{}_dob'.format(self.greatest_age)))

        # study entry date after x year of registration with gp
        demographics = demographics.where(F.col('study_entry') > F.col('least_gp_register_date'))

        # also require the patient records end date after the study entry date
        demographics = demographics.where(F.col('study_entry') < F.col('enddate'))
        return demographics


class CohortSoftCut(Cohort):
    def __init__(self, least_year_register_gp, least_age, greatest_age):
        super().__init__(least_year_register_gp, least_age, greatest_age)

    def pipeline(self, file, spark,  duration=('1995-01-01', '2015-01-01')):
        """
        study entry for patients: greatest of:
        1) xth bday== least_age
        2) date of reg + least_year_register_gp
        3) study_entry_date

        """
        demographics = self.standard_prepare(file, spark)

        demographics = demographics.withColumn('study_entry', F.to_date(F.lit(duration[0])))
        # study entry is greatest of three as described above
        demographics = demographics.withColumn('study_entry_real', F.greatest('{}_dob'.format(self.least_age), 'least_gp_register_date', 'study_entry'))\
            .drop('study_entry') .withColumnRenamed("study_entry_real", "study_entry")


        # last start of study is the second element of the duration (the finish date)

        demographics = demographics.withColumn('exit_date', F.least(F.col('enddate'), F.to_date(F.lit(duration[1])),
                                                                    F.col('{}_dob'.format(self.greatest_age))))

        # requirement of the start of study before the last date (enddate)
        demographics = demographics.where(F.col('study_entry') < F.col('enddate'))
        demographics = demographics.where(F.col('study_entry') < F.col('exit_date'))

        return demographics

class CohortRandomCut(Cohort):
    def __init__(self, least_year_register_gp, least_age, greatest_age):
        super().__init__(least_year_register_gp, least_age, greatest_age)

    def pipeline(self, file, spark, duration=('1995-01-01', '2015-01-01')):
        """
        random select baseline date between the start and end
        start: greatest of dob, gp registration, study start date (duration[0])
        end: least of end date, duration
        other criteria, the start date is smaller than the end date

        duration in (year-month-date) format
        """
        demographics = self.standard_prepare(file, spark)
        demographics = demographics.withColumn('start', F.greatest(F.col('least_gp_register_date'),
                                                                   F.col('{}_dob'.format(self.least_age)),
                                                                   F.to_date(F.lit(duration[0]))))

        demographics = demographics.withColumn('end', F.least(F.col('enddate'), F.to_date(F.lit(duration[1])),
                                                              F.col('{}_dob'.format(self.greatest_age))))

        demographics = demographics.where(F.col('start') < F.col('end'))

        # random generate a date between start and end as baseline
        rand_generate = F.udf(lambda x: random.randrange(x), IntegerType())
        demographics = demographics.withColumn('diff', F.datediff(F.col('end'), F.col('start')))\
            .withColumn('diff', rand_generate('diff'))\
            .withColumn('study_entry', F.expr("date_add(start, diff)")).drop('diff')

        return demographics

