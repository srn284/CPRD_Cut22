import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
import pyspark
from CPRD.config.utils import cvt_str2time, cvt_datestr2time
# def rename_col(df, old, new):
#     """rename pyspark dataframe column"""
#     return df.withColumnRenamed(old, new)
from utils.utils import *

DICT2KEEP = load_obj('/home/workspace/datasets/cprd/cprd2021/linkage/20_095_Results/Documentation/Set 21/linkage_coverage_dictv')

class Patient(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def accept_flag(self):
        """select rows with accpt = 1"""
        return Patient(self.where((F.col('acceptable')==1)))

    def yob_calibration(self):
        """decode yob to regular year"""
        return Patient(self.withColumn('yob', self.yob.cast(pyspark.sql.types.IntegerType())))

    def cvt_tod2date(self):
        """convert tod from string to date"""
        return Patient(self.withColumn('tod', cvt_datestr2time(self, 'regenddate')) .drop('regenddate'))

    def cvt_deathdate2date(self):
        """convert deathdate from string to date"""
        return Patient(self.withColumn('cprd_ddate', cvt_datestr2time(self, 'cprd_ddate')))

    def cvt_crd2date(self):
        """convert crd from string to date"""
        return Patient(self.withColumn('crd', cvt_datestr2time(self, 'regstartdate')).drop('regstartdate'))

    def cvt_pracid(self):
        """get pracid from patid inorder to join with practice table"""
        return Patient(self.withColumn('pracid', self['pracid'].cast(pyspark.sql.types.IntegerType())))


class Clinical(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)


    def cvtEventDate2Time(self):
        """ convert eventdate from strnig to date type"""
        return Clinical(self.withColumn('eventdate', cvt_datestr2time(self, 'obsdate')) .drop('obsdate'))

    def rm_eventdate_medcode_empty(self):
        """rm row with empty eventdate or medcode"""
        return Clinical(self.filter((F.col('obsdate') != '') & (F.col('medcodeid') != '')).withColumnRenamed ('medcodeid', 'medcode') )

    def filter_byobservation(self):
        """remove the rows which are not observations"""
        return Clinical (self.where((F.col('obstypeid')=='7')))

    def filter_byid(self, id_str):
        """remove the rows which are not obs_id"""
        return Clinical (self.where((F.col('obstypeid')==id_str)))


class Consultation(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def cvtEventDate2Time(self):
        """ convert eventdate from strnig to date type"""
        return Consultation(self.withColumn('eventdate', cvt_datestr2time(self, 'consdate')) .drop('consdate'))

    def rm_eventdate_medcode_empty(self):
        """rm row with empty eventdate or medcode"""
        return Consultation(self.filter((F.col('consdate') != '') & (F.col('consmedcodeid') != '')).withColumn('medcode', F.col('consmedcodeid')) .drop('consmedcodeid'))



class Practice(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def cvt_lcd2date(self):
        """convert lcd from string to date"""
        return Practice(self.withColumn('lcd', cvt_datestr2time(self, 'lcd')))

    def cvt_uts2date(self):
        """NOTE: UTS NOT POPULATED IN 2021 CUT -- FOR NOW, SET TO STATIC VALUE!!!"""
        return Practice(self.drop('uts').withColumn('uts', F.to_date(F.lit('10/10/0001') , 'dd/mm/yyyy')))
    def intpracid(self):
        return Practice(self.withColumn('pracid', self['pracid'].cast(pyspark.sql.types.IntegerType())))

    def rmv_badPract(self):
        badlist =[20024, 20036, 20091, 20202, 20254, 20389, 20430, 20469, 20487, 20552, 20554, 20734, 20790, 20803, 20868, 20996, 21001, 21078, 21118, 21172, 21173, 21277, 21334, 21390, 21444, 21451, 21553, 21558, 21585]
        return Practice(self.where(~F.col('pracid') .isin( badlist)))
class Diagnosis(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def cvt_admidate2date(self):
        """conver admidate from string to date"""
        df = self.withColumn('admidate', cvt_datestr2time(self, 'admidate'))

        return Diagnosis(df)

    def icd_rm_dot(self):
        """remove '.' from ICD code"""
        replace = F.udf(lambda x: x.replace('.', ''))
        return Diagnosis(self.withColumn('ICD', replace('ICD')))

    def rm_date_icd_empty(self):
        """remove admidate or icd code is empty"""
        return Diagnosis(self.filter((F.col('admidate') != '') & (F.col('ICD') != '')))
    def hes_apc_timefilter(self):
        """not all HES records are "acceptable" - only those in a certain time range are acceptable and this function
        conducts filtering
        """

        df = self.withColumn('goodstart', F.to_date(F.lit(DICT2KEEP['hes_apc'][0]), 'dd/MM/yyyy')) \
            .withColumn('goodend', F.to_date(F.lit(DICT2KEEP['hes_apc'][1]), 'dd/MM/yyyy'))

        df = df.filter(F.col('admidate') >= F.col('goodstart')).filter(F.col('admidate') < F.col('goodend'))
        return Diagnosis(df.drop('goodend').drop('goodstart'))
class Hes(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def cvt_string2date(self, col):
        df = self.withColumn(col, F.concat(F.col(col).substr(7, 4), F.col(col).substr(4, 2), F.col(col).substr(1, 2)))
        return Hes(df.withColumn(col, cvt_datestr2time(df, col)))

    def rm_dot(self, col):
        rm_dot = F.udf(lambda x: ''.join(x.split('.')))
        return Hes(self.withColumn(col, rm_dot(col)))


class Therapy(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def rm_eventdate_prodcode_empty(self):
        """rm row with empty eventdate or medcode"""
        return Therapy(self.filter((F.col('issuedate') != '') & (F.col('prodcodeid') != '')).withColumnRenamed('prodcodeid','prodcode').withColumnRenamed('issuedate','eventdate'))

    def cvtEventDate2Time(self):
        """ convert eventdate from strnig to date type"""
        return Therapy(self.withColumn('eventdate', cvt_datestr2time(self, 'eventdate')))


class EHR(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def cal_age(self, event_date, yob, year=True, name='age'):
        if year:
            age_cal = F.udf(lambda x, y : x.year - y.year)
        else:
            # assume people born in January
            age_cal = F.udf(lambda x, y : (x.year * 12 + x.month) - (y.year * 12 + 1))

        return EHR(self.withColumn(name, age_cal(F.col(event_date), F.col(yob))))

    def cal_year(self, event_date, name='year'):
        yearCal = F.udf(lambda x: x.year)

        return EHR(self.withColumn(name, yearCal(F.col(event_date))))

    def set_col_to_str(self, col):
        return EHR(self.withColumn(col, F.col(col).cast('string')))

    def array_add_element(self, col, element):
        return EHR(self.withColumn(col, F.concat(F.col(col), F.array(F.lit(element)))))

    def array_flatten(self, col):
        return EHR(self.withColumn(col, F.flatten(F.col(col))))



class Proc_HES(DataFrame):
    def __init__(self, df):
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

    def cvt_admidate2date(self):
        """conver admidate from string to date"""
        df = self.withColumn('evdate', cvt_datestr2time(self, 'evdate'))

        return Proc_HES(df)

    def icd_rm_dot(self):
        """remove '.' from ICD code"""
        replace = F.udf(lambda x: x.replace('.', ''))
        return Proc_HES(self.withColumn('ICD', replace('ICD')))

    def rm_date_opcs_empty(self):
        """remove admidate or icd code is empty"""
        return Proc_HES(self.filter((F.col('evdate') != '') & (F.col('OPCS') != '')))
    def hes_apc_timefilter(self):
        """not all HES records are "acceptable" - only those in a certain time range are acceptable and this function
        conducts filtering
        """


        df = self.withColumn('goodstart', F.to_date(F.lit(DICT2KEEP['hes_apc'][0]), 'dd/MM/yyyy')) \
            .withColumn('goodend', F.to_date(F.lit(DICT2KEEP['hes_apc'][1]), 'dd/MM/yyyy'))

        df = df.filter(F.col('evdate') >= F.col('goodstart')).filter(F.col('evdate') < F.col('goodend'))
        return Proc_HES(df.drop('goodend').drop('goodstart'))