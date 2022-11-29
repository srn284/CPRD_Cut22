from CPRD.base.table import Patient,Practice,Clinical, Diagnosis, Therapy, Hes
from CPRD.config.spark import read_txt, read_csv
import pyspark.sql.functions as F
from CPRD.config.utils import cvt_str2time
from CPRD.config.utils import rename_col


def retrieve_patient(dir, spark):
    """process patient table in CPRD

    1. read all txt file to spark dataframe
    2. select all patients who are flagged as accept
    3. calibrate yob for each patient (decode records into readable year)
    4. convert current registration date (crd), death date into date type
    5. get pracid by taking the last 3 digits in patid

        Args:
            dir: folder contains patient table in .txt
    """
    patient = Patient(read_txt(spark.sc, spark.sqlContext, path=dir)) \
        .accept_flag().yob_calibration().cvt_crd2date().cvt_tod2date().cvt_deathdate2date().get_pracid().drop('accept')

    return patient


def retrieve_clinical(dir, spark):
    """
    process clinical table in CPRD
    :param dir:
    :param spark:
    :return:
    """

    clinical = Clinical(read_txt(spark.sc, spark.sqlContext, path=dir)).rm_eventdate_medcode_empty() \
        .cvtEventDate2Time()

    return clinical


def retrieve_hes_diagnoses(dir, spark):
    """
    process linked diagnoses data from hes
    :param dir:
    :param spark:
    :return: ['patid', "ICD", "eventdate"]
    """

    hes_diagnosis = rename_col(Diagnosis(read_txt(spark.sc, spark.sqlContext, path=dir))
                               .rm_date_icd_empty().cvt_admidate2date(), old='admidate', new='eventdate')

    return hes_diagnosis


def retrieve_practice(dir, spark):
    """
    process practice table in CPRD
    1. convert last collection date (lcd) and up to standard (uts) date into date type

    :param dir: folder contains file (.txt) for practice
    :param spark: initialised spark project contains spark.sc, and spark.sqlContext
    :return: practice spark dataframe
    """
    practice = Practice(read_txt(spark.sc, spark.sqlContext, path=dir)).cvt_lcd2date().cvt_uts2date()
    return practice


def retrieve_therapy(dir, spark):
    """
    process therapy table in CPRD

    1. remove records with either event data is blank or prodcode is blank
    2. convert event date to date type

    :param dir: folder for therapy files
    :param spark: spark object
    :return: therapy dataframe
    """

    therapy = Therapy(read_txt(spark.sc, spark.sqlContext, path=dir)).rm_eventdate_prodcode_empty().cvtEventDate2Time()
    return therapy


def retrieve_demographics(patient, practice, practiceLink=True):
    """
    process patient table and practice table to get general demographics

    1. get patient in both practice and patient table
    2. get start data using up to standard data and current registration data whichever the greater (later)
       get end data using transfer out date and last collection date whichever the less (earlier)

    should only use record between the start date and end date

    :param patient: patent dataframe
    :param practice: practice dataframe
    :return: demographic dataframe
    """
    if practiceLink:
        joinType='inner'
        demographic = patient.join(practice, patient.pracid == practice.pracid, joinType).drop('pracid'). \
            withColumn('startdate', F.greatest('uts', 'crd')).withColumn('enddate', F.least('tod', 'lcd'))

    else:
        demographic = patient. \
            withColumnRenamed('crd', 'startdate').withColumnRenamed('tod', 'enddate')

    return demographic


def retrieve_link_eligible(dir, spark):
    eligible = read_txt(spark.sc, spark.sqlContext, dir).where(F.col('hes_e') == 1)
    return eligible


def retrieve_death(dir, spark):
    """
    fill paitent who are not dead with dod 01/01/3000

    :param dir: file contains death information for NOS
    :param spark: spark object
    :return: death dataframe
    """

    death = read_txt(spark.sc, spark.sqlContext, path=dir)
    death = death.withColumn('dod', cvt_str2time(death, 'dod', year_first=False))

    return death


def retrieve_bnf_prod_crossmap(dir, spark, cut4= True):
    """
    get cross map for bnf mapping

    :param dir: prod2bnf mapping
    :param spark: pyspark object
    :return: crossmap dataframe
    """

    if cut4:
        extract_bnf = F.udf(lambda x: '/'.join([each[0:4] for each in x.split('/')]) if '/' in x else x[0:4])

    else:
        extract_bnf = F.udf(lambda x:x)

    crossmap = read_txt(spark.sc, spark.sqlContext, path=dir).select('prodcode', 'bnfcode')\
        .where((F.col("bnfcode") != '00000000')).withColumn('code', extract_bnf('bnfcode'))

    return crossmap


def retrieve_med2read_map(dir, spark):
    """
    read medcode to read code mapping from file
    :param dir: path to med2read map
    :param spark:
    :return: dataframe [ 'medcode',  'readcode']
    """

    med2read = read_txt(spark.sc, spark.sqlContext, dir) \
        .withColumn('medcode', F.col('medcode').cast('string')).select(['medcode', 'readcode'])

    return med2read


def retrieve_read2icd_map(dir, spark):
    """
    preprocessed read2icd map
    :param dir:
    :param spark:
    :return: ['read', 'ICD']
    """

    read2icd = read_csv(spark.sqlContext, dir) \
        .select(['read', 'icd']).withColumn('read', F.col('read').cast('string'))

    rm_x = F.udf(lambda x: x if x[-1] != 'X' else x[0:-1])
    read2icd = rename_col(read2icd.withColumn('icd', rm_x('icd')), 'icd', 'ICD')

    return read2icd


def retrieve_additional(dir, spark):
    additional = read_txt(spark.sc, spark.sqlContext, dir)
    return additional



def retrieve_procedure(dir, spark):
    hes_procedure = read_txt(spark.sc, spark.sqlContext, dir).select(['patid', 'OPCS', 'evdate'])
    hes_procedure = rename_col(hes_procedure, 'evdate', 'eventdate')
    hes_procedure = hes_procedure.withColumn('eventdate', cvt_str2time(hes_procedure, 'eventdate', year_first=False))
    return hes_procedure


def retrieve_lab_test(dir, spark):
    test = read_txt(spark.sc, spark.sqlContext, dir).drop('staffid').drop('sysdate')
    test = test.withColumn('eventdate', cvt_str2time(test, 'eventdate'))
    return test