import pyspark.sql.functions as F
from CPRD.functions import tables, merge, MedicalDictionary
from CPRD.config.utils import *
from CPRD.config.spark import *
import CPRD.base.table as cprd_table
import pandas as pd
"""
medications, diagnoses, enttype, bmi, drinking, smoking, bp measurement
"""


def retrieve_medications(file, spark, mapping='bnfvtm', duration=(1985, 2021), demographics=None, practiceLink=True):
    """
    retrieve medication
    require patient, practice, therapy from CPRD, and death registration file from NOS, and prod2bnf mapping table

    :param file: file load from yaml contains dir for all necessary files
    :param spark: spark object
    :param bnf_mapping: if need to map from product code to bnf code
    :param duration: inclusion criteria for time range
    :return:
    """
    # read patient table
    if demographics is None:
        patient = tables.retrieve_patient(dir=file['patient'], spark=spark)
        practice = tables.retrieve_practice(dir=file['practice'], spark=spark)
        demographics = tables.retrieve_demographics(patient=patient, practice=practice, practiceLink=practiceLink)
        death = tables.retrieve_death(dir=file['death'], spark=spark)

        # retirve the start date and end date to retrive records for each patient
        time = merge.retrieve_eligible_time(demographics=demographics, death=death)

    else:
        time = demographics.select(['patid','startdate', 'enddate'])

    therapy = tables.retrieve_therapy(dir=file['therapy'], spark=spark)


    # select records that are valid between start date and end date
    therapy = therapy.join(time, 'patid', 'inner')\
        .where((F.col('eventdate') > F.col('startdate')) & (F.col('eventdate') < F.col('enddate'))).drop('deathdate')

    if  mapping=='bnfvtm':
        crossmap = tables.retrieve_bnfvtm_prod_crossmap(dir=file['prod2bnf_vtm'], spark=spark)
        therapy = merge.bnf_mapping(crossmap=crossmap, therapy=therapy)
    elif mapping=='bnf':
        crossmap = tables.retrieve_bnf_prod_crossmap(dir=file['prod2bnf'], spark=spark)
        therapy = merge.bnf_mapping(crossmap=crossmap, therapy=therapy)

    therapy = check_time(therapy, 'eventdate', time_a=duration[0], time_b=duration[1])

    return therapy


def retrieve_diagnoses(file, spark, mapping ='sno2icd' ,  duration=(1985,2021), practiceLink=True):
    """
    file contains all necessary file for processing
    :param file:
    :param spark:
    :param read_mapping:
    :param icd_mapping:
    :param duration:
    :return:
    """

    patient = tables.retrieve_patient(dir=file['patient'], spark=spark)
    practice = tables.retrieve_practice(dir=file['practice'], spark=spark)
    demographics = tables.retrieve_demographics(patient=patient, practice=practice, practiceLink=practiceLink)
    death = tables.retrieve_death(dir=file['death'], spark=spark)
    clinical = tables.retrieve_clinical(dir=file['clinical'], spark=spark).filter_byobservation()
    hes = tables.retrieve_hes_diagnoses(dir=file['diagnosis_hes'], spark=spark)

    # retrieve the start date and end date to retrive records for each patient
    time = merge.retrieve_eligible_time(demographics=demographics, death=death)

    # select records between start and end date, which are valid records for usage
    clinical = clinical.join(time, 'patid', 'inner').where((F.col('eventdate') > F.col('startdate')) &
                                                           (F.col('eventdate') < F.col('enddate'))).drop('deathdate')

    # process CPRD and HES data
    clinical = clinical.select(['patid', 'eventdate', 'medcode']).withColumn('source', F.lit('CPRD'))
    hes = cprd_table.Hes(hes.select(['patid', 'eventdate', 'ICD']).withColumn('source', F.lit('HES'))).rm_dot('ICD')


    if 'sno' in mapping:
        med2sno = tables.retrieve_med2sno_map(dir=file['med2sno'], spark=spark)
        clinical = merge.med2sno_mapping(med2sno, clinical)
        if mapping =='sno2icd':
            sno2icd = tables.retrieve_sno2icd_map(dir=file['sno2icd'], spark=spark)
            clinical = merge.sno2icd_mapping(sno2icd, clinical)
            clinical = clinical.withColumn('ICD', F.explode('ICD'))
    # map medcode to read code
    elif 'read' in mapping:
        med2read = tables.retrieve_med2read_map(dir=file['med2read'], spark=spark)
        clinical = merge.med2read_mapping(med2read, clinical)

    # map read to icd-10
        if mapping =='read2icd':
            read2icd = tables.retrieve_read2icd_map(dir=file['read2icd'], spark=spark)
            clinical = merge.read2icd_mapping(read2icd, clinical)

    # merge cprd and hes
    data = merge.merge_hes_clinical(hes, clinical)
    data = merge.cleanupICD_extrachar(data)
    # apply time filtering
    data = check_time(data, 'eventdate', time_a=duration[0], time_b=duration[1])
    data = data.dropDuplicates()
    return data

def retrieve_diagnoses_cprd(file, spark, mapping ='sno2icd' , duration=(1985,2021), demographics=None, practiceLink=True):
    """
    file contains all necessary file for processing
    :param file:
    :param spark:
    :param read_mapping:
    :param icd_mapping:
    :param duration:
    :return:
    """
    if demographics is None:
        patient = tables.retrieve_patient(dir=file['patient'], spark=spark)
        practice = tables.retrieve_practice(dir=file['practice'], spark=spark)
        demographics = tables.retrieve_demographics(patient=patient, practice=practice, practiceLink=practiceLink)
        death = tables.retrieve_death(dir=file['death'], spark=spark)

        # retrieve the start date and end date to retrive records for each patient
        time = merge.retrieve_eligible_time(demographics=demographics, death=death)

    else:
        time = demographics.select(['patid','startdate','enddate'])


    clinical = tables.retrieve_clinical(dir=file['clinical'], spark=spark).filter_byobservation()


    # select records between start and end date, which are valid records for usage
    clinical = clinical.join(time, 'patid', 'inner').where((F.col('eventdate') > F.col('startdate')) &
                                                           (F.col('eventdate') < F.col('enddate'))).drop('deathdate')

    # process CPRD
    clinical = clinical.select(['patid', 'eventdate', 'medcode']).withColumn('source', F.lit('CPRD'))
    # map medcode to read code
    # if read_mapping:
    #     med2read = tables.retrieve_med2read_map(dir=file['med2read'], spark=spark)
    #     clinical = merge.med2read_mapping(med2read, clinical)
    #
    # # map read to icd-10
    # if icd_mapping:
    #     read2icd = tables.retrieve_read2icd_map(dir=file['read2icd'], spark=spark)
    #     clinical = merge.read2icd_mapping(read2icd, clinical)

    if 'sno' in mapping:
        med2sno = tables.retrieve_med2sno_map(dir=file['med2sno'], spark=spark)
        clinical = merge.med2sno_mapping(med2sno, clinical)
        if mapping == 'sno2icd':
            sno2icd = tables.retrieve_sno2icd_map(dir=file['sno2icd'], spark=spark)
            clinical = merge.sno2icd_mapping(sno2icd, clinical)
            clinical = clinical.withColumn('ICD', F.explode('ICD'))
    # map medcode to read code
    elif 'read' in mapping:
        med2read = tables.retrieve_med2read_map(dir=file['med2read'], spark=spark)
        clinical = merge.med2read_mapping(med2read, clinical)

        # map read to icd-10
        if mapping == 'read2icd':
            read2icd = tables.retrieve_read2icd_map(dir=file['read2icd'], spark=spark)
            clinical = merge.read2icd_mapping(read2icd, clinical)
    clinical = check_time(clinical, 'eventdate', time_a=duration[0], time_b=duration[1])

    return clinical

def retrieve_diagnoses_hes(file, spark, duration=(1985,2021), demographics = None):
    """
    file contains all necessary file for processing
    :param file:
    :param spark:
    :param read_mapping:
    :param icd_mapping:
    :param duration:
    :return:
    """

    hes = tables.retrieve_hes_diagnoses(dir=file['diagnosis_hes'], spark=spark)
    hes = cprd_table.Hes(hes.select(['patid', 'eventdate', 'ICD']).withColumn('source', F.lit('HES'))).rm_dot('ICD')
    if demographics is not None:
        time = demographics.select(['patid','startdate','deathdate'])

        hes = hes.join(time, 'patid', 'inner').where((F.col('eventdate') > F.col('startdate')) &
                                                           (F.col('eventdate') < F.col('deathdate'))).select(['patid', 'eventdate', 'ICD', 'source'])
    else:
        print('need some demographics /patient file to proceed!')
        raise NotImplementedError
    # apply time filtering
    hes = check_time(hes, 'eventdate', time_a=duration[0], time_b=duration[1])

    return hes

def retrieve_by_enttype(file, spark, enttype, id_str='10', duration=(1985, 2021)):
    """
    retrieve additional information from additional table using enttype

    :param file:
    :param spark:
    :param duration:
    :return:
    """
    # for the isin operation ...
    if type(enttype) is not list:
        enttype = [enttype]

    clinical = tables.retrieve_clinical(dir=file['clinical'], spark=spark).filter_byid(id_str)
    clinical = clinical.filter(F.col('medcode').isin(enttype))
    clinical = check_time(clinical, 'eventdate', time_a=duration[0], time_b=duration[1])
    return clinical


def retrieve_bmi(file, spark, duration=(1985, 2021), usable_range=(5, 50)):
    """
    retrive bmi from additional
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'bmi']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    bmi = condition_query.queryMeasurement(['bmi'], merge=True)['bmi']['medcode']

    bmi = retrieve_by_enttype(file, spark, enttype=bmi, id_str='10', duration=duration)
    bmi = bmi.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    bmi = bmi.filter((F.col('value').isNotNull()))
    bmi = bmi.groupby(['patid', 'eventdate']) \
        .agg(F.mean('value').alias('bmi'))

    return bmi

def retrieve_hdlr(file, spark, duration=(1985, 2021), usable_range=(0, 10)):
    """
    retrive bmi from additional
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'hdlr']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    hdlr = condition_query.queryMeasurement(['cholesterol'], merge=True)['cholesterol']['medcode']
    hdlr = retrieve_by_enttype(file, spark, enttype=hdlr, id_str='10', duration=duration)
    hdlr = hdlr.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    hdlr = hdlr.filter((F.col('value').isNotNull()))
    hdlr = hdlr.groupby(['patid', 'eventdate']) \
        .agg(F.mean('value').alias('hdlr'))

    return hdlr

def retrieve_sodium(file, spark, duration=(1985, 2021), usable_range=(20, 200)):
    """
    :param file:
    :param spark:
    :param duration:
    """
    sodium = ['259010012', '259011011', '5514121000006114', '404493014', '4113021000006114', '5514131000006112', \
            '284446019', '6277651000006115', '4113031000006112', '6277661000006118', '6277671000006113']

    sodium = retrieve_by_enttype(file, spark, enttype=sodium, id_str='10', duration=duration)
    sodium = sodium.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    sodium = sodium.filter((F.col('value').isNotNull()))
    sodium = sodium.groupby(['patid', 'eventdate']) \
        .agg(F.mean('value').alias('sodium'))

    return sodium

def retrieve_drinking_status(file, spark, duration=(1985, 2021)):
    """
    get the drinking status table
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'alcohol']
    """

    drink = retrieve_by_enttype(file, spark, enttype=['1221271018'], id_str='10', duration=duration)
    drink = drink.filter((F.col('value').isNotNull()))
    drink = drink.groupby(['patid', 'eventdate']) \
        .agg(F.mean('value').alias('alcohol'))

    return drink


def retrieve_smoking_status(file, spark, duration=(1985, 2021)):
    """
    get the smoking status
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'smoke']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    smoke = condition_query.queryMeasurement(['smoking+yes'], merge=True)['smoking+yes']['medcode']
    ex_smoke = condition_query.queryMeasurement(['smoking+ex'], merge=True)['smoking+ex']['medcode']
    no_smoke = condition_query.queryMeasurement(['smoking+no'], merge=True)['smoking+no']['medcode']

    smoke = retrieve_by_enttype(file, spark, enttype=smoke, id_str='10', duration=duration).withColumn('smoke', F.lit(1))
    ex_smoke = retrieve_by_enttype(file, spark, enttype=ex_smoke, id_str='10', duration=duration).withColumn('smoke', F.lit(2))
    no_smoke = retrieve_by_enttype(file, spark, enttype=no_smoke, id_str='10', duration=duration).withColumn('smoke', F.lit(3))

    return smoke.union(ex_smoke).union(no_smoke).select('patid', 'eventdate', 'smoke')


def retrieve_diastolic_bp_measurement(file, spark, duration=(1985, 2021), usable_range=(10, 140)):
    """
    get the bp measurement diastolic pressure (low number )
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'diastolic']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    dbp = condition_query.queryMeasurement(['dbp'], merge=True)['dbp']['medcode']

    bp = retrieve_by_enttype(file, spark, enttype=dbp, duration=duration)
    bp = bp.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    bp = bp.filter((F.col('value').isNotNull()))
    bp = bp.groupby(['patid', 'eventdate']) \
        .agg(F.mean('value').alias('diastolic'))

    return bp

def retrieve_systolic_bp_measurement(file, spark, duration=(1985, 2021), usable_range=(50, 300)):
    """
    get the bp measurement, systolic pressure (high number)
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'systolic']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    sbp = condition_query.queryMeasurement(['sbp'], merge=True)['sbp']['medcode']

    bp = retrieve_by_enttype(file, spark, enttype=sbp, duration=duration)
    bp = bp.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    bp = bp.filter( (F.col('value').isNotNull()))
    bp = bp.groupby(['patid', 'eventdate'])\
        .agg( F.mean('value').alias('systolic'))

    return bp

def retrieve_heartrate_measurement(file, spark, duration=(1985, 2021), usable_range=(30,250)):
    """
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'heartrate']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    hr = condition_query.queryMeasurement(['heart rate'], merge=True)['heart rate']['medcode']

    hr = retrieve_by_enttype(file, spark, enttype=hasattr, duration=duration)
    hr = hr.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    hr = hr.filter((F.col('value').isNotNull()))
    hr = hr.groupby(['patid', 'eventdate'])\
        .agg(F.mean('value').alias('heartrate'))

    return hr


def retrieve_creatinine_measurement(file, spark, duration=(1985, 2021), usable_range=(0, 250)):
    """
    get the creat measurement
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'creatinine']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    creat = condition_query.queryMeasurement(['creatinine'], merge=True)['creatinine']['medcode']

    creat = retrieve_by_enttype(file, spark, enttype=creat, duration=duration)
    creat = creat.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    creat = creat.filter( (F.col('value').isNotNull()))
    creat = creat.groupby(['patid', 'eventdate'])\
        .agg(F.mean('value').alias('creatinine'))

    return creat

def retrieve_eGFR_measurement(file, spark, duration=(1985, 2021), usable_range=(0, 250)):
    """
    get the egfr measurement, systolic pressure (high number)
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'eGFR']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    egfr = condition_query.queryMeasurement(['egfr'], merge=True)['egfr']['medcode']

    egfr = retrieve_by_enttype(file, spark, enttype=egfr, duration=duration)
    egfr = egfr.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    egfr = egfr.filter( (F.col('value').isNotNull()))
    egfr = egfr.groupby(['patid', 'eventdate'])\
        .agg(F.mean('value').alias('eGFR'))

    return egfr

def retrieve_imd(file, spark):
    """
    retrieve imd

    :param file: file load from yaml contains dir for all necessary files
    :param spark: spark object
    :return: imd table
    """

    imd = read_txt(spark.sc, spark.sqlContext, path=file['imd'])
    imd = imd.select(['patid', 'imd2015_5']).filter(imd.imd2021_5 !='') \
        .filter((F.col('imd2015_5').isNotNull()))

    return imd


def retrieve_test_LEGACY(file, spark, duration=(1985, 2021)):
    """
    retrieve test
    :param file: file load from yaml contains dir for all necessary files
    :param spark: spark object
    :return: test table
    """
    test = tables.retrieve_additional(dir=file['test'], spark=spark)
    # read the entity types from the lookups directly
    ent = pd.read_table(file['entity'])
    entval = list(ent[ent.filetype == 'Test'].enttype.values)
    entval = [str(x) for x in entval]
    # compile entity types that are in the test entitytp types


    # create lookup table for easy tokenizing - first ten characters of the name of the test type
    ent['mapVal'] = ent[['enttype', 'description']].apply(lambda x: x[1][:10] + "ENT_" + str(x[0]), axis=1)
    lookup = ent[ent.filetype == 'Test'].set_index('enttype').to_dict()['mapVal']
    lookup = {str(x): lookup[x].replace(" ", "") for x in lookup}

    test = test.select(['patid', 'eventdate', 'enttype'])
    # filter by entity type and make sure the tests are in the enttype
    test = test.filter(F.col('enttype').isin(entval)).where(F.col("eventdate").isNotNull())
    test = test.withColumn('eventdate', cvt_str2time(test, 'eventdate')).na.drop()


    # check time for this modality
    test = check_time(test, 'eventdate', time_a=duration[0], time_b=duration[1])

    # make tests easily readable by mapping id's of tests to the right name in the lookup table (dict abovedefined as lookup)
    test = test.withColumn("tests", translate(lookup)('enttype')).na.drop().select(['patid', 'eventdate', 'tests'])
    test = test.dropDuplicates()

    return test


def retrieve_lab_test_LEGACY(file, spark, duration=(1985, 2021), demographics=None, start_col='startdate',
                       end_col='enddate'):
    """
    retrieve medication
    require patient, practice, therapy from CPRD, and death registration file from NOS, and prod2bnf mapping table
    :param file: file load from yaml contains dir for all necessary files
    :param spark: spark object
    :param bnf_mapping: if need to map from product code to bnf code
    :param duration: inclusion criteria for time range
    :return:
    """
    # read patient table
    if demographics is None:
        patient = tables.retrieve_patient(dir=file['patient'], spark=spark)
        practice = tables.retrieve_practice(dir=file['practice'], spark=spark)
        demographics = tables.retrieve_demographics(patient=patient, practice=practice)
        death = tables.retrieve_death(dir=file['death'], spark=spark)

        # retirve the start date and end date to retrive records for each patient
        time = merge.retrieve_eligible_time(demographics=demographics, death=death)

    else:
        time = demographics.select(['patid', start_col, end_col])

    test = tables.retrieve_lab_test(dir=file['test'], spark=spark)

    test = test.join(time, 'patid', 'inner')\
        .where((F.col('eventdate') > F.col('startdate')) & (F.col('eventdate') < F.col('enddate')))

    test = check_time(test, 'eventdate', time_a=duration[0], time_b=duration[1])

    return test


def retrieve_procedure(file, spark, duration=(1985, 2021), demographics=None, start_col='startdate', end_col='enddate'):
    """
    retrieve medication
    require patient, practice, therapy from CPRD, and death registration file from NOS, and prod2bnf mapping table
    :param file: file load from yaml contains dir for all necessary files
    :param spark: spark object
    :param bnf_mapping: if need to map from product code to bnf code
    :param duration: inclusion criteria for time range
    :return:
    """
    # read patient table
    if demographics is None:
        patient = tables.retrieve_patient(dir=file['patient'], spark=spark)
        practice = tables.retrieve_practice(dir=file['practice'], spark=spark)
        demographics = tables.retrieve_demographics(patient=patient, practice=practice)
        death = tables.retrieve_death(dir=file['death'], spark=spark)

        # retirve the start date and end date to retrive records for each patient
        time = merge.retrieve_eligible_time(demographics=demographics, death=death)

    else:
        time = demographics.select(['patid', start_col,  end_col])

    procedure = tables.retrieve_procedure(dir=file['hes_procedure'], spark=spark)

    procedure = procedure.join(time, 'patid', 'inner') \
        .where((F.col('eventdate') > F.col('startdate')) & (F.col('eventdate') < F.col('enddate')))

    procedure = check_time(procedure, 'eventdate', time_a=duration[0], time_b=duration[1])

    return procedure
