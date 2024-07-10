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
            clinical = clinical.withColumn('ICD', F.explode_outer('ICD'))

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

    clinical = tables.retrieve_clinical(dir=file['clinical'], spark=spark)
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
def retrieve_tchdlratio(file, spark, duration=(1985, 2021), usable_range=(0, 50)):
    """
    retrive tchdlrat from additional
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'tchdlrat']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    codesrat = ['259250015', '856761000006115', '458249011', '458252015', '8396611000006112']

    tchdlrat = retrieve_by_enttype(file, spark, enttype=codesrat, id_str='10', duration=duration)
    tchdlrat = tchdlrat.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    tchdlrat = tchdlrat.filter((F.col('value').isNotNull()))
    tchdlrat = tchdlrat.groupby(['patid', 'eventdate']) \
        .agg(F.mean('value').alias('tchdl_rat'))

    return tchdlrat
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


def retrieve_nt_probnp(file, spark, duration=(1985, 2021), usable_range=(0, 1000)):
    """
    retrive nt_probnp from additional
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'ntprobnp']
    """
    ntprobnp = ['6864871000006115']
    ntprobnp = retrieve_by_enttype(file, spark, enttype=ntprobnp, id_str='10', duration=duration)
    ntprobnp = ntprobnp.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    ntprobnp = ntprobnp.filter((F.col('value').isNotNull()))
    ntprobnp = ntprobnp.groupby(['patid', 'eventdate']) \
        .agg(F.mean('value').alias('ntprobnp'))

    return ntprobnp
def retrieve_sodium(file, spark, duration=(1985, 2021), usable_range=(80, 230)):
    """
    retrive sodium from additional
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'sodium']
    """
    sodium = ['4113021000006114', '284446019','4113031000006112',]
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


def retrieve_smoking_level_status(file, spark, duration=(1985, 2021)):
    """
    get the smoking status
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'smoke']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)

    heavy = ['3422221000006116', '5003181000006112', '67621000006112', '819331000006110', '855001000006114']
    non = ['1154431000000112', '4120291000006119', '4120281000006117', '4120301000006118', '6217561000006111',
           '903051000006112', '14866014', '5495921000006119', '854951000006113', '4427811000006111', '3926051000006118',
           '250374013', '6217461000006110', '1123751000000113', '397732011', '459702016'] + ['1009271000006120',
                                                                                             '1488666019',
                                                                                             '1009271000006118',
                                                                                             '904111000006113',
                                                                                             '1154431000000110']
    ex = ['1059701000000120', '1151791000000120'] + ['418914010', '6217151000006116', '2735381000000111',
                                                     '2735201000000112', '3513199018', '4980561000006117',
                                                     '250363016', '250364010', '250365011', '250367015',
                                                     '649831000006117', '298701000000114', '2735421000000119',
                                                     '4980771000006118', '649851000006112', '853001000006110',
                                                     '7568991000006119', '3374141000006117', '250371017',
                                                     '1059701000000119', '903041000006110', '854111000006110',
                                                     '3513101011', '2735331000000112',
                                                     '2735281000000119', '6217281000006116', '2735181000000113',
                                                     '2636041000006110', '5496031000006112',
                                                     '1599721000006113', '649821000006115', '250366012',
                                                     '854051000006112', '342602019', '250385010',
                                                     '649861000006114', '852991000006114', '250373019',
                                                     '8017571000006117', '649841000006110']
    light = ['72373013'] + ['136515019', '216212011', '344794017', '344795016', '1780360012', '2669652019',
                            '137711000006111',
                            '854021000006115', '903981000006117', '904031000006115', '1809121000006113',
                            '137791000006118',
                            '5495941000006114', '8153381000006119', '5495901000006112', '12482301000006115',
                            '11904991000006116',
                            '4948531000006116', '5003191000006110', '102921000006112', '250369017', '397733018',
                            '2670126018',
                            '137721000006115', '250387019', '1488576014', '108938018', '1592611000000110',
                            '904001000006111',
                            '854071000006119', '854961000006110', '503483019', '5003151000006116', '88471000006112',
                            '8153371000006117', '1488577017', '250372012', '743331000006116', '7375991000006118',
                            '604961000006114',
                            '1488578010', '99639019', '128130017', '250368013', '5003141000006118', '4533531000006119',
                            '5495951000006111', '2170961000000116', '852981000006111', '2474719011', '344793011',
                            '250375014']
    mod = ['854981000006117', '700121000006118', '5003161000006119', '3419101000006116']

    allcodes_smoke = non + ex + light + mod + heavy
    labelforsmoke = [0] * len(non) + [1] * len(ex) + [2] * len(light) + [3] * len(mod) + [4] * len(heavy)
    smokelevels = pd.DataFrame(np.array([allcodes_smoke, labelforsmoke]).transpose()).rename(
        columns={0: 'medcode', 1: 'smoke'})
    smoke2join = spark.sqlContext.createDataFrame(smokelevels)

    allsmoke = retrieve_by_enttype(file, spark, enttype=allcodes_smoke, id_str='10', duration=duration)
    allsmoke = allsmoke.join(smoke2join, 'medcode', 'inner')
    return allsmoke.select('patid', 'eventdate', 'smoke')


def retrieve_smoking_status(file, spark, duration=(1985, 2021)):
    """
    get the smoking status
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'smoke']
    """
    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)
    smokecode = condition_query.queryMeasurement(['smoking+yes'], merge=True)['smoking+yes']['medcode']
    ex_smokecode = condition_query.queryMeasurement(['smoking+ex'], merge=True)['smoking+ex']['medcode']
    no_smokecode = condition_query.queryMeasurement(['smoking+no'], merge=True)['smoking+no']['medcode']

    allcodes_smoke = smokecode+ex_smokecode+no_smokecode
    allsmoke = retrieve_by_enttype(file, spark, enttype=allcodes_smoke, id_str='10', duration=duration)

    smoke = allsmoke.filter(F.col('medcode').isin(smokecode)).withColumn('smoke', F.lit(1))
    ex_smoke = allsmoke.filter(F.col('medcode').isin(ex_smokecode)).withColumn('smoke', F.lit(2))
    no_smoke = allsmoke.filter(F.col('medcode').isin(no_smokecode)).withColumn('smoke', F.lit(3))

    return smoke.union(ex_smoke).union(no_smoke).select('patid', 'eventdate', 'smoke')


def retrieve_nyha_status(file, spark, duration=(1985, 2021)):
    """
    get the smoking status
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'smoke']
    """

    condition_query = MedicalDictionary.MedicalDictionaryRiskPrediction(file, spark)

    lv1codes = condition_query.queryMeasurement(['new york heart association heart failure classification 1'])[
        'new york heart association heart failure classification 1']['medcode']

    lv2codes = condition_query.queryMeasurement(['new york heart association heart failure classification 2'])[
        'new york heart association heart failure classification 2']['medcode']

    lv3codes = condition_query.queryMeasurement(['new york heart association heart failure classification 3'])[
        'new york heart association heart failure classification 3']['medcode']

    lv4codes = condition_query.queryMeasurement(['new york heart association heart failure classification 4'])[
        'new york heart association heart failure classification 4']['medcode']

    nyha_allother = condition_query.queryMeasurement(['new york heart association heart failure classification'])[
        'new york heart association heart failure classification']['medcode']

    allnyhacodes = lv1codes + lv2codes + lv3codes + lv4codes + nyha_allother


    clinical = tables.retrieve_clinical(dir=file['clinical'], spark=spark).filter(F.col('obstypeid').isin(['7', '10']))
    clinical = clinical.filter(F.col('medcode').isin(allnyhacodes))
    allnyha = check_time(clinical, 'eventdate', time_a=duration[0], time_b=duration[1])
    propercols = ['patid', 'eventdate', 'pracid', 'medcode', 'nyha']
    lv1 = allnyha.filter(F.col('medcode').isin(lv1codes)).withColumn('nyha', F.lit(1)).select(propercols)
    lv2 = allnyha.filter(F.col('medcode').isin(lv2codes)).withColumn('nyha', F.lit(2)).select(propercols)
    lv3 = allnyha.filter(F.col('medcode').isin(lv3codes)).withColumn('nyha', F.lit(3)).select(propercols)
    lv4 = allnyha.filter(F.col('medcode').isin(lv4codes)).withColumn('nyha', F.lit(4)).select(propercols)
    allother = allnyha.filter(F.col('medcode').isin(nyha_allother)).withColumnRenamed('value', 'nyha').select(
        propercols)


    return allother.union(lv1).union(lv2).union(lv3).union(lv4)


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

def retrieve_LVEF_measurement(file, spark, duration=(1985, 2021), usable_range=(0, 100)):
    """
    get the egfr measurement, systolic pressure (high number)
    :param file:
    :param spark:
    :param duration:
    :return: ['patid', 'eventdate', 'eGFR']
    """

    lvef = retrieve_by_enttype(file, spark, enttype=['5292721000006113'], duration=duration)
    lvef = lvef.where((F.col('value') > usable_range[0]) & (F.col('value') < usable_range[1]))
    lvef = lvef.filter( (F.col('value').isNotNull()))
    lvef = lvef.groupby(['patid', 'eventdate'])\
        .agg(F.mean('value').alias('lvef'))

    return lvef

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
    retrieve imd (update still 2015_5)

    :param file: file load from yaml contains dir for all necessary files
    :param spark: spark object
    :return: imd table
    """

    imd = read_txt(spark.sc, spark.sqlContext, path=file['imd'])
    imd = imd.select(['patid', 'imd2015_5']).filter(imd.imd2015_5 !='') \
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

    procedure = tables.retrieve_hes_proc(dir=file['proc_hes'], spark=spark)

    procedure = procedure.join(time, 'patid', 'inner') \
        .where((F.col('eventdate') > F.col('startdate')) & (F.col('eventdate') < F.col('enddate')))

    procedure = check_time(procedure, 'eventdate', time_a=duration[0], time_b=duration[1])

    return procedure

def retrieve_ethnicity(file, spark):
    ethnicity = read_txt(spark.sc, spark.sqlContext, file['hes_patient']).select(['patid', 'gen_ethnicity'])

    ethnicity = ethnicity.withColumn(
        "gen_ethnicity_mapped",
        F.expr(
            """
            CASE gen_ethnicity
                WHEN 'Bangladesi' THEN 'Asian'
                WHEN 'Bl_Carib' THEN 'Black'
                WHEN 'Indian' THEN 'Asian'
                WHEN 'Chinese' THEN 'Asian'
                WHEN 'Pakistani' THEN 'Asian'
                WHEN 'Bl_Other' THEN 'Black'
                WHEN 'White' THEN 'White'
                WHEN 'Bl_Afric' THEN 'Black'
                WHEN 'Oth_Asian' THEN 'Asian'
                WHEN 'Mixed' THEN 'Other or Unknown'
                WHEN 'Other' THEN 'Other or Unknown'
                WHEN 'Unknown' THEN 'Other or Unknown'
                ELSE 'Other or Unknown'
            END
            """
        ),
    )
    return ethnicity
def retrieve_rawethnicity(file, spark):
    ethnicity = read_txt(spark.sc, spark.sqlContext, file['hes_patient']).select(['patid', 'gen_ethnicity'])

    ethnicity = ethnicity.withColumn(
        "gen_ethnicity_mapped",
        F.expr(
            """
            CASE gen_ethnicity
                WHEN 'Bangladesi' THEN 'Bangladesi'
                WHEN 'Bl_Carib' THEN 'Bl_Carib'
                WHEN 'Indian' THEN 'Indian'
                WHEN 'Chinese' THEN 'Chinese'
                WHEN 'Pakistani' THEN 'Pakistani'
                WHEN 'Bl_Other' THEN 'Bl_Other'
                WHEN 'White' THEN 'White'
                WHEN 'Bl_Afric' THEN 'Bl_Afric'
                WHEN 'Oth_Asian' THEN 'Oth_Asian'
                WHEN 'Mixed' THEN 'Other'
                WHEN 'Other' THEN 'Other'
                WHEN 'Unknown' THEN 'Unknown'
                ELSE 'Unknown'
            END
            """
        ),
    )
    return ethnicity
