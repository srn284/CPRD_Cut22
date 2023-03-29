from CPRD.base.table import Patient,Practice,Clinical, Diagnosis, Therapy, Hes
from CPRD.config.spark import read_txt, read_csv, read_txtzip
import pyspark.sql.functions as F
from CPRD.config.utils import cvt_str2time
import os


def retrieve_eligible_time(demographics, death, return_dod=False):
    """
    merge demographics and death date to constrain the eligible time for a valid record

    1. merge time with death
    2. the end date is the end date from time table and the death date whichever the earlier

    :param demographics: demographic dataframe
    :param death: death dataframe
    :return: time reference dataframe with patid, start date, and end date
    """
    time = demographics.select(['patid', 'startdate', 'enddate'])
    death = death.select(['patid', 'dod'])

    time = time.join(death, 'patid', 'left')

    time = time.withColumn('enddate', F.least('enddate', 'dod'))

    if return_dod is False:
        time = time.drop('dod')
    return time


def bnf_mapping(crossmap, therapy):
    """
    map product code to BNF code
    1. left join crossmap to map prod coode to bnf code
    2. drop na (no mapping available)
    3. drop duplicates

    :param crossmap: prod2bnf crossmap dataframe
    :param therapy: therapy dataframe
    :return: mapped therapy dataframe with bnfcode
    """
    therapy = therapy.drop('bnfcode').join(crossmap, 'prodcode', 'left').dropna().dropDuplicates()
    return therapy


def med2read_mapping(crossmap, clinical):
    """
    cross map med code in clinical table to read code
    :param crossmap:
    :param clinical:
    :return: ['patid', 'eventdate', 'medcode', 'source', 'readcode']
    """

    cprd = clinical.join(crossmap, 'medcode', 'left') \
        .select(['patid', 'eventdate', 'medcode', 'readcode', 'source']) \
        .withColumn('firstLetter', F.col('readcode').substr(0, 1))

    # keep all records that belong to diagnoses
    cprd = cprd.filter(
        F.col('firstLetter').isin(*['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'Z', 'U']) == False
    ).select(['patid', 'eventdate', 'medcode', 'readcode', 'source'])

    return cprd


def med2sno_mapping(crossmap, clinical):
    """
    cross map med code in clinical table to read code
    :param crossmap:
    :param clinical:
    :return: ['patid', 'eventdate', 'medcode', 'source', 'snomed']
    """

    cprd = clinical.join(crossmap, 'medcode', 'left') \
        .select(['patid', 'eventdate', 'medcode', 'snomed', 'source'])

    # keep all records that belong to diagnoses
    cprd = cprd.select(['patid', 'eventdate', 'medcode', 'snomed', 'source'])

    return cprd
def read2icd_mapping(crossmap, clinical):
    """
    cross map read code to icd code
    :param crossmap: ['read', 'ICD']
    :param clinical: ['patid', 'eventdate', 'medcode', 'source', 'readcode']
    :return: ['patid', 'eventdate', 'medcode', 'source', 'readcode', 'ICD']
    """

    # get read code first 5 characters for mapping
    clinical = clinical.withColumn('read', F.col('readcode').substr(0, 5))

    # join crossmap
    clinical = clinical.join(crossmap, 'read', 'left').drop('read')

    return clinical
def sno2icd_mapping(crossmap, clinical):
    """
    cross map read code to icd code
    :param crossmap: ['read', 'ICD']
    :param clinical: ['patid', 'eventdate', 'medcode', 'source', 'snomed']
    :return: ['patid', 'eventdate', 'medcode', 'source', 'snomed', 'ICD']
    """

    # get read code first 5 characters for mapping

    # join crossmap
    clinical = clinical.join(crossmap, 'snomed', 'left')

    return clinical


def cleanupICD_extrachar(sno2icd):
    rm_x = F.udf(lambda x: x if x[-1] != 'X' else x[0:-1])
    # this is for removing last letter eg I438A but keeping eg PROC_K908
    rm_extraICDletter = F.udf(lambda x: x if ((len(x) < 5) or ('PROC' in x)) else x[0:-1])
    sno2icd = sno2icd.withColumn('ICD', rm_x('ICD')).withColumn('ICD', rm_extraICDletter('ICD'))

    return sno2icd
def merge_hes_clinical(hes, clinical):
    """

    :param hes:  ['patid', 'eventdate', 'ICD', 'source']
    :param clinical: ['patid', 'eventdate', 'medcode', 'source', 'readcode', 'ICD']
    :return:
    """

    for name in clinical.columns:
        if name not in hes.columns:
            hes = hes.withColumn(name, F.lit(None))

    hes = hes.select(clinical.columns)
    return hes.union(clinical)