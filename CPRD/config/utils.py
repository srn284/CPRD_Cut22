import os
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import numpy as np
import glob
import pandas as pd
def rename_col(df, old, new):
    """rename pyspark dataframe column"""
    return df.withColumnRenamed(old, new)


def check_time(df, col, time_a=1985, time_b=2016):
    """keep data with date between a and b"""
    year = F.udf(lambda x: x.year)
    df = df.withColumn('Y', year(col))
    df = df.filter(F.col('Y') >= time_a)
    df = df.filter(F.col('Y') <= time_b).drop('Y')
    return df


def cvt_str2time(df, col, year_first=True):
    """
    convert column from string to date type

    format: 1993/01/07
    """
    if year_first:
        return F.to_date(F.concat(df[col].substr(1, 4), F.lit('-'), df[col].substr(5, 2), F.lit('-'), df[col].substr(7, 2)))
    else:
        return F.to_date(F.concat(df[col].substr(7, 4), F.lit('-'), df[col].substr(4, 2), F.lit('-'), df[col].substr(1, 2)))

def translate(mapping):
    # this is a translation function for pyspark dataframe - takes a dictionary and maps all keys in column to the value
    def translate_(col):
        return mapping.get(col)
    return udf(translate_, StringType())

def RangeExtract(df, col, range):
    """
    process ranges of measurements - e.g. bp systolic/diastolic ranges, bmi ranges, weight ranges etc
    this is used for User defined function (UDF) consturction in the pyspark pipeline/framework
    :param Val2Change: the element to change (usually lambda defined 0- check modalities for use)
    :return: return only the values in the desired range - otherwise return None to the lambda call
    """
    df = df.filter((F.col(col)>=range[0]) & (F.col(col)<range[1]))
    # if (float(Val2Change) >= range[0]) and (float(Val2Change) <= range[1]):
    #     return float(Val2Change)
    # else:
    #     return None
    return df
# def RangeExtract(Val2Change, range):
#     """
#     process ranges of measurements - e.g. bp systolic/diastolic ranges, bmi ranges, weight ranges etc
#     this is used for User defined function (UDF) consturction in the pyspark pipeline/framework
#
#     :param Val2Change: the element to change (usually lambda defined 0- check modalities for use)
#     :return: return only the values in the desired range - otherwise return None to the lambda call
#     """
#     if (float(Val2Change) >= range[0]) and (float(Val2Change) <= range[1]):
#         return float(Val2Change)
#     else:
#         return None

def icdFlatten(codes):
    codeout = []
    for x in codes:
        x = x.strip()
        if len(x) == 4:
            if x[:-1] not in codes:
                codeout.append(x[:-1])
        y = x.replace(".", "")
        if len(y) == 3:
            codeout.append(y)
            y = y + "0"

        codeout.append(y)
    return codeout

def bnfFlatten(codes):
    codeout = []
    for x in codes:
        splits = x.split('/')
        out=""
        for y in splits:
            out.append(y[:4]+"/")
        codeout.append(out[:-1])
    return codeout


def exclusionProcess(excl):

    # processing for the variety of codes that get put in for exclusion:
    # icd processing - remove dots/flatten and store as dict
    # med processing - make str and store as dict
    # bnf processing - flatten and make in chunks of 4 and store as dict
    # prod processing - make str and store as dict

    if 'bnf' in excl:
        bnf = excl['bnf']
        bnf = bnfFlatten(bnf)
        bnf = {x :1 for x in bnf}
        excl['bnf']= bnf
    if 'prod' in excl:
        prod = excl['prod']

        prod = {str(x): 1 for x in prod}
        excl['prod'] = prod

    if 'diag_icd' in excl:
        icd = excl['diag_icd']
        icd = icdFlatten(icd)
        icd = {x :1 for x in icd}
        excl['diag_icd'] = icd
    if 'diag_medcode' in excl:
        med = excl['diag_medcode']
        med = {str(x) :1 for x in med}
        excl['diag_medcode'] = med



def antiJoin(df1, df2, equiv, badcode):
    # equivalence is the column to join on
    # badcode is the null column to later drop
    # df1/df2 are dataframes to conduct this antileft
    df1 = df1.join(df2,equiv, 'left').where(F.col(badcode).isNull())
    return df1

def MedicationDictionaryCompilerHelper(file,spark):
    """for scraping from the medication files (bristol uni) """

    medDict = {}
    for filepath in glob.iglob(file['medicalDict'] + 'BNF/prod*'):
        file = pd.read_table(filepath)
        name = filepath.split("/")[-1].split(".")[0][5:]
        colbnf = [col for col in file.columns if 'bnf' in col][0]
        colprod = [col for col in file.columns if 'prod' in col][0]

        #   extract BNF codes
        bnf = file[colbnf].dropna().unique()
        bnf = [str(x) for x in bnf if x != '0' and x != 0]
        bnfOUT = []
        #     translate to the 0000/0000/0000 style of bnf coding
        for x in bnf:
            if '/' in x:
                y = x.split('/')
                bnfOUT.append("/".join([z[:4] for z in y]))
            else:
                bnfOUT.append(x[:4])
        bnfOUT = list(set(bnfOUT))

        #   extract prod codes
        prod = file[colprod].dropna().unique()
        prod = [str(int(x)) for x in prod if x != '0' and x != 0]
        prod = list(set(prod))

        temp = {}
        temp['bnf'] = bnfOUT
        temp['prod'] = prod
        medDict[(name).lower()] = temp
    return medDict


def getFromDict(ElementalDict,queryItem, flatten=False ):
    """simple helper function to get from the dict when the type of input is different
    this is specifically for the diag/medicaiton grabbing functions
    """

    if type(queryItem) == str:
        collectionDict = {}
        queryItem = queryItem.lower()
        if queryItem in ElementalDict:
            collectionDict[queryItem] = ElementalDict[queryItem]
            return collectionDict
        else:
            return None
    else:
        collectionDict = {}
        queryItem = list(set(queryItem))
        for query in queryItem:
            if type(query) == str:
                query = query.lower()
                if query in ElementalDict:
                    collectionDict[query] = ElementalDict[query]
                else:
                    print(query + " not in dictionary")
        if len(collectionDict)!=0:

            return collectionDict
        else:

            return None
def DiseaseDictionaryCompilerHelper(file,spark,primarypath,secondarypath):
    """for scraping from the caliber  files"""

    diagPath = file['medicalDict'] + 'Caliber/'
    codes = np.array(pd.read_csv(diagPath + 'dictionary.csv'))
    codes = np.array(codes)
    diseaseVoc = {}
    for x in codes:
        temp = {}
        primary = x[1]
        secondary = x[2]
        death = x[3]
        if type(primary) == str:
            Codefile = pd.read_csv(diagPath + primarypath + primary)
            Codefile['Medcode'] = Codefile['Medcode'].apply(lambda elem: str(int(elem)))
            temp['Read'] = list(set(Codefile.Readcode.values))
            temp['Medcode'] = list(set(Codefile.Medcode.values))
        if type(secondary) == str:
            Codefile = pd.read_csv(diagPath + secondarypath + secondary)
            temp['ICD'] = list(set([el.replace('.', "") for el in Codefile.ICD10code.values]))
        if type(death) == str:
            Codefile = pd.read_csv(diagPath + secondarypath + death)
            temp['Death'] = list(set([el.replace('.', "") for el in Codefile.OPCS4code.values]))
        diseaseVoc[(x[0]).lower()] = temp
    return diseaseVoc


def mergeDict(elementalDict):

    """for merging dictionaries with multiple common nested keys"""

    uniquekeys = [list(elementalDict[x].keys()) for x in elementalDict]
    keys2combine = set(np.array(uniquekeys).flatten())
    outputs = [[] for i in range(len(keys2combine))]
    tempCollection={}

    for iterel, key in enumerate(keys2combine):
        for x in elementalDict:
            if key in elementalDict[x]:
                temp = outputs[iterel]
                temp = temp + elementalDict[x][key]
                outputs[iterel] = temp
        tempCollection[key] = outputs[iterel]
    collectionDict= {}
    collectionDict['merged']= tempCollection
    return collectionDict
