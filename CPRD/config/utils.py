import os
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import numpy as np
import _pickle as pickle
import glob
import pandas as pd
def rename_col(df, old, new):
    """rename pyspark dataframe column"""
    return df.withColumnRenamed(old, new)

def save_obj(obj, name):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f)


def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

def check_time(df, col, time_a=1985, time_b=2016):
    """keep data with date between a and b"""
    year = F.udf(lambda x: x.year)
    df = df.withColumn('Y', year(col))
    df = df.filter(F.col('Y') >= time_a)
    df = df.filter(F.col('Y') <= time_b).drop('Y')
    return df

def cvt_datestr2time(df, col, year_first=True):
    """
    convert column from string to date type

    format: 1993/01/07
    """
    return F.to_date(df[col] ,"dd/MM/yyyy")


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



def CompilerHelper(file,spark, key2find = 'pheno'):
    """for scraping from the raw phenotype dicts files"""

    allpheno = load_obj (file['PhenoMaps'])
    diseaseVoc = {}
    for x in allpheno:
        if key2find in x.lower():
            for y in allpheno[x]:
                if y in diseaseVoc:
                    temp = diseaseVoc[y]
                    for zz in temp:
                        tempzz = temp[zz]
                        for jj in allpheno[x][y][zz]:
                            tempzz.append(jj)
                        diseaseVoc[y][zz] = list ( set(tempzz))
                else:
                    diseaseVoc[y] = allpheno[x][y]
    return diseaseVoc