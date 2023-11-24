import random
from CPRD.functions import tables, merge
import pyspark.sql.functions as F
from utils.utils import *
from CPRD.config.utils import *

from CPRD.functions.modalities import *
from pyspark.sql import Window
from CPRD.config.spark import read_parquet
from typing import Any
import datetime
from pyspark.sql.types import IntegerType
import math
import numpy as np
import glob


class MedicalDictionaryBase:
    """
    Dictionary creation for a variety of modalities. for right now, we have included diseases and medications.
    the functions: showDiseases and showMedications are there to illustrate to user what disease/medications are included for utilising


    sources:
    Dare2think: has aurum codes for diagnoses, medications, measurements, procedures

    Conrad et al: has aurum codes for diag and measurements
    (https://www.thelancet.com/journals/lancet/article/PIIS0140-6736(22)01349-6/fulltext)

    """

    #
    def __init__(self, file, spark):
        """
        load dict
        """
        if 'PhenoMaps' in file and 'medicalDict' in file:
            self.componentDict = load_obj(file['PhenoMaps'])
            self.diagDict = load_obj(file['medicalDict'] + 'DiseaseDict')
            self.medDict = load_obj(file['medicalDict'] + 'MedicationDict')
            self.measureDict = load_obj(file['medicalDict'] + 'MeasurementDict')
            self.procDict = load_obj(file['medicalDict'] + 'ProcedureDict')
            self.element_keys =list(self.measureDict.keys()) +list(self.diagDict.keys())+list(self.medDict.keys())+list(self.procDict.keys())
        else:
            self.componentDict = {}
            self.diagDict = {}
            self.medDict = {}
            self.measureDict = {}
            self.procDict = {}


    def showDiseases(self):
        print('Diseases included currently:')
        print(list(self.diagDict))

    def showMeasurements(self):
        print('Measurements/Tests included currently:')
        print(list(self.measureDict))

    def showProcedures(self):
        print('Procedures included currently:')
        print(list(self.procDict))

    def showMedications(self):
        print('Medications included currently:')
        print(list(self.medDict))

    def compileMedicationDict(self, file, spark, savePath=None):
        """
        compilation of new medication dicts can save it in your local (Arg: savePath).
        will update the medDict on file as well (i.e. self.medDict)
        """
        medDict = CompilerHelper(file, spark, key2find='meds')

        if savePath is not None:
            save_obj(medDict, savePath)

        print('updated medication vocabulary dictionary...')
        self.medDict = medDict

    def compileMeasurementsDict(self, file, spark, savePath=None):
        """
        compilation of new measurement dicts can save it in your local (Arg: savePath).
        will update the dict on file as well (i.e. self.measureDict)
        """
        medDict = CompilerHelper(file, spark, key2find='obs')

        if savePath is not None:
            save_obj(medDict, savePath)

        print('updated measurements vocabulary dictionary...')
        self.measureDict = medDict

    def compileProcedureDict(self, file, spark, savePath=None):
        """
        compilation of new procedures dicts can save it in your local (Arg: savePath).
        will update the dict on file as well (i.e. self.procDict)
        """
        medDict = CompilerHelper(file, spark, key2find='proc')

        if savePath is not None:
            save_obj(medDict, savePath)

        print('updated procedure vocabulary dictionary...')
        self.procDict = medDict

    def compileDiseaseDict(self, file, spark, savePath=None):
        """
        this will compile from the exisiting phenomaps available


        compilation of new disease dicts can save it in your local (Arg: savePath).
        will update the diagDict on file as well (i.e. self.diagDict)
        """
        diseaseVoc = CompilerHelper(file, spark, key2find='pheno')
        if savePath is not None:
            save_obj(diseaseVoc, savePath)

        print('updated disease vocabulary dictionary...')
        self.diagDict = diseaseVoc


class MedicalDictionaryRiskPrediction(MedicalDictionaryBase):
    def __init__(self, file, spark):
        super().__init__(file, spark)

    """
    Use: 

    medD = MedicalDictionary(file, spark)
    print(medD.showDiseases())
    # print the diseases available to select from....

    hypertensionDict = medD.queryDisease('hypertension')
   #  now can use hypertensionDict as an input for a function or just use codes inside

    2diseseDict = medD.queryDisease(['hypertension' , 'diabetes'])
   #  now 2diseseDict has 2 keys - one for hypertension and another for diabetes

   CVDdict = medD.queryDisease(['heart failure' , 'ihd','stroke'], merge=True)
   #  now CVDdict has 1 key: merged. and the read codes are merged within it. the HES codes are merged within it. and so on...

   # the same is with the medications....

   """

    def getAllComponents(self):
        return self.componentDict

    def queryDisease(self, queryItem=None, merge=False):

        queryOut = getFromDict(self.diagDict, queryItem)

        if queryOut is None:
            print('Element not in disease dictionary')
            return None
        else:
            if merge and type(queryItem) != str and len(queryOut) > 1 :
                queryOut = mergeDict(queryOut)


            return queryOut

    def queryMeasurement(self, queryItem=None, merge=False):

        queryOut = getFromDict(self.measureDict, queryItem)

        if queryOut is None:
            print('Element not in measurement dictionary')
            return None
        else:
            if merge and type(queryItem) != str and len(queryOut) > 1 :
                queryOut = mergeDict(queryOut)

            return queryOut

    def queryProcedure(self, queryItem=None, merge=False):

        queryOut = getFromDict(self.procDict, queryItem)

        if queryOut is None:
            print('Element not in procedure dictionary')
            return None
        else:
            if merge and type(queryItem) != str and len(queryOut) > 1 :
                queryOut = mergeDict(queryOut)

            return queryOut

    def queryMedication(self, queryItem=None, merge=False):

        queryOut = getFromDict(self.medDict, queryItem)

        if queryOut is None:
            print('Element not in medication dictionary')
            return None
        else:
            if merge and type(queryItem) != str and len(queryOut) > 1 :
                queryOut = mergeDict(queryOut)

            return queryOut

    def findItem(self,queryItem=None):
        terms = [x for x in self.element_keys if queryItem.lower().strip() in x.lower().strip()]
        return terms
