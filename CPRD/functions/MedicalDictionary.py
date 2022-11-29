import random
from CPRD.functions import tables, merge
import pyspark.sql.functions as F
from utils.utils import *

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
    for diseases:
        Machine-readable versions (CSV files) of electronic health record phenotyping algorithms for Kuan V., Denaxas S., Gonzalez-Izquierdo A. et al.
        A chronological map of 308 physical and mental health conditions from 4 million individuals in the National Health Service
        published in the Lancet Digital Health - DOI 10.1016/S2589-7500(19)30012-3
        https://github.com/spiros/chronological-map-phenotypes



    for medications:
        Rupert Payne, Rachel Denholm (2018): CPRD product code lists used to define long-term preventative, high-risk, and palliative medication.
        https://doi.org/10.5523/bris.k38ghxkcub622603i5wq6bwag
        https://data.bristol.ac.uk/data/dataset/k38ghxkcub622603i5wq6bwag



    """
    #
    def __init__(self, file, spark):
        self.diagDict = load_obj(file['medicalDict'] + 'CompiledDict/' + 'DiseaseDict' )
        self.medDict =load_obj(file['medicalDict'] + 'CompiledDict/' + 'MedicationDict' )

    def showDiseases(self):
        print('Diseases included currently:')
        print(list(self.diagDict))

    def showMedications(self):
        print('Medications included currently:')
        print(list(self.medDict))


    def compileMedicationDict(self, file, spark, savePath=None):
        """
        compilation of new medication dicts can save it in your local (Arg: savePath).
        will update the medDict on file as well (i.e. self.medDict)
        """
        medDict = MedicationDictionaryCompilerHelper(file, spark)

        if savePath is not None:
            save_obj(medDict, savePath)

        print('updated medication vocabulary dictionary...')
        self.medDict = medDict

    def compileCaliberDict(self, file,spark,primarypath = 'primary_care/',secondarypath = 'secondary_care/', savePath=None):
        """
        compilation of new disease dicts can save it in your local (Arg: savePath).
        will update the diagDict on file as well (i.e. self.diagDict)
        """
        diseaseVoc = DiseaseDictionaryCompilerHelper(file,spark,primarypath,secondarypath)
        if savePath is not None:
            save_obj(diseaseVoc, savePath)

        print('updated disease vocabulary dictionary...')
        self.diagDict=diseaseVoc


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

    def queryDisease(self, queryItem=None, merge=False):

        queryOut = getFromDict(self.diagDict, queryItem)

        if queryOut is None:
            print('Element not in disease dictionary')
            return None
        else:
            if merge and type(queryItem)!=str and len(queryItem)>1:
                queryOut = mergeDict(queryOut)

            return queryOut

    def queryMedication(self, queryItem=None, merge=False):

        queryOut = getFromDict(self.medDict, queryItem)

        if queryOut is None:
            print('Element not in medication dictionary')
            return None
        else:
            if merge and type(queryItem)!=str and len(queryItem)>1:
                queryOut = mergeDict(queryOut)

            return queryOut
