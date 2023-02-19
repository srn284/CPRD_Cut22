import datetime
import glob
import math
import random
from pathlib import Path
from typing import Any

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

from CPRD.config.spark import read_parquet
from CPRD.functions import merge, tables
from CPRD.functions.modalities import *
from utils.utils import *


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
        if "PhenoMaps" in file and "medicalDict" in file:
            self.componentDict = load_obj(file["PhenoMaps"])
            self.diagDict = load_obj(file["medicalDict"] + "DiseaseDict")
            self.medDict = load_obj(file["medicalDict"] + "MedicationDict")
            self.measureDict = load_obj(file["medicalDict"] + "MeasurementDict")
            self.procDict = load_obj(file["medicalDict"] + "ProcedureDict")
            self.element_keys = (
                list(self.measureDict.keys())
                + list(self.diagDict.keys())
                + list(self.medDict.keys())
                + list(self.procDict.keys())
            )
        else:
            self.componentDict = {}
            self.diagDict = {}
            self.medDict = {}
            self.measureDict = {}
            self.procDict = {}

    def compile_medical_dictionary(self, file, save_path: Path = None, key2find: str):
        """Compile dicts so you can save them to a file. Will update the instance as well."""
        compiled_dict = CompilerHelper(file=file, key2find=key2find)

        if save_path is not None:
            save_obj(compiled_dict, save_path)

        return compiled_dict

    def compileMedicationDict(self, file, savePath=None):
        """Compile medication dictionaries"""
        compiled_dict = self.compile_medical_dictionary(
            file=file, save_path=savePath, key2find="meds"
        )

        print("updated medication vocabulary dictionary...")
        self.medDict = compiled_dict

    def compileMeasurementsDict(self, file, savePath=None):
        """
        compilation of new measurement dicts can save it in your local (Arg: savePath).
        will update the dict on file as well (i.e. self.measureDict)
        """
        compiled_dict = self.compile_medical_dictionary(
            file=file, save_path=savePath, key2find="obs"
        )

        print("updated measurements vocabulary dictionary...")
        self.measureDict = compiled_dict

    def compileProcedureDict(self, file, savePath=None):
        """Compile new procedure dict"""
        medDict = self.compile_medical_dictionary(
            file=file, save_path=savePath, key2find="proc"
        )

        print("updated procedure vocabulary dictionary...")
        self.procDict = medDict

    def compileDiseaseDict(self, file, savePath=None):
        """Compile disease dictionary"""
        diseaseVoc = self.compile_medical_dictionary(
            file=file, save_path=savePath, key2find="pheno"
        )

        print("updated disease vocabulary dictionary...")
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

    def query_dictionary(
        self,
        medical_dictionary,
        medical_dictionary_name: str,
        queryItem=None,
        merge=False,
    ):
        """Query the medical dictionary"""

        queryOut = getFromDict(medical_dictionary, queryItem)

        if queryOut is None:
            print(f"Element not in {medical_dictionary_name} dictionary")
            return None
        else:
            if merge and type(queryItem) != str and len(queryItem) > 1:
                queryOut = mergeDict(queryOut)

            return queryOut

    def queryDisease(self, queryItem=None, merge=False):
        self.query_dictionary(
            medical_dictionary=self.diagDict,
            medical_dictionary_name="diagnosis",
            queryItem=queryItem,
            merge=merge,
        )

    def queryMeasurement(self, queryItem=None, merge=False):
        self.query_dictionary(
            medical_dictionary=self.measureDict,
            medical_dictionary_name="measurement",
            queryItem=queryItem,
            merge=merge,
        )

    def queryProcedure(self, queryItem=None, merge=False):
        self.query_dictionary(
            medical_dictionary=self.procDict,
            medical_dictionary_name="procedure",
            queryItem=queryItem,
            merge=merge,
        )

    def queryMedication(self, queryItem=None, merge=False):
        self.query_dictionary(
            medical_dictionary=self.medDict,
            medical_dictionary_name="medication",
            queryItem=queryItem,
            merge=merge,
        )

    def findItem(self, queryItem=None):
        terms = [
            x
            for x in self.element_keys
            if queryItem.lower().strip() in x.lower().strip()
        ]
        return terms
