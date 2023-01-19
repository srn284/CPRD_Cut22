from CPRD.functions.modalities import retrieve_medications
import os
from utils.utils import create_folder


def main(params, spark):
    # merge diagnoses from CPRD and HES, and map all codes to ICD10
    file = params['file_path']
    data_params = params['params']

    medication = retrieve_medications(file, spark, mapping=data_params.exposure[0], duration=data_params['time_range'])

    # remove none and duplicate
    create_folder(params['save_path'])

    medication.write.parquet(os.path.join(params['save_path'], params['save_file']))