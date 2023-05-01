#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash


# @task(log_prints=True)

def read_txt_files_from_folder(folder_path):
    file_list = [f for f in os.listdir(folder_path) if f.endswith('.txt')]
    df_list = []
    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'r') as file:
            lines = file.readlines()
            data = [line.strip().split(',') for line in lines]
            df_list.append(pd.DataFrame(data))
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df

""" def upload_to_gcs(bucket, object_name, local_file):
    
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
"""

# @flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

# @flow(name="Ingest Data")
def main_flow(datasource: str):
    folder_path =  'C:\\Users\\Manish_Bhoge\\OneDrive - EPAM\\Tut\\DataEngineering-zoomcamp\\Project\\Huge Stock Market Dataset\\Data\\Stocks'
    df = read_txt_files_from_folder(folder_path)
    print(df.head())

if __name__ == '__main__':
    main_flow(datasource = "stocks")