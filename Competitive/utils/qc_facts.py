from datetime import datetime
import time
import pandas as pd
import numpy as np
import os
import pyodbc
import glob
from qc_dimensions import *
from static import *

# paths
OUTPUTS_PATH = paths["OUTPUTS_PATH"]
DATA_PATH = paths['DATA_PATH']

'''
As of 10/8/2020, facts will be manually appended to database via the import wizard. Waiting on BULK access.
'''


def new_fact(folder, df, db_dimensions):
    '''
    Converts new data into fact table.
        Parameters:
            folder (str): name of folders in datapath, either 00_pathmatics or 01_kantar
            df (dataframe): new data from 02_data
            df_dimensions (dict): dimensions from dataframe
            db_dimensions (dict): dimensions from database
        Returns:
            df (dataframe): dataframe fact table
    '''

    for d in db_dimensions.keys():
        if d == 'crea':
            df_ = db_dimensions[d][['crea_ID','Creative_ID']]
            df = df.merge(df_, how='left')
            df.drop(columns='Creative_ID', inplace=True)
        else:
            df = df.merge(db_dimensions[d], how='left')

    fact_cols = [c for c in df.columns if '_id' in c.lower()] + meta[folder]['metrics']
    df = df[fact_cols]

#     ts = datetime.strftime(datetime.now(), '%Y%m%d')
    ts = int(time.time())
    df['timestamp'] = ts

    for metric in meta[folder]['metrics']:
        df[metric] = df[metric].astype(float)

    # extra insurance
    if df.iloc[:, :-4].isnull().sum().sum() > 0:
        print('\n[CONTAINS NULLS]')
        print(df.isnull().sum())

    else:
        print(f"\nExporting fact\n{'-'*25}")
        print(df.info())
        filename = f"{meta[folder]['short']}_fact_{ts}.csv"
        df.to_csv(os.path.join(OUTPUTS_PATH, folder, '01_facts', filename), index=False)

    return df

def insert_fact(folder, df_fact_table):
    '''
    Inserts facts from dataframe to the database.
        Parameters:
            folder (str): name of folders in datapath, either 00_pathmatics or 01_kantar
            df_fact_table (dataframe): converted raw data fact
        Returns:
            None
    '''
    # extra insurance
    if df_fact_table.isnull().sum().sum() > 0:
        print('\n[CONTAINS NULLS]')
        print(df_fact_table.isnull().sum())

    else:
        print(f"\nInserting to MSSQL\n{'-'*25}")
        print(f"Rows: {len(df_fact_table)}")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        if folder == '00_pathmatics':
            for i, r in df_fact_table.iterrows():
                insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_fact({df_fact_table.columns[0]},{df_fact_table.columns[1]},{df_fact_table.columns[2]},{df_fact_table.columns[3]},{df_fact_table.columns[4]},{df_fact_table.columns[5]},{df_fact_table.columns[6]},{df_fact_table.columns[7]},{df_fact_table.columns[8]},{df_fact_table.columns[9]},{df_fact_table.columns[10]},{df_fact_table.columns[11]},{df_fact_table.columns[12]},{df_fact_table.columns[13]},{df_fact_table.columns[14]}) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                values = (r[df_fact_table.columns[0]],r[df_fact_table.columns[1]],r[df_fact_table.columns[2]],r[df_fact_table.columns[3]],r[df_fact_table.columns[4]],r[df_fact_table.columns[5]],r[df_fact_table.columns[6]],r[df_fact_table.columns[7]],r[df_fact_table.columns[8]],r[df_fact_table.columns[9]],r[df_fact_table.columns[10]],r[df_fact_table.columns[11]],r[df_fact_table.columns[12]],r[df_fact_table.columns[13]],r[df_fact_table.columns[14]])

                cursor.execute(insert_qry, values)
                conn.commit()

        elif folder == '01_kantar':
            for i, r in df_fact_table.iterrows():
                insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_fact({df_fact_table.columns[0]},{df_fact_table.columns[1]},{df_fact_table.columns[2]},{df_fact_table.columns[3]},{df_fact_table.columns[4]},{df_fact_table.columns[5]},{df_fact_table.columns[6]},{df_fact_table.columns[7]},{df_fact_table.columns[8]},{df_fact_table.columns[9]},{df_fact_table.columns[10]},{df_fact_table.columns[11]},{df_fact_table.columns[12]},{df_fact_table.columns[13]},{df_fact_table.columns[14]},{df_fact_table.columns[15]},{df_fact_table.columns[16]},{df_fact_table.columns[17]},{df_fact_table.columns[18]}) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                values = (r[df_fact_table.columns[0]],r[df_fact_table.columns[1]],r[df_fact_table.columns[2]],r[df_fact_table.columns[3]],r[df_fact_table.columns[4]],r[df_fact_table.columns[5]],r[df_fact_table.columns[6]],r[df_fact_table.columns[7]],r[df_fact_table.columns[8]],r[df_fact_table.columns[9]],r[df_fact_table.columns[10]],r[df_fact_table.columns[11]],r[df_fact_table.columns[12]],r[df_fact_table.columns[13]],r[df_fact_table.columns[14]],r[df_fact_table.columns[15]],r[df_fact_table.columns[16]],r[df_fact_table.columns[17]],r[df_fact_table.columns[18]])

                cursor.execute(insert_qry, values)
                conn.commit()

        conn.close()
        return None

def qc_facts():
    '''
    Loops through folders in 02_data and load facts.
        Parameters:
            None
        Returns:
            None
    '''
    for folder in os.listdir(DATA_PATH):
        try:
            print(f"{'='*50}\nRunning: {folder}")

            # get the latest filepath in dir
            list_of_files = glob.glob(os.path.join(DATA_PATH, folder, meta[folder]['ext']))
            # filepath = max(list_of_files, key=os.path.getctime)
            print(f"\nLoading data from: {list_of_files}")

            # load the file
            df = load_file(folder, list_of_files)
            df_dims = defined_dims[meta[folder]['short']]
            df_dims['date'] = [meta[folder]['freq'][1]]

            # dimensions from database
            db_dims = load_dimensions(folder, connection_string)

            # new fact
            df_fact = new_fact(folder, df, db_dims)

                # insert fact
                # insert_fact(folder, df_fact)
        except ValueError:
            print(f"No files in {folder}")

    return None
