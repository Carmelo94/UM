# from IPython.core.display import display, HTML
# display(HTML("<style>.container { width:80% !important; }</style>"))

from datetime import datetime
import time
import pandas as pd
import numpy as np
import os
import pyodbc
from glob import glob

# import sys
# sys.path.append('../01_utils')
from static import *

def load_file(folder, list_of_files):
    '''

    '''
    if folder == '00_pathmatics':
        dfs = [pd.read_csv(f, skiprows=1) for f in list_of_files]
        df = pd.concat(dfs)

        # fillna for dimensions
        df.iloc[:, :-2] = df.iloc[:, :-2].fillna('<NA>')

        # rename columns
        df.rename(columns={'Direct/Indirect':'Direct_Indirect', 'Brand (Major)':'Brand_Major', 'Brand (Minor)':'Brand_Minor', 'Brand (Leaf)':'Brand_Leaf'}, inplace=True)
        df.rename(columns={c:c.replace(' ','_') for c in df.columns}, inplace=True)

        # df['Landing_Page'] = df['Landing_Page'].apply(lambda x: f"'{x}'" if 'http' in str(x).lower() else x)
        df['Link_to_Creative'] = df['Link_to_Creative'].apply(lambda x: f"'{x}'" if 'http' in str(x).lower() else x)

        df['Site'] = df['Site'].apply(lambda x: odd_chars[x] if x in odd_chars.keys() else x)
        df['Date'] = pd.to_datetime(df['Date'])

    else:
        dfs = [pd.read_excel(f, sheet_name='Report', keep_default_na=False) for f in list_of_files]
        df = pd.concat(dfs)

        # adjust NA
        df = df.replace('NA', '<NA>')
        df = df.replace('None', '<NA>')

        skip = df.index[df.iloc[:, 0] == 'TIME PERIOD'][0]
        df.columns = df.iloc[skip, :]
        df = df.iloc[skip+1:, :].reset_index(drop=True)
        df.drop(columns=['YEAR','QUARTER'], inplace=True)
        df['TIME PERIOD'] = pd.to_datetime(df['TIME PERIOD'])

        df.rename(columns={'DOLS (000)':'DOLS_000'}, inplace=True)
        df.rename(columns={c:c.replace(' ','_') for c in df.columns}, inplace=True)

    return df

def table_names(folder):
    dims = defined_dims[meta[folder]['short']]

    tables = dict()
    for d in dims.keys():
        table_name = f"amex.{meta[folder]['short']}_dim_{d}"
        tables[d] = table_name

    # append date
    tables['date'] = f"amex.{meta[folder]['short']}_dim_date"

    return tables

def load_tables(folder):
    tables = table_names(folder)

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    loaded_dims = dict()
    for tbl in tables.keys():
        qry = f"SELECT * FROM {tables[tbl]};"
        df_sql = pd.read_sql_query(qry, conn)

        # python does not read in the same datatype as in sql
        if tbl == 'date':
            col = meta[folder]['freq'][1]
            df_sql[col] = pd.to_datetime(df_sql[col])
            df_sql['Month'] = pd.to_datetime(df_sql['Month'])

        elif tbl == 'crea':
            df_sql['Creative_ID'] = pd.to_numeric(df_sql['Creative_ID'])

        loaded_dims[tbl] = df_sql

    conn.close()
    return loaded_dims

def create_fact(folder, df, loaded_dimensions):
    fact = df.copy()
    for d in loaded_dimensions.keys():
        if d == 'crea':
            df_ = loaded_dimensions[d][['crea_ID','Creative_ID']]
            fact = fact.merge(df_ , how='left')
            fact.drop(columns='Creative_ID', inplace=True)
        else:
            fact = fact.merge(loaded_dimensions[d], how='left')

    fact_cols = [c for c in fact.columns if '_id' in c.lower()] + meta[folder]['metrics']

    fact = fact[fact_cols]

    # ts = datetime.strftime(datetime.now(), '%Y%m%d')
    ts = int(time.time())
    fact['timestamp'] = ts

    print('Null Count')
    print(fact.isnull().sum())

    filename = f"{meta[folder]['short']}_fact.csv"
    fact.to_csv(os.path.join(outputpath, folder, '01_facts', filename), index=False)

    return fact

def main():
    for folder in os.listdir(datapath):
        if 'path' in folder:
            print(f'\n{folder}')

            list_of_files = glob(os.path.join(datapath, folder, meta[folder]['ext']))
            df = load_file(folder, list_of_files)

            # load dimensions from db
            db_dims = load_tables(folder)

            # fact
            fact_table = create_fact(folder, df, db_dims)

            print(f'{folder} done!\n')

    return None


if __name__ == '__main__':
    main()
