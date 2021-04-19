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

def date_dimension(folder):
    col = meta[folder]['freq'][1]
    freq = meta[folder]['freq'][0]

    dates = pd.date_range(start=datetime(2017,1,1), end=datetime(2021,12,31), freq=freq)
    dates = pd.DataFrame(dates, columns=[col])
    dates['Year'] = dates[col].dt.year
    dates['Quarter'] = dates[col].dt.quarter
    dates['Month_No'] = dates[col].dt.month
    dates['Month'] = dates.apply(lambda x: datetime(x['Year'], x['Month_No'], 1), axis=1)

    dates.drop(columns=['Month_No'], inplace=True)

    return dates


def df_dimensions(folder, df):
    '''
    '''

    dims = defined_dims[meta[folder]['short']]

    dict_of_dims = dict()
    for k in dims.keys():
        df_ = df[dims[k]].drop_duplicates()
        df_ = df_.sort_values(by=dims[k])
        dict_of_dims[k] = df_

    dates = date_dimension(folder)
    dict_of_dims['date'] = dates

    return dict_of_dims

def write_dimension_qry(folder, dimensions):
    datatype = {'TIME_PERIOD':'datetime2',
    'Date':'datetime2',
    'Month':'datetime2',
    'Year':'int',
    'Quarter':'int',
    # 'Creative_Text':'varchar(max)',
    # 'Width':'int',
    # 'Height':'int',
    # 'Landing_Page':'varchar(max)',
    'Link_to_Creative':'varchar(max)'}

    qry = dict()
    for d in dimensions.keys():
        base_qry =  f'''
        CREATE TABLE amex.{meta[folder]['short']}_dim_{d} (
        {d}_ID int IDENTITY(1,1) PRIMARY KEY,
        '''

        cnt = 0
        for col in dimensions[d].columns:
            no_of_cols = len(dimensions[d].columns)
            cnt += 1

            if cnt == no_of_cols:
                if col in datatype.keys():
                    base_qry = base_qry + f'{col} {datatype[col]} NOT NULL\n);'
                else:
                    base_qry = base_qry + f'{col} varchar(255) NOT NULL\n);'

            else:
                if col in datatype.keys():
                    base_qry = base_qry + f'{col} {datatype[col]} NOT NULL,\n'
                else:
                    base_qry = base_qry + f'{col} varchar(255) NOT NULL,\n'

        qry[d] = base_qry

    return qry

def create_table(dimensions, create_queries):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for k in create_queries.keys():
        # temp
        if k == 'crea':

            qry = create_queries[k]
            print(f'\n{qry}\n')
            cursor.execute(qry)
            conn.commit()
            conn.close()

    return None

def insert_data(folder, dimensions):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for d in dimensions.keys():
        for i, r in dimensions[d].iterrows():
            # if d == 'date':
            #     insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_dim_{d}({dimensions[d].columns[0]}, {dimensions[d].columns[1]}, {dimensions[d].columns[2]}, {dimensions[d].columns[3]}) values(?,?,?,?)"
            #     values = (r[dimensions[d].columns[0]], r[dimensions[d].columns[1]], r[dimensions[d].columns[2]], r[dimensions[d].columns[3]])

            if d == 'crea':
                insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_dim_{d}({dimensions[d].columns[0]}, {dimensions[d].columns[1]}) values(?,?)"
                values = (r[dimensions[d].columns[0]], r[dimensions[d].columns[1]])

            # else:
            #     insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_dim_{d}({dimensions[d].columns[0]}) values(?)"
            #     values = r[dimensions[d].columns[0]]

                cursor.execute(insert_qry, values)
                conn.commit()

    conn.close()

    return None

def main():
    for folder in os.listdir(datapath):
        if 'path' in folder:
            print(f'\n{folder}')
            time.sleep(5)

            # get the latest filepaht in dir
            print('\nGetting the list of files\n')
            time.sleep(5)
            list_of_files = glob(os.path.join(datapath, folder, meta[folder]['ext']))
            print(list_of_files)

            # load the file
            print('\nLoading files\n')
            time.sleep(5)
            df = load_file(folder, list_of_files)

            # dict objs
            print('Getting dimensions from loaded files and setting up create table qry\n')
            time.sleep(5)
            df_dims = df_dimensions(folder, df)
            qry = write_dimension_qry(folder, df_dims)

            df_dims['crea'].to_csv('temp.csv', index=False)

            # create table
            print('Creating db tables\n')
            time.sleep(5)
            create_table(df_dims, qry)

            # insert data
            # print('Inserting dimensions from loaded files to db')
            # time.sleep(5)
            # insert_data(folder, df_dims)

            print(f'{folder} done!\n')

    return None

    #     break


if __name__ == '__main__':
    main()
