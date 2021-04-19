from datetime import datetime
import time
import pandas as pd
import numpy as np
import os
import pyodbc
import glob
from static import *

# paths
DATA_PATH = paths['DATA_PATH']
OUTPUTS_PATH = paths['OUTPUTS_PATH']


def tables(folder, cursor):
	'''
	Returns dictionary of dimension tables names from MSSQL database.
		Parameters:
			folder (str): name of folders in datapath, either 00_pathmatics or 01_kantar
			cursor (pyodbc): database connection object to perform SQL operations
		Returns:
			dims (dict): table name (folder_dim removed) and full table name
		Example:
			{'adv':'path_dim_adv'}
	'''
	dims = dict()
	for row in cursor.tables():
		if meta[folder]['short'] == row.table_name.split('_')[0] and len(row.table_name.split('_')) > 2:
			# kant_dim & path_dim are 9 characaters long
			dims[row.table_name[9:]] = f'{row.table_schem}.{row.table_name}'

	dims['date'] = f"amex.{meta[folder]['short']}_dim_date"

	return dims

def load_dimensions(folder, connection):
	'''
	Returns dictionary of dimension table outputs from MSSQL database.
		Parameters:
			connection (dict): connection parameters for pyodbc module - see static.py for connection_string
		Returns:
			tbl_names (dict): table name (folder_dim removed) and dataframe

	'''
	print(f"\nConnecting to MSSQL\n{'-'*25}")
	conn = pyodbc.connect(connection)
	cursor = conn.cursor()

	tbl_names = tables(folder, cursor)

	for k in tbl_names.keys():
		qry = f'SELECT * FROM {tbl_names[k]};'
		tbl = pd.read_sql_query(qry, conn)

		# adjust for python datatype conversion
		if k == 'date':
			col = meta[folder]['freq'][1]
			tbl[col] = pd.to_datetime(tbl[col])
			tbl['Month'] = pd.to_datetime(tbl['Month'])

		elif k == 'crea':
			tbl['Creative_ID'] = pd.to_numeric(tbl['Creative_ID'])

		tbl_names[k] = tbl

	return tbl_names

def check_dimensions(folder, df, df_dims):
	'''
	Returns dictionary of new dimensions not found in the MSSQL dimension tables.
		Parameters:
			folder (str): name of folders in datapath, either 00_pathmatics or 01_kantar
			df (dataframe): new data from 02_data
			df_dim (dict): pre-defined dimensions - see static.py defined_dims
		Returns:
			missing_dims_in_db (dict): dictionary of missing dimensions from database, organized by short table name
			saved csv output of missing dims to 03_outputs
		Process Outline:
			1. Loop through each dimension key in the defined_dims obj in static.py
			2. Filter the new data by the dimension column
				2a. Drop duplicate data
				2b. Reset the index and drop index column
			3. Left Join the database dimension table to the filtered new data column
			4. Count the number of total NA's or missing values in the joined table
				4a. If there are any missing values, save the missing results in the repective 02_missing folder
				4b. Print prompt to notify user of missing dimensions

	'''
	tbls = load_dimensions(folder, connection_string)

	# house missing dataframes in dict
	missing_dims_in_db = dict()
	for dim in df_dims.keys():
#         print(f"[DIMENSION CHECK]: {dim}")
		time.sleep(1)
		df_ = df[df_dims[dim]].drop_duplicates().reset_index(drop=True)
		df_ = df_.merge(tbls[dim], how='left')

		# check for missing values
		count_of_missing = df_.isnull().sum().sum()
		if count_of_missing > 0:
			missing_dims_in_db[dim] = df_.iloc[df_[(df_.isnull().sum(axis=1) > 0)].index]

			print(f"[MISSING DIMENSION]:amex_{meta[folder]['short']}_{dim}\n")

			name = f"{meta[folder]['short']}_{dim}.csv"
			missing_dims_in_db[dim].to_csv(os.path.join(OUTPUTS_PATH, folder, '02_missing', name), index=False)

	return missing_dims_in_db

def insert_dimensions(folder, dimensions):
	'''
	Returns None - inserts results from check_dimensions() function.
		Parameters:
			folder (str): name of folders in datapath, either 00_pathmatics or 01_kantar
			dimensions (dict): dictionary of dimensions not found in the database - results from check_dimensions() function
		Returns:
			None
	'''
	if len(dimensions) > 0:
		print(f"\nInserting to MSSQL\n{'-'*25}")
		conn = pyodbc.connect(connection_string)
		cursor = conn.cursor()
		for d in dimensions.keys():
			print(f"Table: amex.{meta[folder]['short']}_dim_{d}")
			for i, r in dimensions[d].iterrows():
				if d == 'date':
					insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_dim_{d}({dimensions[d].columns[0]}, {dimensions[d].columns[1]}, {dimensions[d].columns[2]}, {dimensions[d].columns[3]}) values(?,?,?,?)"
					values = (r[dimensions[d].columns[0]], r[dimensions[d].columns[1]], r[dimensions[d].columns[2]], r[dimensions[d].columns[3]])

				elif d == 'crea':
					insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_dim_{d}({dimensions[d].columns[0]}, {dimensions[d].columns[1]}) values(?,?)"
					values = (r[dimensions[d].columns[0]], r[dimensions[d].columns[1]])

				else:
					insert_qry = f"INSERT INTO amex.{meta[folder]['short']}_dim_{d}({dimensions[d].columns[0]}) values(?)"
					values = r[dimensions[d].columns[0]]

				# print(f"{insert_qry} {values}")
				cursor.execute(insert_qry, values)
				conn.commit()
			print('')
		conn.close()

	return None

def load_file(folder, path):
	'''
	Returns modofied raw data from pathmatics or kantar
		Parameters:
			folder (str): name of folders in datapath, either 00_pathmatics or 01_kantar
			path (str): full path to the latest raw data file - lastest file determined by 'Date modfied'
		Returns:
			df (dataframe): dataframe
	'''
	if folder == '00_pathmatics':
		# pathmatics requires multiple data inputs
		dfs = [pd.read_csv(p, skiprows=1) for p in path]
		df = pd.concat(dfs, sort=False).reset_index(drop=True)
#         df = pd.read_csv(path, skiprows=1)

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
		# we want to grab the max kantar file in the directory
		p = max(path, key=os.path.getctime)
		df = pd.read_excel(p, sheet_name='Report', keep_default_na=False)

		# adjust NA
		df = df.replace('NA', '<NA>')
		df = df.replace('None', '<NA>')

		skip = df.index[df.iloc[:, 0] == 'TIME PERIOD'][0]
		df.columns = df.iloc[skip, :]
		df = df.iloc[skip+1:, :].reset_index(drop=True)
		df['TIME PERIOD'] = pd.to_datetime(df['TIME PERIOD'])

		# there are cases where the value is '' for the following columns
		df['AMEX CARD TYPE'] = df['AMEX CARD TYPE'].apply(lambda x: '<NA>' if x == '' else x)
		df['AMEX COBRAND'] = df['AMEX COBRAND'].apply(lambda x: '<NA>' if x == '' else x)

		df.rename(columns={'DOLS (000)':'DOLS_000'}, inplace=True)
		df.rename(columns={c:c.replace(' ','_') for c in df.columns}, inplace=True)

		# numeric adjustments
		df[meta[folder]['metrics']] = df[meta[folder]['metrics']].apply(pd.to_numeric)
		for metric in meta[folder]['metrics']:
			df[metric] = df[metric].astype(float)

		# control columns
		df = df[['TIME_PERIOD',
				 'QUARTER',
				 'YEAR',
				 'CATEGORY',
				 'MICROCATEGORY',
				 'PARENT',
				 'ADVERTISER',
				 'BRAND',
				 'PRODUCT',
				 'AMEX_CATEGORY',
				 'AMEX_CARD_TYPE',
				 'AMEX_COBRAND',
				 'AMEX_PARENT',
				 'AMEX_PRODUCT_TYPE',
				 'AMEX_SEGMENT',
				 'MEDIA',
				 'MEDIA_TYPE',
				 'UNITS',
				 'DOLS_000',
				 'DOLS']]

	return df

def qc_dimensions():
	'''
	Loops through folders in 02_data and checks for new dimensions to load to the database.
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

			# check for missing dimensions in the database
			missing_dimensions = check_dimensions(folder, df, df_dims)
			insert_dimensions(folder, missing_dimensions)

		except ValueError:
			print(f"No files in {folder}")


	return None
