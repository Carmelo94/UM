from datetime import datetime, timedelta
import os
import getpass
import openpyxl
import pandas as pd
import numpy as np
from selenium import webdriver
from config import *
import calendar


def locate_branch(branch='ResponseEngineDataUpdate_v2'):
    try:
      cwd = os.getcwd()
      root = '/'.join(cwd.split('/')[:2])
      for r,d,f in os.walk(root):
        if  branch in r:
          fullpath = r
          break
      return fullpath

    except UnboundLocalError:
        print('File not located')
        return None
	
def mastermap():
	try:
		# load map file tabs
		filepath = os.path.join(UTILS_PATH, 'map.xlsx')
		wb = openpyxl.load_workbook(filepath, read_only=True)

		dict_ = dict()
		for sht in wb.sheetnames:
			df = pd.read_excel(filepath, sheet_name=sht)
			
			if sht == 'states':
				dict_['state_names'] = df.set_index('state').to_dict()['state_abbr']
				dict_['state_pop'] = df.set_index('state').to_dict()['census_popestimate2019']
				dict_['state_meta'] = df.set_index('state_abbr').to_dict()
				dict_['bls_state_id'] = df.set_index('bls_UnemploymentSeriesID').to_dict()['state_abbr']
			elif sht == 'markets':
			    dict_['market_meta'] = df.set_index('market').to_dict()
			    dict_['market_abbr'] = df[df['market']!='Great Britain'].set_index('market_abbr').to_dict()['market']
			elif sht == 'terms':
			    dict_['google_categories'] = df.iloc[:, :-1]
			elif sht == 'county_pop':
			    dict_['county_pop'] = df
			    
		return dict_

	except:
		print('Master mapping dictionary not created, check map.xlsx in 01_utils.')


def drvr():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=options)
    return driver
    
# sentiment function
def find_monitor(list_):
    if type(list_) is list:
        for l in list_:
            if l.lower().find('amexbae') == 0:
                return l[:l.find('|')-1]

# sentiment function                
def to_date(x):
    '''
    Convert string date from BrandIndex powerpoint to datetime.
    
    Uses calendar package to map month abbreviations to an integer.
    '''
    month_no = dict((v,k) for k,v in enumerate(calendar.month_abbr))
    try:
        date = int(x[:x.find(' ')])
        month = x[x.find(' '):].strip()
        return datetime(2020, month_no[month], date)
    except:
        return 'Error'
        
# sentiment function
    # sentiment function
def count_char(string, character):
    '''
    Count the number of a charater in a string.
    Parameters:
      string (str): string object
      chatacter (str): character we want a counted
    Returns:
      cnt (int): total number of a character in the string
    '''
    cnt = 0
    for i in string:
        if character == i:
            cnt += 1

    return cnt
    
def google_smly(df, frame, frame_base, metrics):
    df_smly = []

    for i in range(len(metrics[:-1])):
      df_temp = frame_base.copy()
      year = int(metrics[i+1].split('_')[-1])
      df_temp['year_no'] = year
      df_temp['index'] = (frame[metrics[i+1]]/frame[metrics[i]])-1
      df_temp['metric'] = '% Chg vs. Same Month Last Year'
      df_smly.append(df_temp)

    df_smly = pd.concat(df_smly).reset_index(drop=True)
    df_smly['month_temp'] = df_smly.apply(lambda x: datetime(x['year_no'], x['month_no'], 1), axis=1) 
    df_smly = df_smly[df_smly['month_temp'] <= df['month'].max()].iloc[:, :-1]

    return df_smly
    
def google_mom(df_grp):
    df_mom = df_grp.copy()
    df_mom.sort_values(by=['market', 'variable', 'month'], inplace=True)
    df_mom['index'] = df_mom.groupby(['market', 'variable'])['value'].transform(lambda x: x.pct_change())
    df_mom['metric'] = '% Chg vs. Previous Month'
    df_mom.drop(columns=['month', 'value'], inplace=True)

    return df_mom
    
def google_abs(df_grp):
    df_abs = df_grp.copy()
    df_abs['metric'] = 'Absolute Value'
    df_abs.rename(columns={'value':'index'}, inplace=True)
    df_abs.drop(columns=['month'], inplace=True)

    return df_abs
    
def google_covid(df_grp):
    df_covid = df_grp.copy()
    df_covid = df_covid[df_covid['year_no'] >= 2020]

    # grouped dataframe of covid start index
    df_covid_months = df_grp.loc[np.where((df_grp['month']>=datetime(2020,1,1)) & (df_grp['month']<datetime(2020,3,1)))]
    df_covid_months = df_covid_months.groupby(['market', 'variable'])['value'].mean().to_frame().reset_index()
    df_covid_months.rename(columns={'value':'covid_value'}, inplace=True)

    # join to dataframe for period >= 2020-03-01
    df_covid = df_covid[df_covid['month']>=datetime(2020,3,1)].merge(df_covid_months, how='left')

    # calculate index
    df_covid['index'] = (df_covid['value']/df_covid['covid_value'])-1
    df_covid['metric'] = '% Chg vs. Covid'
    df_covid.drop(columns=['month', 'value', 'covid_value'], inplace=True)

    return df_covid
    
def google_combine_mom(df_smly, df_mom, df_abs, df_covid, mkt_country, cat, dt):
    df_all = pd.concat([df_smly, df_mom, df_abs, df_covid], sort=False)

    # clean
    df_all['index'] = df_all['index'].replace(np.inf, np.nan)
    df_all['market_abbr'] = df_all['market'].str.strip()

    # map
    df_all['market'] = df_all['market'].apply(lambda x: mkt_country[x] if x in mkt_country.keys() else None)
    df_all = df_all[-df_all['market'].isna()].reset_index(drop=True)
    df_all = df_all.merge(cat, how='left', on='variable')
    df_all['geo'] = 'Global'

    # column order
    col_order = ['year_no', 'month_no', 'month_name', 'geo', 'market', 'market_abbr', 'Master Category', 'Category', 'variable', 'metric', 'index']
    df_all = df_all[col_order]
    df_all['last_updated'] = dt

    return df_all
    
def create_repo_folder():
    '''
    Create repository folder with processed data.
    '''
    dt = datetime.strftime(datetime.now(), '%Y%m%d')
    repo_path = os.path.join(REPO_PATH, dt)
    
    print(f"\nRepository: {repo_path}\n")
    
    if not os.path.exists(repo_path):
        os.makedirs(repo_path)
        
    return repo_path