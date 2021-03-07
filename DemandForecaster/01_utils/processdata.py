from datetime import datetime, timedelta
from hlpr import *
from getdata import *
from config import *
import calendar
import pandas as pd
import numpy as np
import os
import glob
import calendar

import warnings
warnings.filterwarnings('ignore')

mastermap_ = mastermap()
priority_markets = [k for k in mastermap_['market_meta']['priority'].keys() if mastermap_['market_meta']['priority'][k]==1]
dt = datetime.strftime(datetime.now(), '%Y-%m-%d')

repo_path = create_repo_folder()

# directory
# dir = locate_branch()

def create_frame(start_date = '2020-01-01', end_date= '2020-12-31'):
  '''
  Create the framed data for the response engine.
  '''
  dates = pd.date_range(start_date, end_date)

  state_abbr = mastermap_['state_meta']['state']
  market_abbr = mastermap_['market_abbr']

  dfs_list = []
  for s in state_abbr.keys():
    df = pd.DataFrame(dates, columns=['date'])
    df['month'] = df['date'].apply(lambda x: datetime(x.year, x.month, 1))
    df['market'] = s
    df['market_name'] = state_abbr[s]
    df['geo_type'] = 'state'
    dfs_list.append(df)

  for m in market_abbr.keys():
    df = pd.DataFrame(dates, columns=['date'])
    df['month'] = df['date'].apply(lambda x: datetime(x.year, x.month, 1))
    df['market'] = m
    df['market_name'] = market_abbr[m]
    df['geo_type'] = 'country'
    dfs_list.append(df)

  df = pd.concat(dfs_list).reset_index(drop=True)
  
  # save to repo
  df.to_csv(os.path.join(repo_path, 'frame.csv'), index=False)

  return df


def process_cci():
  '''
  Process downloaded CSV files.
  '''
  filepath = os.path.join(DATA_PATH, 'cci')
  dfs = [pd.read_csv(os.path.join(filepath, f)) for f in os.listdir(filepath)]
  df = pd.concat(dfs)
  
  # filter to monthly data
  df = df[df['Frequency']=='Monthly']
  
  # convert and filter date
  df['DateTime'] = pd.to_datetime(df['DateTime'])
  df = df[df['DateTime'].dt.year >= 2020]
  df['date'] = df['DateTime'].apply(lambda x: datetime(x.year, x.month, 1))
  
  # add 100 to any negative values
  df['value'] = df['Value'].apply(lambda x: x if abs(x)==x else 100+x)
  
  # get the baseline values, baseline starting 1/1/2020
  baselines = df[df['date']==df['date'].min()][['Country', 'value']].set_index('Country').to_dict()['value']
  df['index'] = df.apply(lambda x: (x['value']/baselines[x['Country']])*100, axis=1)
  
  df = df[['Country', 'date', 'Value', 'index']]
  df.rename(columns={'Country':'market_name', 'date':'month', 'Value':'cci_value', 'index':'cci_index'}, inplace=True)

  # save to repo
  df.to_csv(os.path.join(repo_path, 'cci.csv'), index=False)

  return df

def process_uempl_gl(start_date = '2020-01'):
  '''
  Process global unemployment data
  '''
  subject_code = 'LRHUTTTT'
  OECD_markets = {'CAN':'Canada','USA':'United States','MEX':'Mexico','GBR':'United Kingdom','AUS':'Australia'}
  
  dfs = [create_DataFrame_from_OECD(country=m, subject=[subject_code], startDate=start_date).reset_index() for m in OECD_markets.keys()]
  df = pd.concat(dfs)
  
  df['date'] = df['time'].apply(lambda x: datetime(int(x.split('-')[0]), int(x.split('-')[1]), 1))
  df['market'] = df['location'].apply(lambda x: OECD_markets[x])
  
  df['uempl_rate'] = df['LRHUTTTT_STSA']/100
  df = df[['market', 'date', 'uempl_rate']]
  df.rename(columns={'market':'market_name', 'date':'month'}, inplace=True)
  
  # save to repo
  df.to_csv(os.path.join(repo_path, 'uempl_gl.csv'), index=False)
  
  return df

def process_uempl_st(start_year=2020, end_year=2020):
  res = get_unemployment(start_year, end_year)
  
  month_names = {calendar.month_name[i].lower():i for i in range(1,13)}
  mastermap_ = mastermap()
  
  dfs = []
  for r in res:
    for s in r['Results']['series']:
        df_ = pd.DataFrame(s['data'])
        df_['seriesID'] = s['seriesID']
        dfs.append(df_)
  df = pd.concat(dfs)
  
  df['state'] = df['seriesID'].apply(lambda x: mastermap_['bls_state_id'][x] if x in mastermap_['bls_state_id'].keys() else None)
  df['state_name'] = df['state'].apply(lambda x: mastermap_['state_meta']['state'][x]) 
  df['month'] = df.apply(lambda x: datetime(int(x['year']), month_names[x['periodName'].lower()], 1), axis=1)
  
  df['uempl_rate'] = pd.to_numeric(df['value'])/100
  df.sort_values(by=['state', 'month'], inplace=True)
  df['uempl_mom'] = df.groupby('state')['uempl_rate'].transform(lambda x: x.diff())
  df['uempl_mom_%'] = df.groupby('state')['uempl_rate'].transform(lambda x: x.pct_change())
  df = df[['state', 'state_name', 'month', 'uempl_rate', 'uempl_mom', 'uempl_mom_%']]
  
  df.rename(columns={'state_name':'market_name'}, inplace=True)
  
  # save to repo
  df.to_csv(os.path.join(repo_path, 'uempl_st.csv'), index=False)
  
  return df

def process_apple():
  res = get_apple()

  # for processing
  cnt = 0
  for c in res.dtypes:
      if c == 'float':
          break
      cnt += 1

  id_vars = res.columns[:cnt]
  df = res.melt(id_vars=id_vars)


  # filter to US states
  df_us_0 = df.loc[np.where((df['geo_type']=='sub-region') & (df['country']=='United States'))]
  df_us_1 = df[df['region']=='Washington DC']
  df_us = pd.concat([df_us_0, df_us_1])
  df_us.loc[:, 'geo_type'] = 'state'

  # filter to priority markets
  df_pm = df[df['region'].isin(priority_markets)]
  df_pm.loc[:, 'geo_type'] = 'global'

  df_apple = pd.concat([df_us, df_pm])[['geo_type', 'region', 'transportation_type', 'variable', 'value']]
  df_apple.rename(columns={'variable':'date'}, inplace=True)

  # pviot transportation_type
  df_apple['date'] = pd.to_datetime(df_apple['date'])
  df_apple.fillna(0, inplace=True)
  df_apple = df_apple.pivot_table(index=['geo_type', 'region', 'date'], columns='transportation_type', values='value', aggfunc='mean').reset_index()
  
  df_apple.rename(columns={'region':'market_name'}, inplace=True)
  
  # save to repo
  df_apple.to_csv(os.path.join(repo_path, 'apple.csv'), index=False)

  return df_apple
  
def process_oxford():
  res = get_oxford()
  df = res[res['location'].isin(priority_markets)][['location', 'date', 'stringency_index']].reset_index(drop=True)
  df['date'] = pd.to_datetime(df['date'])
  df.rename(columns={'location':'market_name'}, inplace=True)
  
  # save to repo
  df.to_csv(os.path.join(repo_path, 'oxford.csv'), index=False)
  
  return df
  
def process_covid():
  res = get_covid()
  
  res_global = res['global'][res['global']['market'].isin(list(mastermap_['market_abbr'].values()))]
  res_us_total = res['state'].groupby(['date', 'market']).sum().reset_index()
  res_state = res['state'].iloc[:, 1:].rename(columns={'state':'market'})
  
  res_us_total['market'] = res_us_total['market'].replace('US', 'United States')
  res_all = pd.concat([res_global, res_us_total, res_state]).rename(columns={'market':'market_name'})
  res_all['population'] = res_all['market_name'].apply(lambda x: mastermap_['state_pop'][x] if x in mastermap_['state_pop'].keys() else None)
  res_all['geo_type'] = res_all['market_name'].apply(lambda x: 'global' if x in priority_markets else 'state')
  
  # save to repo
  res_all.to_csv(os.path.join(repo_path, 'covid.csv'), index=False)
  
  return res_all
    
def process_google_trend():
  '''
  Process Google historical and current data for the trend tab.
  '''
  # temp market map
  mkt_map = {'AU':'Australia','GB':'United Kingdom','US':'United States','CA':'Canada','MX':'Mexico'}
  
  # import google search results
  GOOGLE_PATH = os.path.join(DATA_PATH, 'google')
  
  df_list = []
  for f in os.listdir(GOOGLE_PATH):
    for d in os.listdir(os.path.join(GOOGLE_PATH, f)):
      df = pd.read_csv(os.path.join(GOOGLE_PATH, f, d))
      df_list.append(df)

  # combine
  df = pd.concat(df_list, sort=False).reset_index(drop=True)
  
  # adjust columns
  df['date'] = pd.to_datetime(df['date'])
  df = df[df['date']<df['date'].max()].reset_index(drop=True)
  df.rename(columns={'variable':'term'}, inplace=True)
  
  # join to category map
  df_join = mastermap_['google_categories'].merge(df, how='left')
  
  # create the aggregated view for the trend tab 
  df_join['day_of_year'] = df_join['date'].dt.dayofyear
  
  # group data
  df_grp = df_join.groupby(['date', 'day_of_year', 'market', 'Master Category', 'Category'])['value'].mean().reset_index()
  
  # add historical value
  df_grp_hist = df_grp[df_grp['date'].dt.year < 2020].groupby(['day_of_year', 'market', 'Master Category', 'Category'])['value'].mean().reset_index()
  df_grp_hist.rename(columns={'value':'value_3yr'}, inplace=True)
  
  # filter to current >= 2020
  df_grp_curr = df_grp[df_grp['date'].dt.year >= 2020].reset_index(drop=True)
  
  # set preliminary data
  df_prelim = df_grp_curr.merge(df_grp_hist, how='left', on=['day_of_year', 'market', 'Master Category', 'Category'])
  
  # set market columns
  df_prelim['market_name'] = df_prelim['market'].apply(lambda x: mkt_map[x] if x in mkt_map.keys() else None)
  df_prelim = df_prelim[-df_prelim['market_name'].isna()].reset_index(drop=True)
  
  # column order
  cols = ['date','market_name','Master Category', 'Category','value','value_3yr']
  df_fnl = df_prelim[cols]
  
  # save to repo
  df_fnl.to_csv(os.path.join(repo_path, 'google_trends.csv'), index=False)
  
  return df_fnl
  
def process_covid_county_nyc(date_cutoffs_dict):
    '''
    Get data from John Hopkings for NYC only.
      Parameters:
        date_cutoffs_dict (dict): dictionary of date cutoffs from process_covid_county function
    '''
    # loop thorugh the cutoffs, format in m-d-y
    report_dates = {datetime.strftime(datetime.strptime(k, '%Y-%m-%d %H:%M:%S'),'%m-%d-%Y'):date_cutoffs_dict[k] for k in date_cutoffs_dict.keys()}

    # base url without date
    base_url = url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'

    # nyc FIPS
    nyc_counties = {'Bronx': 36005.0, 'Kings': 36047.0, 'New York': 36061.0	, 'Queens': 36081.0	, 'Richmond': 36085.0}

    df_list = []
    for k in report_dates.keys():
      url = f"{base_url}{k}.csv"
      # status_code = requests.get(url).status_code

      # filter to NYC boroughs
      df_temp = pd.read_csv(url) 
      df_temp = df_temp[df_temp['FIPS'].isin(list(nyc_counties.values()))]
      
      # add and rename columns to match process_covid_data format
      df_temp['tag_cutoffs'] = report_dates[k]
      df_temp.rename(columns={'Province_State':'state', 'Admin2':'county', 'Confirmed':'cases', 'Deaths':'deaths', 'FIPS':'fips'}, inplace=True)
      
      # filter cols
      cols = ['county', 'state', 'fips', 'cases', 'deaths', 'tag_cutoffs']
      df_temp = df_temp[cols]
      df_list.append(df_temp)

    df_nyc = pd.concat(df_list, sort=False).reset_index(drop=True)
    
    # save to repo
    df_nyc.to_csv(os.path.join(repo_path, 'covid_nyc.csv'), index=False)

    return df_nyc

  
def process_covid_county(show=False):
    '''
    Process county level data from the NY Times Github repo.
    Note: NYC boroughs grouped as 'New York City' and does not have an FIP. 
        Parameters:
            show (bool): set to True to view dataframe results; for testing purposes only
    '''
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    df = pd.read_csv(url)

    # convert date object and get max date
    df['date'] = pd.to_datetime(df['date'])
    max_date = df['date'].max()

    # dictionary of cutoff dates based on the max date of the date
    date_cutoffs = {str(max_date): '',
                str(max_date - timedelta(days=7)): 'l7', 
                str(max_date - timedelta(days=14)): 'l14', 
                str(max_date - timedelta(days=21)): 'l21', 
                str(max_date - timedelta(days=28)): 'l28'}
    # return date_cutoffs

    # tag the data that contains the dates
    df['tag_cutoffs'] = df['date'].apply(lambda x: date_cutoffs[str(x)] if str(x) in list(date_cutoffs.keys()) else None)
    df = df[-df['tag_cutoffs'].isna()]
    df = df[-df['fips'].isna()]

    # add nyc data
    df_nyc = process_covid_county_nyc(date_cutoffs)
    df = pd.concat([df, df_nyc])
    df.reset_index(drop=True, inplace=True)

    # pivot data
    df_pvt = df.pivot_table(index=['state', 'county', 'fips'], columns='tag_cutoffs', values=['cases', 'deaths'])
    # return df_pvt
    
    # rename columns
    cols = [f"{c[0]}_{c[1]}" for c in df_pvt.columns]
    df_pvt.columns = cols
    df_pvt.reset_index(inplace=True)

    # calculate case mortality by each cutoff
    for i in list(date_cutoffs.values()):
      deaths_col = f"deaths_{i}"
      cases_col = f"cases_{i}"
      mort_col = f"case_mortality_{i}"

      # create metrics
      df_pvt[mort_col] = df_pvt[deaths_col] / df_pvt[cases_col]

      df_pvt[f"cases_v_{i}"] = df_pvt['cases_'] - df_pvt[cases_col]
      df_pvt[f"cases_v_{i}_perc"] = (df_pvt['cases_'] / df_pvt[cases_col]) - 1
      
      df_pvt[f"deaths_v_{i}"] = df_pvt['deaths_'] - df_pvt[deaths_col]
      df_pvt[f"deaths_v_{i}_perc"] = (df_pvt['deaths_'] / df_pvt[deaths_col]) - 1

      df_pvt[f"case_mortality_v_{i}"] = df_pvt['case_mortality_'] - df_pvt[mort_col]
      df_pvt[f"case_mortality_v_{i}_perc"] = (df_pvt['case_mortality_'] / df_pvt[mort_col]) - 1

    # drop columns 
    drop_cols = [c for c in df_pvt.columns if '__perc' in c or c[-2:] == 'v_']
    df_pvt.drop(columns=drop_cols, inplace=True)

    # rename
    rename_cols = {c:c[:-1] for c in df_pvt.columns if c[-1:] == '_'}
    df_pvt.rename(columns=rename_cols, inplace=True)


    # rename column and map to get some state features
    df_pvt.rename(columns={'state':'market_name'}, inplace=True)
    df_pvt['state'] = df_pvt['market_name'].apply(lambda x: mastermap_['state_names'][x] if x in mastermap_['state_names'].keys() else None)
    df_pvt['region'] = df_pvt['state'].apply(lambda x: mastermap_['state_meta']['region'][x] if x in mastermap_['state_meta']['region'].keys() else None)
    df_pvt['division'] = df_pvt['state'].apply(lambda x: mastermap_['state_meta']['division'][x] if x in mastermap_['state_meta']['division'].keys() else None)
    df_pre = df_pvt[-df_pvt['state'].isna()].reset_index(drop=True)

    # add population
    df_pre = df_pre.merge(mastermap_['county_pop'][['fips', 'POPESTIMATE2019']], how='left', on='fips')
    df_pre.rename(columns={'POPESTIMATE2019':'population'}, inplace=True)
    df_pre.drop(columns='fips', inplace=True)


    df_pre['last_updated'] = max_date

    # export data
    df_pre.to_csv(os.path.join(OUTPUT_PATH, 'df_covid_county.csv'), index=False)

    if show:
        return df_pre
    else:
        return None
      
def process_google_mom():
    # timestamp
    ts = int(time.time())
    dt = datetime.strftime(datetime.now(), '%Y%m%d')

    # paths
    ROOT = '/content/drive/My Drive/Colab Notebooks/AMEX/ResponseEngineDataUpdate_v2'
    UTILS_PATH = os.path.join(ROOT, '01_utils')
    OUTPUTS_PATH = os.path.join(ROOT, '03_outputs')
    GOOGLE_PATH = os.path.join(ROOT, '02_data/google')
    GOOGLE_YTD_PATH = os.path.join(GOOGLE_PATH, 'current')
    GOOGLE_HIST_PATH = os.path.join(GOOGLE_PATH, 'historical')
    GOOGLE_CITIES_PATH = os.path.join(GOOGLE_PATH, 'cities')

    # category mapping
    cat = pd.read_excel(os.path.join(UTILS_PATH, 'map.xlsx'), sheet_name='terms', usecols=[0,1,2])
    cat['term'] = cat['term'].str.strip().str.lower()
    cat.rename(columns={'term':'variable'}, inplace=True)

    # market mapping
    mkt = pd.read_excel(os.path.join(UTILS_PATH, 'map.xlsx'), sheet_name='markets')
    mkt_country = mkt[mkt['priority']==1].set_index('market_abbr').to_dict()['market']
    mkt_country['GB'] = 'United Kingdom'
    mkt_country['UK'] = 'United Kingdom'

    # import data
    paths = [GOOGLE_HIST_PATH, GOOGLE_YTD_PATH, GOOGLE_CITIES_PATH]
    data_paths = [max(glob.glob(os.path.join(p, '*.csv')), key=os.path.getmtime) for p in paths]
    df = pd.concat([pd.read_csv(d) for d in data_paths], sort=False).reset_index(drop=True)

    # clean columns
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] < df['date'].max()].reset_index(drop=True)
    df['variable'] = df['variable'].str.strip().str.lower()
    

    # add date variables
    df['month'] = df['date'].apply(lambda x: datetime(x.year, x.month, 1))
    df['month_no'] = df['date'].dt.month
    df['month_name'] = df['month_no'].apply(lambda x: calendar.month_abbr[x])
    df['year_no'] = df['date'].dt.year

    # group data
    df_grp = df.groupby(['month', 'month_no', 'month_name', 'year_no', 'market', 'variable'])['value'].mean().to_frame().reset_index()
    frame = df_grp[['month_no', 'month_name', 'market', 'variable']].drop_duplicates()

    # add year data to frame
    metrics = []
    for y in df_grp['year_no'].unique():
      df_year = df_grp.copy()
      df_year = df_year[df_year['year_no']==y].reset_index(drop=True)
      df_year.rename(columns={'value':f'value_{y}'}, inplace=True)
      df_year.drop(columns=['month', 'year_no'], inplace=True)
      frame = frame.merge(df_year, how='left')
      metrics.append(f'value_{y}')

    frame_base = frame.drop(columns=metrics)

    df_smly = google_smly(df, frame, frame_base, metrics)
    df_mom = google_mom(df_grp)
    df_abs = google_abs(df_grp)
    df_covid = google_covid(df_grp)
    df_all = google_combine_mom(df_smly, df_mom, df_abs, df_covid, mkt_country, cat, dt)

    # export
    df_all.to_csv(os.path.join(OUTPUTS_PATH, 'df_google_mom.csv'), index=False)      
      
def main():
  # results
  results = {
    'covid': process_covid(),
    'cci': process_cci(),
    'uempl-gl': process_uempl_gl(),
    'uempl-st': process_uempl_st(end_year=2021),
    'apple': process_apple(),
    'oxford': process_oxford(),
    'google': process_google_trend(), 
    'google_mom': process_google_mom()
  }

  # frames
  global_frame = results['google']
  global_frame['month'] = global_frame['date'].apply(lambda x: datetime(x.year, x.month, 1)) # add month

  state_frame = results['covid'][results['covid']['geo_type']=='state']
  state_frame['month'] = state_frame['date'].apply(lambda x: datetime(x.year, x.month, 1)) # add month

  # global data
  global_data = global_frame \
    .merge(results['covid'], how='left', on=['date', 'market_name']) \
    .merge(results['cci'], how='left', on=['month', 'market_name']) \
    .merge(results['uempl-gl'], how='left', on=['month', 'market_name']) \
    .merge(results['apple'], how='left', on=['date', 'market_name']) \
    .merge(results['oxford'], how='left', on=['date', 'market_name']) 

  dims_gl = ['date', 'month', 'market_name', 'Master Category', 'Category']
  metrics_gl = [c for c in [c for c in global_data.columns if not('geo_type' in c.lower())] if not(c in dims_gl)]
  global_data = global_data[dims_gl + metrics_gl]

  # state data
  state_data = state_frame \
    .merge(results['uempl-st'], how='left', on=['month', 'market_name']) \
    .merge(results['apple'], how='left', on=['date', 'market_name'])
    
  state_post_join = len(state_data)
  state_data['state'] = state_data['market_name'].apply(lambda x: mastermap_['state_names'][x] if x in mastermap_['state_names'].keys() else None)
  state_data = state_data[-state_data['state'].isna()].reset_index(drop=True)

  dims_st = ['date', 'month', 'market_name', 'state']
  metrics_st = [c for c in [c for c in state_data.columns if not('geo_type' in c.lower())] if not(c in dims_st)]
  state_data = state_data[dims_st + metrics_st]

  global_data['last_updated'] = dt
  state_data['last_updated'] = dt
  
  if len(global_frame) == len(global_data) and len(state_frame) == state_post_join:
    print('Exporting Data')
    global_data.to_csv(os.path.join(OUTPUT_PATH, 'df_global.csv'), index=False)
    state_data.to_csv(os.path.join(OUTPUT_PATH, 'df_state.csv'), index=False)
    process_covid_county()
    print('Done!')

  else:
    print('Data Join Duplication')
    print(f"Global: global_frame {len(global_frame)}, global_data {len(global_data)}")
    print(f"State: state_frame {len(state_frame)}, state_data {len(state_data)}")

  return None
  
if __name__ == "__main__":
  main()