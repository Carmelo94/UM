from datetime import datetime, timedelta
from hlpr import *
from getdata import *
from config import *
import calendar
import pandas as pd
import numpy as np
import os

import warnings
warnings.filterwarnings('ignore')

mastermap_ = mastermap()
priority_markets = [k for k in mastermap_['market_meta']['priority'].keys() if mastermap_['market_meta']['priority'][k]==1]
dt = datetime.strftime(datetime.now(), '%Y-%m-%d')

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

  return df_apple
  
def process_oxford():
  res = get_oxford()
  df = res[res['location'].isin(priority_markets)][['location', 'date', 'stringency_index']].reset_index(drop=True)
  df['date'] = pd.to_datetime(df['date'])
  df.rename(columns={'location':'market_name'}, inplace=True)
  
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
  df.rename(columns={'variable':'term'}, inplace=True)
  
  # join to category map
  df_join = mastermap_['google_categories'].merge(df, how='left')
  
  # create the aggregated view for the trend tab 
  df_join['day_of_year'] = df_join['date'].dt.dayofyear
  
  # group data
  df_grp = df_join.groupby(['date', 'day_of_year', 'market', 'master_category', 'category'])['value'].mean().reset_index()
  
  # add historical value
  df_grp_hist = df_grp[df_grp['date'].dt.year < 2020].groupby(['day_of_year', 'market', 'master_category', 'category'])['value'].mean().reset_index()
  df_grp_hist.rename(columns={'value':'value_3yr'}, inplace=True)
  
  # filter to current >= 2020
  df_grp_curr = df_grp[df_grp['date'].dt.year >= 2020].reset_index(drop=True)
  
  # set preliminary data
  df_prelim = df_grp_curr.merge(df_grp_hist, how='left', on=['day_of_year', 'market', 'master_category', 'category'])
  
  # set market columns
  df_prelim['market_name'] = df_prelim['market'].apply(lambda x: mkt_map[x])
  
  # column order
  cols = ['date','market_name','master_category','category','value','value_3yr']
  df_fnl = df_prelim[cols]
  
  return df_fnl
  
def process_covid_county_nyc():
    '''
    Get data from John Hopkings for NYC only.
    '''
    # initialize variables
    status_code = 404
    today = datetime.today()
    base_url = url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'

    # continue until status code changes
    # if there is an error reading the file then subtact a day from the today and try again
    while status_code == 404:
        today_formatted = datetime.strftime(today, '%m-%d-%Y')
        url = f"{base_url}{today_formatted}.csv"
        status_code = requests.get(url).status_code

        try:
            # print(f"Getting NYC data from: {url}")
            df = pd.read_csv(url)
            df['last_updated'] = datetime(today.year, today.month, today.day)
            lower_cols = {c:c.lower() for c in df.columns}
            df.rename(columns=lower_cols, inplace=True)

            # US
            #####################
            df_us = df[df['country_region']=='US']
            df_us = df_us[df_us['province_state'].isin(list(mastermap_['state_names'].keys()))] # filter to US States
            # map
            df_us['state'] = df_us['province_state'].apply(lambda x: mastermap_['state_names'][x])
            df_us['region'] = df_us['state'].apply(lambda x: mastermap_['state_meta']['region'][x])
            df_us['division'] = df_us['state'].apply(lambda x: mastermap_['state_meta']['division'][x])
            # adjust
            df_us['case_fatality_ratio'] = df_us['case_fatality_ratio']/100

            df_us.rename(columns={'admin2':'county', 
                                'province_state':'market_name', 
                                'long_':'lon', 
                                'confirmed':'cases', 
                                'case_fatality_ratio':'case_mortality'}, inplace=True) # Admin2 is County for the US only

            col_order = ['state', 'market_name', 'county', 'region', 'division', 'cases', 'deaths', 'case_mortality', 'last_updated']
            df_us = df_us[col_order]

            # NYC
            #####################
            nyc_counties = ['New York', 'Kings', 'Bronx', 'Richmond', 'Queens']
            df_ny = df_us[df_us['market_name']=='New York']
            df_nyc = df_ny[df_ny['county'].isin(nyc_counties)]

        except:
          today = today - timedelta(days=1)
          # print("Invalid date, trying again...")

    return df_nyc
  
def process_covid_county():
    '''
    Process county level data from the NY Times Github repo.
    Note: NYC boroughs grouped as 'New York City' and does not have an FIP. 
    '''
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    df = pd.read_csv(url)
    
    # filter data to the max date
    df_fltr = df[df['date']==df['date'].max()]
    df_fltr = df_fltr.groupby(['state', 'county', 'fips'])['cases', 'deaths'].sum().reset_index()
    # df_fltr = df_fltr.groupby(['state', 'county'])['cases', 'deaths'].sum().reset_index()
    df_fltr['case_mortality'] = df_fltr['deaths']/df_fltr['cases'] # calculate the case mortality
    
    # rename column and map to get some state features
    df_fltr.rename(columns={'state':'market_name'}, inplace=True)
    df_fltr['state'] = df_fltr['market_name'].apply(lambda x: mastermap_['state_names'][x] if x in mastermap_['state_names'].keys() else None)
    df_fltr['region'] = df_fltr['state'].apply(lambda x: mastermap_['state_meta']['region'][x] if x in mastermap_['state_meta']['region'].keys() else None)
    df_fltr['division'] = df_fltr['state'].apply(lambda x: mastermap_['state_meta']['division'][x] if x in mastermap_['state_meta']['division'].keys() else None)
    df_pre = df_fltr[-df_fltr['state'].isna()].reset_index(drop=True)
    
    # set the column order
    df_pre['last_updated'] = df['date'].max()
    col_order = ['state', 'market_name', 'county', 'region', 'division', 'cases', 'deaths', 'case_mortality', 'last_updated']
    df_pre = df_pre[col_order]
    
    # add NYC data
    df_nyc = process_covid_county_nyc()
    df_pre = pd.concat([df_pre, df_nyc], sort=False).reset_index(drop=True)
    
    # export data
    df_pre.to_csv(os.path.join(OUTPUT_PATH, 'df_covid_county.csv'), index=False)
    
    return None
      
def main():
  # results
  results = {
    'covid': process_covid(),
    'cci': process_cci(),
    'uempl-gl': process_uempl_gl(),
    'uempl-st': process_uempl_st(),
    'apple': process_apple(),
    'oxford': process_oxford(),
    'google': process_google_trend()}

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

  dims_gl = ['date', 'month', 'market_name', 'master_category', 'category']
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