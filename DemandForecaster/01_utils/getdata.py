'''
https://stackoverflow.com/questions/40565871/read-data-from-oecd-api-into-python-and-pandas
'''
# from pptx import Presentation
from hlpr import *
from config import *
# import requests as rq
from csse_covid import CSSE_COVID
import pandas as pd
import numpy as np
import requests
import time
import json
import re

# static
covid = CSSE_COVID()
OECD_ROOT_URL = "http://stats.oecd.org/SDMX-JSON/data"

def make_OECD_request(dsname, dimensions, params = None, root_dir = OECD_ROOT_URL):
    # Make URL for the OECD API and return a response
    # 4 dimensions: location, subject, measure, frequency
    # OECD API: https://data.oecd.org/api/sdmx-json-documentation/#d.en.330346

    if not params:
        params = {}

    dim_args = ['+'.join(d) for d in dimensions]
    dim_str = '.'.join(dim_args)

    url = root_dir + '/' + dsname + '/' + dim_str + '/all'

    # print('Requesting URL ' + url)
    return requests.get(url = url, params = params)


def create_DataFrame_from_OECD(country = 'CZE', subject = [], measure = [], frequency = 'M',  startDate = None, endDate = None):     
    # Request data from OECD API and return pandas DataFrame

    # country: country code (max 1)
    # subject: list of subjects, empty list for all
    # measure: list of measures, empty list for all
    # frequency: 'M' for monthly and 'Q' for quarterly time series
    # startDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
    # endDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations

    # Data download

    response = make_OECD_request('MEI'
                                 , [[country], subject, measure, [frequency]]
                                 , {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions'})

    # Data transformation
    if (response.status_code == 200):
        responseJson = response.json()
        obsList = responseJson.get('dataSets')[0].get('observations')

        if (len(obsList) > 0):
            # print('Data downloaded from %s' % response.url)
            timeList = [item for item in responseJson.get('structure').get('dimensions').get('observation') if item['id'] == 'TIME_PERIOD'][0]['values']
            subjectList = [item for item in responseJson.get('structure').get('dimensions').get('observation') if item['id'] == 'SUBJECT'][0]['values']
            measureList = [item for item in responseJson.get('structure').get('dimensions').get('observation') if item['id'] == 'MEASURE'][0]['values']

            obs = pd.DataFrame(obsList).transpose()
            obs.rename(columns = {0: 'series'}, inplace = True)
            obs['id'] = obs.index
            obs = obs[['id', 'series']]
            obs['dimensions'] = obs.apply(lambda x: re.findall('\d+', x['id']), axis = 1)
            obs['subject'] = obs.apply(lambda x: subjectList[int(x['dimensions'][1])]['id'], axis = 1)
            obs['measure'] = obs.apply(lambda x: measureList[int(x['dimensions'][2])]['id'], axis = 1)
            obs['time'] = obs.apply(lambda x: timeList[int(x['dimensions'][4])]['id'], axis = 1)
            obs['names'] = obs['subject'] + '_' + obs['measure']
            obs['location'] = [item for item in responseJson.get('structure').get('dimensions').get('observation') if item['id']=='LOCATION'][0]['values'][0]['id'] # cu add

            data = obs.pivot_table(index =['time','location'], columns = ['names'], values = 'series').reset_index() # cu add

            return(data)

        else:
            print('Error: No available records, please change parameters')

    else:
        print('Error: %s' % response.status_code)
        
def get_unemployment(start_year=2020, end_year=2020):
    '''
    Use API to get data from BLS site.
    '''
    # bls api
    key = '2c6ccdce65a244e99f82c8207aa7926e'

    mastermap_ = mastermap()
    bls_series_codes = list(mastermap_['state_meta']['bls_UnemploymentSeriesID'].values())

    cutoff = int(round(len(bls_series_codes)/2, 0))
    bls_codes_list = [bls_series_codes[:cutoff], bls_series_codes[cutoff:]]

    headers = {'Content-type': 'application/json'}
    json_list = []
    for bls_series_codes in bls_codes_list:
        time.sleep(1)
        data = json.dumps({"seriesid": bls_series_codes,"startyear":str(start_year), "endyear":str(end_year), "registrationKey":key})
        p = requests.post(f'https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        json_data = json.loads(p.text)
        json_list.append(json_data)

    if json_data['status'].upper() == 'REQUEST_SUCCEEDED':
        return json_list
    else:
        print('REQUEST_FAILED')
        return None
        
def get_apple():
	# global df
	driver_ = drvr()
	url = 'https://covid19.apple.com/mobility'
	driver_.get(url)
	time.sleep(5)

	elements = driver_.find_elements_by_xpath("//a[@href]")
	time.sleep(5)
	for e in elements:
		if 'applemobilitytrends' in e.get_attribute("href"):
  			data_url = e.get_attribute("href")
  			df = pd.read_csv(data_url)
  			return df
	
def get_oxford():
    '''
    Get data from the Our World in Data Github page.
    '''
    url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
    df = pd.read_csv(url)
    return df
    
def get_covid():
    covid_global = covid.get_data(True, True)
    covid_us_state = covid.get_data(False, True)

    return {'global':covid_global, 'state':covid_us_state}
    
# def get_sentiment():
#     folderpath = os.path.join(DATA_PATH, 'sentiment')
#     list_of_dataframes = []
#     for f in os.listdir(folderpath): 
#         file = os.path.join(folderpath, f)
    
#         # code start
#         prs = Presentation(file)
#         slides = prs.slides
        
#         market = f.split('_')[1]
        
#         slide_no = 0
#         dfs = pd.DataFrame()
#         for slide in slides:
#           sentiments = dict()
#           texts = []
#           slide_no += 1
#           # shapes are elements in powerpoint, such as textboxes or graphs
#           for shape in slide.shapes:
              
#               if shape.has_chart:
#                   # each sentiment [Pos, Neg, Neu] is a series obj
#                   for series in shape.chart.series:
#                       sentiments[series.name] = [value for value in series.values]
                  
#                   # categories are dates
#                   for series in shape.chart.plots:
#                       sentiments['Date'] = [cat for cat in series.categories]
                      
#               elif shape.has_text_frame:
#                   texts.append(shape.text)
        
#           # build dataframe
#           df = pd.DataFrame(sentiments)
#           df['Monitor'] = find_monitor(texts)
#           df['slide_no'] = slide_no
        
#           # add to dfs - master dataframe
#           dfs = pd.concat([dfs, df], axis=0, sort=False, ignore_index=True)
        
#         dfs = dfs.loc[np.where(-(dfs['Monitor'].isnull()))].reset_index(drop=True)
#         # dfs['Date'] = dfs['Date'].apply(to_date)
        
#         # reorder
#         dfs = dfs[['Date', 'slide_no', 'Monitor', 'Neutral', 'Negative', 'Positive']]
        
#         # find the categories and slide
#         adj_monitor = dict()
#         slide_no = 0
#         for slide in slides:
#             slide_no += 1
#             for shape in slide.shapes:
#                 if shape.has_text_frame:
#                     l = shape.text
#                     l = l.strip()
#                     if count_char(l, '_') == 2 and count_char(l, '|') == 0:
#                         adj_monitor[slide_no + 1] = l
        
#         # dfs['adj_Monitor'] = dfs['slide_no'].apply(lambda x: adj_monitor[x] if x in adj_monitor.keys() else None)
#         dfs['Monitor'] = dfs['slide_no'].apply(lambda x: adj_monitor[x] if x in adj_monitor.keys() else None)
#         dfs['Market'] = market
        
#         list_of_dataframes.append(dfs)
    
#     df_combined = pd.concat(list_of_dataframes)
#     df_combined = df_combined[['Date', 'Market', 'Monitor', 'Neutral', 'Negative', 'Positive']]
#     df_combined['Monitor'] = df_combined['Monitor'].str.strip('') 
    
#     return df_combined

