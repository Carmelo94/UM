from datetime import datetime, timedelta
from itertools import chain
import pandas as pd
import numpy as np
import glob
import os
import openpyxl

import sys
sys.path.append('../utils')
from SharePoint import *
from hlpr import *
from static import *
from format_qa import *

import warnings
warnings.filterwarnings('ignore')

def media_qa(media, redshift_metrics_filename, redshift_qa_query):
    '''
    QA data by media type.
    '''
    print(f"\nStarting QA")

    if media == 'search':
        return search_qa(redshift_metrics_filename, redshift_qa_query)
    elif media == 'social':
        return social_qa(redshift_metrics_filename, redshift_qa_query)
    elif media == 'digital':
        return digital_qa(redshift_metrics_filename, redshift_qa_query)

def search_qa(redshift_metrics_filename, redshift_qa_query):
    # print('starting qa\n')
    time.sleep(1)
# import metrics
# ========================================================================================================================
    pth = glob.glob(os.path.join(ASSETS_PATH, f"*{redshift_metrics_filename}*"))[0]
    df_metrics = pd.read_excel(pth, sheet_name='FloodLight')

# import sa360 data
# add to error log for metric mismatches
# ========================================================================================================================
    list_of_floodlights = []
    list_of_dfs = []

    files = glob.glob(os.path.join(DATA_PATH, '*.xlsx'))
    for f in files:
        df = pd.read_excel(f)

        # column check and add floodlights
        if list(df.columns[:17]) == sa360_col_check:
            list_of_floodlights.append(list(df.columns[17:]))
        else:
            print(f'\ncolumn name mismatch: {f}\n')

        # transpose sa360 data
        id_vars = list(df.columns[:11])
        df_trans = df.melt(id_vars = id_vars)
        list_of_dfs.append(df_trans)

# combine sa360 data
# ========================================================================================================================
    df_ui = pd.concat(list_of_dfs)
    df_ui = df_ui[-df_ui['variable'].isin(['CTR', 'Avg CPC', 'Avg pos'])].reset_index(drop=True)

# create database aruments
# add to error log if floodlight mismatch
# ========================================================================================================================
    db_advertisers = "','".join(list(df_ui['Account'].unique()))
    db_start_date = datetime.strftime(df_ui['From'].min(), '%Y-%m-%d')
    db_end_date = datetime.strftime(df_ui['From'].max() + timedelta(days=6), '%Y-%m-%d')

    # adjust floodlights
    floodlights_columns = list(chain(*list_of_floodlights)) # unpack list
    floodlights_columns = list(dict.fromkeys(floodlights_columns,None).keys()) # remove dupes

    # filter metrics
    floodlights_names = df_metrics[df_metrics['sa360_col_name'].isin(floodlights_columns)]
    db_metrics = ['clicks', 'impr', 'cost']

    if len(floodlights_names['sa360_col_name'].unique()) == len(floodlights_columns):
        db_metrics = "','".join(list(floodlights_names['metric']) + db_metrics)
    else:
        db_metrics = "','".join(db_metrics)

# get data from redshift
# ========================================================================================================================
    qry_txt = get_qry_text(redshift_qa_query)
    qry_txt = qry_txt.replace('load_metrics', db_metrics) \
                     .replace('load_advertisers', db_advertisers) \
                     .replace('load_start_date', db_start_date) \
                     .replace('load_end_date', db_end_date)

    time.sleep(1)
    print(f"    Querying search")
    df_qry_raw = run_qry(qry_txt)

# map floodlights and remove nulls
# add nulls to error log
# ========================================================================================================================
    df_qry = df_qry_raw.copy()
    df_qry = df_qry.merge(floodlights_names, how='left', on=['advertiser', 'metric'])
    df_qry['sa360_col_name'] = df_qry.apply(lambda x: x['metric'].title() if x['metric'] in ['impr', 'cost', 'clicks'] else x['sa360_col_name'], axis=1)

    # null results
    df_qry_na = df_qry[df_qry['sa360_col_name'].isna()]
    df_qry_na_metrics = df_qry_na[['advertiser', 'metric']].drop_duplicates().reset_index(drop=True)

    if len(df_qry_na_metrics.merge(floodlights_names, how='inner')) > 0:
        print('advertiser and metric combination is not in df_qry, check sa360_metrics.xlsx')
        print(df_qry_na_metrics)

# set up the qa dataframe
# ========================================================================================================================
    df_qry = df_qry[-df_qry['sa360_col_name'].isna()]

    # align ui column names
    ui_col_rename = {
        'From': 'week',
        'Account': 'advertiser',
        'Campaign': 'campaign_name',
        'variable': 'sa360_col_name',
        'value': 'value'
    }

    # filter ui and database results columns
    df_ui_fltr = df_ui[ui_col_rename.keys()]
    df_ui_fltr.rename(columns=ui_col_rename, inplace=True)
    df_qry_fltr = df_qry[ui_col_rename.values()]

    # add source column
    df_qry_fltr['source'], df_ui_fltr['source'] = 'redshift', 'sa360'

    # combine and adjust
    df_qa = pd.concat([df_qry_fltr, df_ui_fltr])
    df_qa['advertiser'] = df_qa['advertiser'].str.replace('Amex - Shop small', 'Amex - Shop Small')
    df_qa['value'] = np.round(df_qa['value'])

    # align dates
    df_qa['week'] = pd.to_datetime(df_qa['week'])
    df_qa['week'] = df_qa['week'].apply(lambda x: x - timedelta(days=x.weekday()))

# set up the dictionary file to save results
# ========================================================================================================================
    pvts_dict = dict()

    # details
    db_params_dict = {'last_updated':datetime.now(), 'advertisers': db_advertisers, 'start_date': db_start_date, 'end_date': db_end_date, 'sql_qry': qry_txt}
    details = pd.DataFrame.from_dict(db_params_dict, orient='index').reset_index()
    details.columns = ['variable', 'value']
    pvts_dict['details'] = details

    # create qa views
    df_v0 = df_qa.copy()
    df_v0 = df_v0.pivot_table(index=['advertiser', 'campaign_name', 'week', 'sa360_col_name'], columns=['source'], values='value', aggfunc='sum').reset_index().fillna(0)
    df_v0 = df_v0.melt(id_vars=['advertiser', 'campaign_name', 'week', 'sa360_col_name'])
    df_v0 = df_v0.sort_values(by=['advertiser', 'campaign_name', 'week', 'sa360_col_name']).reset_index(drop=True)
    df_v0.rename(columns={'sa360_col_name':'metric'}, inplace=True)

    # redshift v sa360
    df_v1 = df_qa.copy()

    df_v1 = df_v1.pivot_table(index=['advertiser', 'campaign_name', 'week', 'sa360_col_name'], columns=['source'], values='value', aggfunc='sum').reset_index()
    df_v1['%_diff'] = abs((df_v1['redshift']/df_v1['sa360'])-1)
    df_v1.rename(columns={'sa360_col_name':'metric'}, inplace=True)

    view_args = {
        'advertiser': {'index':['advertiser'],
                       'dim_cutoff': 1},

        'campaign': {'index':['advertiser', 'campaign_name'],
                     'dim_cutoff': 2},

        'week': {'index':['advertiser', 'campaign_name', 'week'],
                 'dim_cutoff': 3}
                }

    # loop to calculate difference by view args keys
    for k in view_args.keys():
        df_pvt = df_qa_week = df_qa.pivot_table(index=view_args[k]['index'], columns=['sa360_col_name', 'source'], values='value', aggfunc='sum').reset_index()

        list_of_metric_diff = []
        metrics = df_pvt.columns.levels[0][:list(df_pvt.columns.levels[0]).index(view_args[k]['index'][-1])]
        for m in metrics:
            df_temp = df_pvt.copy()
            df_temp = df_temp[m]
            df_temp.columns = [f"{m}_{c}" for c in df_temp.columns]

            # fill na based on condition
            col0 = df_temp.columns[0]
            col1 = df_temp.columns[1]
            df_temp[col0] = df_temp.apply(lambda x: 0 if np.isnan(x[col0]) and not(np.isnan(x[col1])) else x[col0], axis=1)
            df_temp[col1] = df_temp.apply(lambda x: 0 if np.isnan(x[col1]) and not(np.isnan(x[col0])) else x[col1], axis=1)

            df_temp[f"{m}_%_diff"] = (df_temp.iloc[:,1]/df_temp.iloc[:,0])-1
            df_temp[f"{m}_%_diff"] = df_temp[f"{m}_%_diff"].apply(lambda x: 1 if x == float('inf') else x)

            list_of_metric_diff.append(df_temp)

        metric_diffs = pd.concat(list_of_metric_diff, axis=1)

        df_pvt_qa = pd.concat([df_pvt.iloc[:, :view_args[k]['dim_cutoff']], metric_diffs], axis=1)
        df_pvt_qa.columns = [c[0] if type(c) is tuple else c for c in df_pvt_qa.columns]

        pvts_dict[k] = df_pvt_qa

    pvts_dict['raw_data'], pvts_dict['redshift_v_sa360'] = df_v0, df_v1

# remove campaigns with no value
# ========================================================================================================================
    campaign_qa = pvts_dict['campaign'].copy()
    remove_campaigns = list(campaign_qa[campaign_qa.iloc[:, 2:].sum(axis=1) == 0.0]['campaign_name'].unique())

    # clean dictionary
    keys = list(pvts_dict.keys())
    for k in keys[2:]:
        pvts_dict[k] = pvts_dict[k][-(pvts_dict[k]['campaign_name'].isin(remove_campaigns))]
        pvts_dict[k].reset_index(drop=True)

    print('qa completed\n')

    return pvts_dict

def social_qa(redshift_metrics_filename, redshift_qa_query):
    # print('starting qa\n')
    time.sleep(1)

# import metrics
# ========================================================================================================================
    pth = glob.glob(os.path.join(ASSETS_PATH, f"*{redshift_metrics_filename}*"))[0]
    df_metrics = pd.read_excel(pth, sheet_name='metric_map')

# import and qa loop
# ========================================================================================================================
    platforms = ['fb', 'li', 'pi', 'tw']
    social_dict = dict()

    for p in platforms:
        pth = glob.glob(os.path.join(DATA_PATH, f"{p}*")) # starts with platform abbreviation

        if len(pth) == 0:
            continue

        # import data
        social_dict[p] = import_social(p, pth, df_metrics)

        # query database
        qry_txt = get_qry_text(redshift_qa_query)
        qry_txt = qry_txt.replace('load_metrics', social_dict[p]['params']['metrics']) \
                         .replace('load_campaign_name', social_dict[p]['params']['campaign']) \
                         .replace('load_start_date', social_dict[p]['params']['start']) \
                         .replace('load_end_date', social_dict[p]['params']['end']) \
                         .replace('load_adset_id', social_dict[p]['params']['adset'])

        if p == 'li':
            qry_txt = qry_txt.replace('AND universal_adset_id IN', 'AND universal_adset IN')

        print(f"    Querying {p}")
        df_qry_raw = run_qry(qry_txt)
        social_dict[p]['qry'] = qry_txt
        social_dict[p]['qry_results'] = df_qry_raw

        # combine to create qa file
        social_dict[p]['qa_results'] = create_social_qa(p, social_dict[p]['pro_data'], social_dict[p]['qry_results'], df_metrics)

    return social_dict

def digital_qa(redshift_metrics_filename, redshift_qa_query):
    # print('starting qa\n')
    time.sleep(1)

# import metrics
# ========================================================================================================================
    pth = glob.glob(os.path.join(ASSETS_PATH, f"*{redshift_metrics_filename}*"))[0]
    df_metrics = pd.read_excel(pth, sheet_name='ConvMap')

    campaign_name_list = [c for c in df_metrics['Campaign'].unique()]
    campaign_ids_list = [str(c) for c in df_metrics['CampaignID'].unique()]
    activity_list = [str(a) for a in df_metrics['ActivityID'].dropna().unique()]
    core_metrics = ['Impressions', 'Clicks', 'Media Cost', 'Video Plays', 'Video Views', 'Video Completions']

# process raw DCM and site served data
# ========================================================================================================================
    df_ui = process_dcm_ui(campaign_ids_list, core_metrics, activity_list)
    df_ss_template = process_ss_templates(campaign_name_list)

# get database parameters from DCM raw data
# ========================================================================================================================
    db_campaign_id = "','".join(list(df_ui['Campaign ID'].map(str).unique()))
    db_placement_id = "','".join(list(df_ui['Placement ID'].map(str).unique()))
    db_start_date = datetime.strftime(df_ui['Date'].min(), '%Y-%m-%d')
    db_end_date = datetime.strftime(df_ui['Date'].max(), '%Y-%m-%d')
    db_metrics = "','".join(list(df_ui['metric'].unique()))

# get data from redshift
# ========================================================================================================================
    qry_txt = get_qry_text(redshift_qa_query)
    qry_txt = qry_txt.replace('load_metrics', db_metrics) \
                     .replace('load_campaign_id', db_campaign_id) \
                     .replace('load_placement_id', db_placement_id) \
                     .replace('load_start_date', db_start_date) \
                     .replace('load_end_date', db_end_date)

    time.sleep(1)
    print(f"    Querying dcm")
    df_qry_raw = run_qry(qry_txt)

# combine ui and db data
# ========================================================================================================================
    df_qry = df_qry_raw.copy()
    df_qry_dcm = df_qry[df_qry['source']!='override'] # contains dcm & cadreon
    df_qry_ss = df_qry[df_qry['source']=='override']

    df_qa = combine_ui_db(df_qry_dcm, df_qry_ss, df_ui, df_ss_template)

# create qa file
# ========================================================================================================================
    pvts_dict = create_digital_qa(df_qa, db_start_date, db_end_date, qry_txt)

    return pvts_dict
