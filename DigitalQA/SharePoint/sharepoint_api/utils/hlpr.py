from datetime import datetime, timedelta
from static import *
import pandas as pd
import numpy as np
import psycopg2
import sys
import glob
import os
import shutil
import app_settings
import re
import zipfile

try:
    from office365.sharepoint.file_creation_information import FileCreationInformation
    from office365.runtime.auth.authentication_context import AuthenticationContext
    from office365.sharepoint.folder_collection import FolderCollection
    from office365.sharepoint.client_context import ClientContext
    from office365.runtime.client_request import RequestOptions
    from office365.sharepoint.listitem import ListItem
    from office365.sharepoint.folder import Folder
    from office365.sharepoint.file import File

except:
    from office365.sharepoint.files.file_creation_information import FileCreationInformation
    from office365.runtime.auth.authentication_context import AuthenticationContext
    from office365.sharepoint.folders.folder_collection import FolderCollection
    from office365.sharepoint.client_context import ClientContext
    # from office365.runtime.client_request import RequestOptions
    from office365.sharepoint.listitems.listitem import ListItem
    from office365.sharepoint.folders.folder import Folder
    from office365.sharepoint.files.file import File

def create_folders():
    # paths
    folder = 'sharepoint_api'

    cur_dir = os.getcwd()
    idx = cur_dir.lower().find(folder)
    ROOT = os.path.join(cur_dir[:idx], folder)

    branches = ['assets', 'data', 'outputs', 'utils']
    ASSETS_PATH, DATA_PATH, OUTPUTS_PATH, UTILS_PATH = {os.path.join(ROOT, branch): os.makedirs(os.path.join(ROOT, branch), exist_ok=True) for branch in branches}.keys()

    return ASSETS_PATH, DATA_PATH, OUTPUTS_PATH, UTILS_PATH

def create_case_when(x):
    '''
    Dynamicall create the 'ISNULL(SUM(CASE WHEN))' qry for a given input
    '''
    qry = f'''ISNULL(SUM(CASE WHEN metric = '{x}' THEN value ELSE NULL END),0) "{x}"'''
    return qry

def unzip():
    '''
    Extract all contents from ZIP files.
    '''
    files = glob.glob(os.path.join(DATA_PATH, '*.zip'))

    for f in files:
        filename, file_extension = os.path.splitext(f)
        if file_extension.lower()=='.zip':
            with zipfile.ZipFile(f, 'r') as zip_ref:
                zip_ref.extractall(DATA_PATH)

def get_qry_text(table_qry):
    '''
    Read in the qry as text.
    '''
    path = glob.glob(os.path.join(ASSETS_PATH, f'*{table_qry}*'))[0]
    qry = open(path, 'r')
    qry = qry.read()

    return qry

def run_qry(qry):
    '''
    Execute query.
        Parameters:
            qry (str): user defined qry
    '''
    # connect to rs
    con = psycopg2.connect(**rs_cred)
    cur = con.cursor()

    # read into dataframe
    results = pd.read_sql(qry, con)

    # close connection
    con.close()

    return results

def get_app_settings(site_name):
    return app_settings.settings[site_name]

def ctx_hlpr(sharepoint_url, client_id, client_secret):
    '''Authorize Application'''
    ctx_auth = AuthenticationContext(url=sharepoint_url)
    ctx_auth.acquire_token_for_app(client_id=client_id, client_secret=client_secret)
    ctx = ClientContext(sharepoint_url, ctx_auth)
    print(f"Connected to SharePoint: {sharepoint_url}")
    return ctx

def printAllContents(ctx, relativeUrl):
    '''
    List files and folders in URL path.
    Source: https://github.com/vgrem/Office365-REST-Python-Client/issues/307
    '''
    file_list = []
    folder_list = []

    try:
        libraryRoot = ctx.web.get_folder_by_server_relative_url(relativeUrl)
        # print(1)
        ctx.load(libraryRoot)
        ctx.execute_query()

        folders = libraryRoot.folders
        ctx.load(folders)
        ctx.execute_query()
        # print(2)

        for myfolder in folders:
            folder_list.append(myfolder.properties["ServerRelativeUrl"])
#             print("Folder name: {0}".format(myfolder.properties["ServerRelativeUrl"]))
            printAllContents(ctx, relativeUrl + '/' + myfolder.properties["Name"])

        files = libraryRoot.files
        ctx.load(files)
        ctx.execute_query()

        for myfile in files:
            file_list.append(myfile.properties["ServerRelativeUrl"])
#             print("File name: {0}".format(myfile.properties["ServerRelativeUrl"]))
    except:
        sys.exit(1)

    return {'files': file_list, 'folders':folder_list}

def upload_files_hlpr(ctx, sharepoint_upload_path, local_file_path, info):
    '''Upload files to from local to SharePoint folder'''

    with open(local_file_path, 'rb') as content_file:
        info.content = content = content_file.read()

    info.url = os.path.split(local_file_path)[-1]
    info.overwrite = True

    # print('preparing upload')
    folder = ctx.web.get_folder_by_server_relative_url(sharepoint_upload_path)
    target_file = folder.files.add(info)
    ctx.execute_query()
    # print(f"file uploaded: {os.path.join(sharepoint_upload_path, info.url)}")

def findn(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n = n - 1
    return start

def delete_local_data(delete_all=True):
    '''
    Delete local files/folders in DATA_PATH and OUTPUTS_PATH.
        Parameters:
            delete_all (bool): True will delete local data in DATA_PATH and OUTPUTS_PATH, False deletes data in DATA_PATH
    '''
    paths_dict = {
        True: [
            [glob.glob(os.path.join(path, '*')) for path in [DATA_PATH]],
            'data and outputs folders'],
        False: [
            [glob.glob(os.path.join(path, '*')) for path in [DATA_PATH]],
            'data folder']
        }

    print(f"\nDeleting contents in {paths_dict[delete_all][1]}")

    paths = paths_dict[delete_all][0]
    if delete_all:
        paths = paths[0] + paths[1]
    else:
        paths = paths[0]

    for p in paths:
        try:
            os.remove(p)
        except PermissionError:
            shutil.rmtree(p)
        except FileNotFoundError:
            continue

def import_social(platform, path, df_metrics):
    '''Conditionally import social data.'''

    if platform == 'fb':
        dfs_list = [pd.read_csv(i) for i in path]
        df_pre = preprocess_social(dfs_list, platform, df_metrics)
        db_params = get_db_params(platform, df_pre, df_metrics)
        df_pro = process_social(platform, df_pre)

    elif platform == 'li':
        dfs_list = [pd.read_csv(i, skiprows=5, encoding='cp1252') for i in path]
        df_pre = preprocess_social(dfs_list, platform, df_metrics)
        db_params = get_db_params(platform, df_pre, df_metrics)
        df_pro = process_social(platform, df_pre)

    elif platform == 'pi':
        dfs_list = [pd.read_csv(i) for i in path]
        df_pre = preprocess_social(dfs_list, platform, df_metrics)
        db_params = get_db_params(platform, df_pre, df_metrics)
        df_pro = process_social(platform, df_pre)

    elif platform == 'tw':
        dfs_list = [pd.read_excel(i) for i in path]
        df_pre = preprocess_social(dfs_list, platform, df_metrics)
        # print("EXPORTING TWITTER PRE DATA")
        # df_pre.to_csv('dnu.csv', index=False)
        db_params = get_db_params(platform, df_pre, df_metrics)
        df_pro = process_social(platform, df_pre)

    return {'pre_data':df_pre, 'params':db_params, 'pro_data':df_pro}

def add_twitter_conversion(platform, df_pre):
    '''
    Adds Twitter conversions/floodlights to preprocessed dataframe.
        Parameters:
            platform (str): platform identifier
            dfs_list (str, path): list of paths to the raw platform file in the local data folder
            df_pre (dataframe): preprocessed dataframe
        Returns:
            df_pre_fnl (dataframe): preprocessed dataframe with twitter conversion metrics
    '''
    if platform != 'tw':
        return df_pre
    else:
# get floodlights
        pth = glob.glob(os.path.join(ASSETS_PATH, "*Social_Metrics*"))[0]
        df_metrics = pd.read_excel(pth, sheet_name='tw_conversions')
        floodlight = list(df_metrics['floodlight_name'].unique()) # filter dcm data by these floodlights

# split ad group name to get placement id
        df_ui_plcmnt = df_pre.copy()
        df_ui_plcmnt['Time period'] = pd.to_datetime(df_ui_plcmnt['Time period'])
        df_ui_plcmnt['Placement ID'] = df_ui_plcmnt['Ad Group name'].apply(lambda x: x.split('_')[-1])
        df_ui_plcmnt = df_ui_plcmnt[['Time period', 'Campaign name', 'Campaign ID', 'Ad Group name', 'Ad Group ID', 'Placement ID']].drop_duplicates()
        df_ui_plcmnt.rename(columns={'Time period':'Date'}, inplace=True)

# import dcm data
        pth = glob.glob(os.path.join(DATA_PATH, '553*'))
        if len(pth)==0:
            return df_pre
        else:
            df_dcm = pd.read_csv(pth[0], skiprows=10).iloc[:-1, :]
            df_dcm['Date'] = pd.to_datetime(df_dcm['Date'])

            # filter out columns
            dcm_cols = ['Date', 'Campaign', 'Campaign ID', 'Placement ID', 'Activity Group', 'Activity Group ID', 'Activity', 'Activity ID', 'Click-through Conversion Events + Cross-Environment','View-through Conversion Events + Cross-Environment']
            df_dcm_conv = df_dcm[df_dcm['Activity'] != '(not set)'][dcm_cols]

            # inner join on date and placement id
            df_dcm_tw = df_dcm_conv.merge(df_ui_plcmnt[['Date', 'Placement ID']].drop_duplicates(), how='inner', on=['Date', 'Placement ID'])

            # return df_pre is len == 0
            if len(df_dcm_tw) == 0:
                print('\nno twitter conversions\n')
                return df_pre

    # create the floodlight name to align with redshift
            id_vars = df_dcm_tw.columns[:-2]
            df_dcm_tw = df_dcm_tw.melt(id_vars=id_vars)

            # create floodlight name
            df_dcm_tw['metric'] = df_dcm_tw.apply(lambda x: x['Activity Group'] + ' : ' + x['Activity'] + ': ' + x['variable'], axis=1)

            df_dcm_tw = df_dcm_tw[df_dcm_tw['metric'].isin(floodlight)]
            df_dcm_tw = df_dcm_tw.pivot_table(index=['Date', 'Placement ID'], columns='metric', values='value', aggfunc='sum').reset_index()

    # join data
            df_tw_joined = df_ui_plcmnt.merge(df_dcm_tw, how='left', on=['Date', 'Placement ID'])
            df_tw_joined.fillna(0, inplace=True)

    # get the number of ad group and placement id to de-dupe
            # adgroup_cnt = df_tw_joined[['Date', 'Ad Group ID', 'Placement ID']].drop_duplicates().groupby(['Date', 'Placement ID'])['Ad Group ID'].count().to_frame().reset_index()
            # adgroup_cnt.rename(columns={'Ad Group ID':'count'}, inplace=True)
            adgroup_cnt = lookup_tw_conversions(df_tw_joined, floodlight)

            df_tw_joined = df_tw_joined.merge(adgroup_cnt, how='left', on=['Date', 'Placement ID'])

            # divide by the count of ad group per placement
            uniformed_metrics = np.array(df_tw_joined[floodlight]) / np.array(df_tw_joined['count'])[:, np.newaxis]
            df_tw_joined[floodlight] = uniformed_metrics
            df_tw_joined = df_tw_joined.iloc[:, :-1]

            # filter data where total floodlights > 0
            df_tw_joined = df_tw_joined[df_tw_joined[floodlight].sum(axis=1) > 0]

    # align to preprocessed twitter data
            id_vars = [c for c in df_tw_joined.columns if not(c in floodlight)]
            df_tw_joined = df_tw_joined.melt(id_vars=id_vars)

            df_tw_joined.rename(columns={'Date':'Time period'}, inplace=True)
            df_tw_joined['platform'] = 'tw'
            df_tw_joined.drop(columns='Placement ID', inplace=True)

    # append to preprocessed data
            df_pre_fnl = pd.concat([df_pre, df_tw_joined], sort=False)
            df_pre_fnl.reset_index(drop=True, inplace=True)

            return df_pre_fnl

def preprocess_social(dfs_list, platform, df_metrics):
    '''
    Conditionally process raw files from the platforms.
        Parameters:
            dfs_list (str, path): list of paths to the raw platform file in the local data folder
            platform (str): abbreviated platform name: fb, li, pi, tw
            df_metrics (dataframe): imported metric mapping file from SharePoint 02Mapping
        Returns:
            df (dataframe): dataframe of the processed platform data
    '''

    # set up platform parameters
    date_field = social_param_fields[platform]['date']
    dims =  social_param_fields[platform]['core_dims']
    metrics = list(df_metrics[df_metrics['platform']==platform]['platform_metric'].unique())

    processed_list = []
    for df in dfs_list:
        # convert date field to a datetime object
        df[date_field] = pd.to_datetime(df[date_field])

        # filter the raw data columns
        df['platform'] = platform # platform is not part of the raw data
        df = df[dims + metrics]

        # transpose metrics to variable and value
        df = df.melt(id_vars=dims)

        # adjust values
        df['value'] = df['value'].apply(lambda x: re.sub("[^\d\.]", "", str(x)))
        df['value'] = pd.to_numeric(df['value'])

        # append to list
        processed_list.append(df)

    # combine list
    df = pd.concat(processed_list)

    # add twitter conversion function to append floodlight names to df
    df = add_twitter_conversion(platform, df)
    df[date_field] = pd.to_datetime(df[date_field])

    return df

def get_db_params(platform, df, df_metrics):
    '''Identify the paramters required for the database query.'''

    campaign_field = social_param_fields[platform]['campaign']
    ad_field = social_param_fields[platform]['ad']
    date_field = social_param_fields[platform]['date']
    metrics = list(df_metrics[df_metrics['platform']==platform]['redshift_universal_metric'].unique())

    # if twitter, add conversions
    if platform=='tw':
        pth = glob.glob(os.path.join(ASSETS_PATH, "*Social_Metrics*"))[0]
        df_metrics = pd.read_excel(pth, sheet_name='tw_conversions')
        floodlight = list(df_metrics['floodlight_name'].unique()) # filter dcm data by these floodlights
        metrics = metrics + floodlight

    # adjust adset for pinterest
    db_adset = "','".join(list(df[ad_field].map(str).unique()))
    if platform=='pi':
        db_adset = db_adset.replace('AG', '')

    db_campaign = "','".join(list(df[campaign_field].map(str).unique()))
    db_start_date = datetime.strftime(df[date_field].min(), '%Y-%m-%d')
    db_end_date = datetime.strftime(df[date_field].max(), '%Y-%m-%d')
    db_metrics = "','".join(metrics)

    return {'campaign':db_campaign, 'adset':db_adset, 'start':db_start_date, 'end':db_end_date, 'metrics':db_metrics}

def process_social(platform, df):
    # get parameters
    rename_cols = social_param_fields[platform]['rename_cols']
    date_field = social_param_fields[platform]['date']

     # adjust columns
    df['week'] = df[date_field].apply(lambda x: x - timedelta(days=x.weekday()))
    df.rename(columns=rename_cols, inplace=True)

    # group data
    group_fields = ['platform', 'week'] + list(rename_cols.values()) + ['variable']
    df_grp = df.groupby(group_fields)['value'].sum().to_frame().reset_index()

    return df_grp


def create_qa_file(media, date, qa_results):
    '''
    Create raw QA file by media.
        Parameters:
            media (str): media type: search, social, etc.
            date (str): string datetime obj for qa filename
            qa_results (dict): dicionary object containing the qa metric results by media
        Returns:
            qa_filename (str): filename for formatting function.
    '''

    qa_filename = f"QA_{media.title()}_{date}.xlsx"
    writer = pd.ExcelWriter(os.path.join(OUTPUTS_PATH, qa_filename), engine='xlsxwriter')

    if media in ['search', 'digital']:
        for k in qa_results.keys():
            qa_results[k].to_excel(writer, sheet_name=f"{k}", index=False)
        writer.save()

    elif media == 'social':
        # details
        all_qry = [f"{qa_results[p]['qry']}\n\n" for p in qa_results.keys()]
        all_qry = ''.join(all_qry)

        start_dates = []
        end_dates = []

        for p in qa_results.keys():
            date_field = social_param_fields[p]['date']
            start_dates.append(qa_results[p]['pre_data'][date_field].min())
            end_dates.append(qa_results[p]['pre_data'][date_field].max())

        db_params_dict = {'last_updated': datetime.now(), 'start_date':min(start_dates),  'end_date':max(end_dates), 'sql_qry': all_qry}
        details = pd.DataFrame.from_dict(db_params_dict, orient='index').reset_index()
        details.columns = ['variable', 'value']
        details.to_excel(writer, 'detials', index=False)

        for p in qa_results.keys():
            for k in qa_results[p]['qa_results'].keys():
                qa_results[p]['qa_results'][k].to_excel(writer, sheet_name=f"{p}_{k}", index=False)
        writer.save()

    return qa_filename


def create_social_qa(platform, df_ui, df_rs, df_metrics):
    '''
    Combine platform and Redshift data to calculate diff across metrics.
        Parameters:
            platform (str): abbreviated platform name: 'fb', 'li', 'pi', 'tw'
            df_ui (df): platform dataframe
            df_rs (df): redshift dataframe
            df_metrics (df): platform metrics and redshift equivalent
        Returns:
            pvts_dict (dict): dictionary of metric differences
    '''

    pvts_dict = dict()

    # rename redshift columns to align with platform data
    rs_rename_cols = {
        'universal_campaign':'campaign_name',
        'universal_adset': 'adset',
        'universal_campaign_id': 'campaign_id',
        'universal_adset_id': 'adset_id',
        'platform_metric': 'variable'
                    }
    # add platform to redshift dataframe
    df_rs['platform'] = platform

    # rename columns for mapping
    df_metrics_fltr = df_metrics[df_metrics['platform']==platform][['platform_metric', 'redshift_universal_metric']].drop_duplicates()
    df_metrics_fltr.rename(columns={'redshift_universal_metric':'universal_metric'}, inplace=True)

    # add the twitter conversions to the filter
    if platform =='tw':
        pth = glob.glob(os.path.join(ASSETS_PATH, "*Social_Metrics*"))[0]
        df_metrics = pd.read_excel(pth, sheet_name='tw_conversions')
        floodlight = list(df_metrics['floodlight_name'].unique()) # filter dcm data by these floodlights
        universal_campaign = list(df_metrics['universal_campaign'].unique())

        # create a dataframe to stack the twitter conversions
        df_tw_floodlights = pd.concat([pd.DataFrame(floodlight), pd.DataFrame(floodlight)], axis=1)
        df_tw_floodlights.columns = ['platform_metric', 'universal_metric']

        df_metrics_fltr = pd.concat([df_metrics_fltr,df_tw_floodlights])

    # map variables to platform name
    df_rs = df_rs.merge(df_metrics_fltr, how='left')
    df_rs.rename(columns=rs_rename_cols, inplace=True)

    # group data
    group_fields = list(df_ui.columns[:-1])
    group_fields = [f for f in group_fields if not('_id' in f.lower())] if platform == 'li' else group_fields # remove id from li qa
    df_rs = df_rs.groupby(group_fields)['value'].sum().to_frame().reset_index()
    df_ui = df_ui.groupby(group_fields)['value'].sum().to_frame().reset_index()

    # add source
    df_rs['source'] = 'redshift'
    df_ui['source'] = 'ui'

    # combine rs and ui for qa
    df_qa = pd.concat([df_rs, df_ui])
    if platform == 'tw':
        # split
        df_qa_split = df_qa.copy()
        df_qa_split_0 = df_qa[-df_qa['variable'].isin(floodlight)] # core metrics
        df_qa_split_1 = df_qa[df_qa['variable'].isin(floodlight)] # floodlights
        df_qa_split_2 = df_qa_split_1[df_qa_split_1['campaign_name'].isin(universal_campaign)] # floodlight and campaing
        df_qa = pd.concat([df_qa_split_0, df_qa_split_2], sort=False).reset_index(drop=True)

    # print('exporting combined rs and ui data')
    # df_qa.to_csv('df_qa.csv', index=False)
    # df_qa['value'] = np.round(df_qa['value'])

    # set up views
    view_args = {
        'campaign': {
            'index':['campaign_name'],
            'dim_cutoff': 1
                    },
        'adset': {
            'index':['campaign_name', 'adset'],
            'dim_cutoff': 2
                 },
        'week': {
            'index':['campaign_name', 'adset', 'week'],
            'dim_cutoff': 3
                 }
                }

    for k in view_args.keys():
        df_pvt = df_qa.pivot_table(index=view_args[k]['index'], columns=['variable', 'source'], values='value', aggfunc='sum').reset_index()

        list_of_metric_diff = []
        metrics = df_pvt.columns.levels[0][:list(df_pvt.columns.levels[0]).index(view_args[k]['index'][-1])]

        # move core metrics to the beginning
        core_metrics = social_param_fields[platform]['core_metrics']
        metrics = core_metrics + [m for m in metrics if not(m in core_metrics)]

        for m in metrics:
            df_temp = df_pvt.copy()
            df_temp = df_temp[m]
            df_temp.columns = [f"{m}_{c}" for c in df_temp.columns]

            # fill na based on condition
            if len(df_temp.columns) < 2:
                continue
            col0 = df_temp.columns[0]
            col1 = df_temp.columns[1]
            df_temp[col0] = df_temp.apply(lambda x: 0 if np.isnan(x[col0]) and not(np.isnan(x[col1])) else x[col0], axis=1)
            df_temp[col1] = df_temp.apply(lambda x: 0 if np.isnan(x[col1]) and not(np.isnan(x[col0])) else x[col1], axis=1)

            df_temp[f"{m}_%_diff"] = (df_temp.iloc[:,0]/df_temp.iloc[:,1])-1
            df_temp[f"{m}_%_diff"] = df_temp[f"{m}_%_diff"].apply(lambda x: 1 if x == float('inf') else x)

            list_of_metric_diff.append(df_temp)

        metric_diffs = pd.concat(list_of_metric_diff, axis=1)
        df_pvt_qa = pd.concat([df_pvt.iloc[:, :view_args[k]['dim_cutoff']], metric_diffs], axis=1)
        df_pvt_qa.columns = [c[0] if type(c) is tuple else c for c in df_pvt_qa.columns]

        pvts_dict[k] = df_pvt_qa

    print(f"    {platform} QA completed\n")
    return pvts_dict


def parse_ss_filename(x):
    '''
    Parse Site Served template filename to timestamp and filename
    '''
    # find the last occurence of '.' and last occurence of '_'
    x_cut = x[:x.rfind('.')]
    if 'v' in x_cut[-2:].lower():
        filename_raw = x_cut[x_cut.find('_')+1:-3]
    else:
        filename_raw = x_cut[x_cut.find('_')+1:]

    # find the 1st '_'
    timestamp = float(x_cut[:x_cut.find('_')])

    return filename_raw, timestamp

def latest_ss_filename(filename_parse_list):
    df_filename = pd.DataFrame(filename_parse_list, columns=['filename', 'filename_raw', 'timestamp'])
    df_filename['max_timestamp'] = df_filename.groupby(['filename_raw'])['timestamp'].transform(max)
    df_filename['bool'] = df_filename['timestamp'] == df_filename['max_timestamp']
    filenames = list(df_filename[df_filename['bool']==True]['filename'])

    return filenames

# def process_ss_templates(ss_files):
#     dfs_dict = {os.path.split(f)[-1]: pd.read_excel(f) for f in ss_files}
#
#     filename_parse_list = []
#     dfs_list = []
#     for k in dfs_dict.keys():
#         df = dfs_dict[k]
#         df['filename'] = k
#         filename_parse_results = parse_ss_filename(k)
#         filename_parse_list.append([k, filename_parse_results[0], filename_parse_results[1]])
#         cols = list(df.columns)
#         if 'date' in [c.lower() for c in cols]:
#             df.rename(columns={c: c.strip().lower() for c in df.columns}, inplace=True) # remove trailing space and make lower
#             dfs_list.append(df)
#
#     # get list of the latest filenames
#     latest_ss = latest_ss_filename(filename_parse_list)
#
#     # combine and filter to the latest filename
#     df = pd.concat(dfs_list, sort=True)
#     df = df[df['filename'].isin(latest_ss)].reset_index(drop=True)
#
#     # remove quartule and unnamed metrics
#     col_filter = [c for c in df.columns if not('quartile' in c) and not('unnamed' in c)]
#     df = df[col_filter]
#     df['date'] = pd.to_datetime(df['date'])
#
#     # split col by metrics, override, and dimensions
#     metrics = ['clicks', 'spend', 'impressions', 'video plays', 'video completions']
#     override = [c for c in df.columns if 'override' in c]
#     dims = [c for c in df.columns if not(c in metrics) and not(c in override)]
#     df = df[dims + metrics + override]
#
#     # pair up the metric and override
#     metric_override_pair = []
#     for m in metrics:
#         for o in override:
#             if m in o:
#                 metric_override_pair.append([m, o])
#
#
#     df_temp_list = []
#     for p in metric_override_pair:
#         df_temp = df.copy()
#         df_temp = df_temp[dims + p]
#         df_temp['metric'] = p[0]
#         df_temp.rename(columns={p[0]:'value', p[1]:'override'}, inplace=True)
#         df_temp_list.append(df_temp)
#
#     df_trans = pd.concat(df_temp_list, sort=False)
#     df_trans['value'] = df_trans['value'].fillna(0)
#     df_trans['value'] = df_trans['value'].map(str).replace('-', None)
#     df_trans['value'] = pd.to_numeric(df_trans['value'])
#
#     # replace override and keep True
#     df_trans['override'] = df_trans['override'].apply(lambda x: True if 'x' in str(x).lower() else False)
#     df_trans = df_trans[df_trans['override']==True]
#     df_trans.drop(columns=['filename', 'override'], inplace=True)
#     df_trans.reset_index(drop=True, inplace=True)
#
#     return df_trans

def ancillary_data(app, media):
    '''
    Download additional data required for specified media.
        Parameters:
            app (obj): sharepoint object
            media (str): media to be processed
        Returns:
            Downloaded additioanl data files
    '''
    dict_ = {'digital':'Site Served', 'social':'DCM'}

    if media in ['digital', 'social']:
        print(f"    Downloading {dict_[media]} data to: {DATA_PATH}")
        app.list_contents(media_args[media]['sharepoint']['ancillary_data'])
        app.download_files(DATA_PATH)

    return None

def archive_data_path(folder_paths):
    '''
    Get data from the maximum archive folder date.
        Parameters:
            folder_paths: app generated list of folders in specified path
        Returns:
            Latest archive path with data
    '''
    dict_ = dict()
    for folder in folder_paths:
        archive_date = os.path.split(folder)[-1]
        try:
            dict_[int(archive_date)] = folder
        except ValueError:
            continue

    max_archive_date = max(dict_, key=int)
    archive_data_path = dict_[max_archive_date].replace(' ', '%20')

    return archive_data_path

def process_dcm_ui(campaign_ids_list, core_metrics, activity_list):
    '''
    Process and format raw data from DCM.
        Parameters:
            campaign_ids_list (list): list of camapign ids from Digital_mapping
            core_metrics (list): list of metrics like impressions, clicks, and spend
            activity_list (list): list of activity ids from Digital_mapping
        Returns:
            Processed dataframe of DCM UI data

    '''
    # import digital data
    list_of_dfs = []

    unzip()
    files = glob.glob(os.path.join(DATA_PATH, '*.csv'))
    for f in files:
        df = pd.read_csv(f, skiprows=10).iloc[:-1, :]
        df = df[df['Campaign ID'].map(str).isin(campaign_ids_list)] # filter campaigns
        df['Date'] = pd.to_datetime(df['Date'])

    # core metrics
        df_core = df[df['Activity ID']=='(not set)'].iloc[:, :-2]
        id_vars = [c for c in df_core.columns if not(c in core_metrics)]
        df_core = df_core.melt(id_vars=id_vars)
        df_core['metric'] = df_core['variable']

    # activity
        df_act = df[df['Activity ID'].isin(activity_list)]
        df_act.drop(columns=core_metrics, inplace=True) # remove core metrics

        # create floodlight metrics
        id_vars = df_act.columns[:-2]
        df_act = df_act.melt(id_vars=id_vars)
        if len(activity_list) > 0:
            df_act['metric'] = df_act.apply(lambda x: x['Activity Group'] + ' : ' + x['Activity'] + ': ' + x['variable'], axis=1)
        else:
            df_act['metric'] = None # dummy

    # combine core and activity
        df_combined = pd.concat([df_core, df_act], sort=False)

        # drop activity
        drop_activity = [c for c in df_combined.columns if 'activity' in c.lower()]
        df_combined = df_combined.drop(columns=drop_activity)

        list_of_dfs.append(df_combined)

    # combine digital data
    df_ui = pd.concat(list_of_dfs)

    return df_ui

def process_ss_templates(campaign_name_list):
    ss_metric_rename = {
    'clicks': 'Clicks',
    'spend': 'Media Cost',
    'impressions': 'Impressions',
    'video plays': 'Video Plays',
    'video completions': 'Video Completions'
    }

    files = glob.glob(os.path.join(DATA_PATH, '*xlsx'))

    if len(files)==0:
        return None

    dfs_dict = {os.path.split(f)[-1]: pd.read_excel(f) for f in files}

    filename_parse_list = []
    dfs_list = []
    for k in dfs_dict.keys():
        df = dfs_dict[k]
        df['filename'] = k
        filename_parse_results = parse_ss_filename(k)
        filename_parse_list.append([k, filename_parse_results[0], filename_parse_results[1]])
        cols = list(df.columns)
        if 'date' in [c.lower() for c in cols]:
            df.rename(columns={c: c.strip().lower() for c in df.columns}, inplace=True) # remove trailing space and make lower
            dfs_list.append(df)

    # get list of the latest filenames
    latest_ss = latest_ss_filename(filename_parse_list)

    # combine and filter to the latest filename
    df = pd.concat(dfs_list, sort=True)
    df = df[df['filename'].isin(latest_ss)].reset_index(drop=True)

    # remove quartule and unnamed metrics
    col_filter = [c for c in df.columns if not('quartile' in c) and not('unnamed' in c)]
    df = df[col_filter]
    df['date'] = pd.to_datetime(df['date'])

    # split col by metrics, override, and dimensions
    metrics = ['clicks', 'spend', 'impressions', 'video plays', 'video completions']
    override = [c for c in df.columns if 'override' in c]
    dims = [c for c in df.columns if not(c in metrics) and not(c in override)]
    df = df[dims + metrics + override]

    # pair up the metric and override
    metric_override_pair = []
    for m in metrics:
        for o in override:
            if m in o:
                metric_override_pair.append([m, o])

    df_temp_list = []
    for p in metric_override_pair:
        df_temp = df.copy()
        df_temp = df_temp[dims + p]
        df_temp['metric'] = p[0]
        df_temp.rename(columns={p[0]:'value', p[1]:'override'}, inplace=True)
        df_temp_list.append(df_temp)

    df_trans = pd.concat(df_temp_list, sort=False)
    df_trans['value'] = df_trans['value'].fillna(0)
    df_trans['value'] = df_trans['value'].map(str).replace('-', None)
    df_trans['value'] = pd.to_numeric(df_trans['value'])

    # replace override and keep True
    df_trans['override'] = df_trans['override'].apply(lambda x: True if 'x' in str(x).lower() else False)
    df_trans = df_trans[df_trans['override']==True]
    df_trans.drop(columns=['filename', 'override'], inplace=True)
    df_trans.reset_index(drop=True, inplace=True)

    # filter ss campaigns
    df_ss_template = df_trans[df_trans['campaign name'].isin(campaign_name_list)]

    # align columns to redshift
    df_ss_template['week'] = df_ss_template['date'].apply(lambda x: x - timedelta(days=x.weekday()))
    df_ss_template.rename(columns={'publisher (site)':'site_name'}, inplace=True)
    df_ss_template.rename(columns={c: c.replace(' ', '_') for c in df_ss_template.columns}, inplace=True)

    df_ss_template['metric'] = df_ss_template['metric'].apply(lambda x: ss_metric_rename[x])

    return df_ss_template

def combine_ui_db(df_qry_dcm, df_qry_ss, df_ui, df_ss_template):
    ''''''
# site served
# ========================================================================================================================
    # add source
    if df_ss_template is None:
        df_qa_ss = df_ss_template # make empty
    else:
        df_ss_template['source'] = 'ui'
        df_qry_ss['source'] = 'redshift'
        df_qa_ss = pd.concat([df_qry_ss, df_ss_template], sort=True)
        df_qa_ss.drop(columns=['date', 'campaign_id', 'site_id'], inplace=True)
        df_qa_ss['site_served'] = True

# dcm
# ========================================================================================================================
    # filter cols in UI data
    df_ui_fltr = df_ui[ui_col_rename.keys()]
    df_ui_fltr.rename(columns=ui_col_rename, inplace=True)

    # combine
    df_qry_dcm['source'] = 'redshift'
    df_ui_fltr['source'] = 'ui'

    df_qa = pd.concat([df_qry_dcm, df_ui_fltr], sort=False)

    # normalize data
    id_cols = [c for c in df_qa.columns if '_id' in c]
    for c in id_cols:
        df_qa[c] = df_qa[c].map(int)

    df_qa['week'] = pd.to_datetime(df_qa['week'])
    df_qa['week'] = df_qa['week'].apply(lambda x: x - timedelta(days=x.weekday()))

    # remove trailing spaces
    df_qa['campaign_name'] = df_qa['campaign_name'].str.rstrip()
    df_qa['placement_name'] = df_qa['placement_name'].str.rstrip()
    df_qa['site_name'] = df_qa['site_name'].str.rstrip()
    df_qa['metric'] = df_qa['metric'].str.rstrip()

    df_qa['site_served'] = False

# combine
# ========================================================================================================================
    df_qa_combined = pd.concat([df_qa, df_qa_ss])

    return df_qa_combined

def create_digital_qa(df_qa, db_start_date, db_end_date, qry_txt):
    '''
    '''
# initiate dictionary
# ========================================================================================================================
    pvts_dict = dict()

    db_params_dict = {'last_updated':datetime.now(), 'start_date': db_start_date, 'end_date': db_end_date, 'sql_qry': qry_txt}
    details = pd.DataFrame.from_dict(db_params_dict, orient='index').reset_index()
    details.columns = ['variable', 'value']

    pvts_dict['details'] = details
    pvts_dict['raw_data'] = df_qa

# qa parameters
# ========================================================================================================================
    view_args = {
        'campaign': {'index':['site_served', 'campaign_name', 'site_name'],
                       'dim_cutoff': 3},

        'placement': {'index':['site_served', 'campaign_name', 'placement_id', 'site_name'],
                     'dim_cutoff': 4},

        'week': {'index':['site_served', 'campaign_name', 'week', 'site_name'],
                 'dim_cutoff': 4}
    }

    digital_param_fieds = {
        'dcm': {'core_metrics': ['Clicks', 'Media Cost', 'Impressions', 'Video Plays', 'Video Views', 'Video Completions']}}

# start qa
# ========================================================================================================================
    for k in view_args.keys():
        df_pvt =  df_qa.pivot_table(index=view_args[k]['index'], columns=['metric', 'source'], values='value', aggfunc='sum').reset_index()

        list_of_metric_diff = []
        metrics = df_pvt.columns.levels[0][:list(df_pvt.columns.levels[0]).index(view_args[k]['index'][-1])]

        # move core metrics to the beginning
        core_metrics = digital_param_fieds['dcm']['core_metrics']
        metrics = core_metrics + [m for m in metrics if not(m in core_metrics)]

        for m in metrics:
            df_temp = df_pvt.copy()
            df_temp = df_temp[m]
            df_temp.columns = [f"{m}_{c}" for c in df_temp.columns]

            # fill na based on condition
            if len(df_temp.columns) < 2:
                continue
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

    return pvts_dict

def lookup_tw_conversions(df, floodlight):

    db_campaign = "','".join(list(df['Campaign name'].map(str).unique()))
    db_start_date = datetime.strftime(df['Date'].min(), '%Y-%m-%d')
    db_end_date = datetime.strftime(df['Date'].max(), '%Y-%m-%d')
    db_metrics = "','".join(floodlight)

    db_params = {'campaign':db_campaign, 'start':db_start_date, 'end':db_end_date, 'metrics':db_metrics}

    qry_txt = get_qry_text('social_twitter_qry')
    qry_txt = qry_txt.replace('load_metrics', db_metrics) \
                     .replace('load_campaign_name', db_campaign) \
                     .replace('load_start_date', db_start_date) \
                     .replace('load_end_date', db_end_date)

    qa_results = run_qry(qry_txt)
    qa_results['placement_id'] = qa_results['universal_adset'].apply(lambda x: x.split('_')[-1])
    adgroup_cnt = qa_results[['full_date', 'universal_adset_id', 'placement_id']].drop_duplicates().groupby(['full_date', 'placement_id'])['universal_adset_id'].count().to_frame().reset_index()
    adgroup_cnt.rename(columns={'universal_adset_id':'count', 'full_date':'Date', 'placement_id':'Placement ID'}, inplace=True)
    adgroup_cnt['Date'] = pd.to_datetime(adgroup_cnt['Date'])

    return adgroup_cnt


def choose_medias():
    medias_base = ['search', 'social', 'digital']
    if len(sys.argv)==1:
        medias_base
        return medias_base
    else:
        medias_input = [s.replace(',','').lower().strip() for s in sys.argv[1:]]
        invalid_media = [m for m in medias_input if not(m in medias_base)]
        if len(invalid_media) > 0:
            sys.exit(f"\n{'='*50}\ninvalid media entered\n{invalid_media}\nexiting script...\n{'='*50}")
        return medias_input

def update_log(media, qa_filename, runtime):
    # import current log
    df_log = pd.read_csv(os.path.join(OUTPUTS_PATH, 'log.csv'))

    df_dict = {
        'date':[datetime.now()],
        'media':[media],
        'qa_filename': [qa_filename],
        'runtime':[runtime]
    }

    new_log = pd.DataFrame.from_dict(df_dict)
    df_log = pd.concat([df_log, new_log])

    df_log.to_csv(os.path.join(OUTPUTS_PATH, 'log.csv'), index=False)
