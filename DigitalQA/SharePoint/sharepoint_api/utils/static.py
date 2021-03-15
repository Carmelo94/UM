import os

# redshift credentials
rs_cred = {
    "dbname":"",
    "host":"",
    "port":,
    "user":"",
    "password":""
}

# expected columns from SA360 UI reports
sa360_col_check = [
    'Row Type',
    'Status',
    'Sync errors',
    'From',
    'To',
    'Engine',
    'Account',
    'Campaign',
    'Engine status',
    'Daily budget',
    'Engine bid strategy name',
    'Clicks',
    'Impr',
    'Cost',
    'CTR',
    'Avg CPC',
    'Avg pos']

# social parameters for database
social_param_fields = {
    'fb':{
        'campaign': 'Campaign name',
        'ad': 'Ad set ID',
        'date': 'Day',
        'core_dims': ['platform', 'Day', 'Campaign name', 'Ad set name', 'Campaign ID', 'Ad set ID'],
        'core_metrics': ['Link clicks', 'Amount spent (USD)', 'Impressions'],
        'metrics': 'variable',
        'rename_cols': {
            'Campaign name': 'campaign_name',
            'Ad set name': 'adset',
            'Campaign ID': 'campaign_id',
            'Ad set ID': 'adset_id'
                        }
        },
    'li':{
        'campaign': 'Account Name',
        'ad': 'Campaign Name',
        'date': 'Start Date (in UTC)',
        'core_dims': ['platform', 'Start Date (in UTC)', 'Account Name', 'Campaign Name', 'Campaign ID'],
        'core_metrics': ['Clicks', 'Total Spent', 'Impressions'],
        'metrics': 'variable',
        'rename_cols': {
            'Account Name': 'campaign_name',
            'Campaign Name': 'adset',
            # '': 'campaign_id',
            'Campaign ID': 'adset_id'            }
        },
    'pi':{
        'campaign': 'Campaign name',
        'ad': 'Ad group',
        'date': 'Date range start',
        'core_dims': ['platform', 'Date range start', 'Campaign name', 'Ad group name', 'Campaign ID', 'Ad group'],
        'core_metrics': ['Pin clicks', 'Spend in account currency', 'Impressions'],
        'metrics': 'variable',
        'rename_cols': {
            'Campaign name': 'campaign_name',
            'Ad group name': 'adset',
            'Campaign ID': 'campaign_id',
            'Ad group': 'adset_id'
                        }
        },
    'tw':{
        'campaign': 'Campaign name',
        'ad': 'Ad Group ID',
        'date': 'Time period',
        'core_dims': ['platform', 'Time period', 'Campaign name', 'Ad Group name', 'Campaign ID', 'Ad Group ID'],
        'core_metrics': ['Link clicks', 'Spend', 'Impressions'],
        'metrics': 'variable',
        'rename_cols': {
            'Campaign name': 'campaign_name',
            'Ad Group name': 'adset',
            'Campaign ID': 'campaign_id',
            'Ad Group ID': 'adset_id'
                        }
        }
}

# site servde and dcm column renaming
ss_metric_rename = {
    'clicks': 'Clicks',
    'spend': 'Media Cost',
    'impressions': 'Impressions',
    'video plays': 'Video Plays',
    'video completions': 'Video Completions'
}

# align ui column names
ui_col_rename = {
    'Date': 'week',
    'Campaign': 'campaign_name',
    'Campaign ID': 'campaign_id',
    'Placement': 'placement_name',
    'Placement ID': 'placement_id',
    'Site (DCM)': 'site_name',
    'Site ID (DCM)': 'site_id',
#     'variable': 'sa360_col_name',
    'metric': 'metric',
    'value': 'value'
}


# media arguments
# media arguments
media_args = {
    'search':
        {
            'sharepoint':{
                'data': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Search/01Sandbox/SA360_Files/Gmail/National',
                'mapping': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Search/02Mapping',
                'qa': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Search/03QAFiles',
                'flat': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Search/04FlatFiles'
                        },
            'redshift':{
                'all_metrics': '''SELECT DISTINCT metric FROM amex.v_sa360_campaign_joined WHERE campaign_name LIKE 'US%' ''',
                'metric_filename': 'SA360_LiveCampaigns',
                'metric_sheetname': 'sa360_metrics',
                'qa_qry': 'sa360_qry'
                        },
            'flatfile':{
                'group_columns': ['From', 'Campaign'],
                'final_filename': 'Search_FlatFile'
                        }
        },
    'social':
        {
            'sharepoint':{
                'data':'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Social/01Sandbox/National',
                'ancillary_data':'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Digital/01Sandbox/DCMPull/DCMReport',
                'mapping': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Social/02Mapping',
                'qa': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Social/03QAFiles',
                'flat': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Social/04FlatFiles'
                        },
            'redshift':{
                'all_metrics': '''SELECT DISTINCT universal_platform, metric, universal_metric FROM amex.v_social_joined_updated''',
                'metric_filename': 'Social_Metrics',
                'metric_sheetname': 'redshift_metrics',
                'qa_qry': 'social_qry'
                        },
            'flatfile':{
                'group_columns': {
                    'fb':['Day', 'campaign_id', 'adset_id'],
                    'li': None,
                    'pi': ['Date range start', 'campaign_id', 'adset_id'],
                    'tw':['Time period', 'campaign_id', 'adset_id']
                },
                'final_filename': None
                        }
        },
    'digital':
        {
            'sharepoint':{
                'data':'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Digital/01Sandbox/DCMPull/DCMReport',
                'ancillary_data':'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Digital/01Sandbox/SiteServed',
                'mapping': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Digital/02Mapping',
                'qa': 'Measurement%20%20Analytics%20Folder/GABM/Global%20Analytics/DigitalQA/Digital/03QAFiles',
                'flat': None
                        },
            'redshift':{
                'all_metrics': '''SELECT DISTINCT metric FROM amex.v_digital_joined WHERE campaign_name LIKE 'US%' ''',
                'metric_filename': 'Digital_mapping',
                'metric_sheetname': 'redshift_metrics',
                'qa_qry': 'digital_qry'
                        },
            # 'flatfile':{
            #     'group_columns': ['From', 'Campaign'],
            #     'final_filename': 'DCM_FlatFile'
            #             }
        }
}


# project path
folder = 'sharepoint_api'

cur_dir = os.getcwd()
idx = cur_dir.lower().find(folder)
ROOT = os.path.join(cur_dir[:idx], folder)

branches = ['assets', 'data', 'outputs', 'utils']
ASSETS_PATH, DATA_PATH, OUTPUTS_PATH, UTILS_PATH = [os.path.join(ROOT, branch) for branch in branches]
