import os

# set directory
ROOT = os.getcwd()
dir_ = '01_database'

while os.path.split(ROOT)[-1]!=dir_:
    ROOT = os.path.split(ROOT)[0]
paths = {f'{f.upper()}_PATH':os.path.join(ROOT, f) for f in os.listdir(ROOT) if len(f.split('.'))==1}

# datapath = '../02_data'
# outputpath = '../03_outputs'
# ASSETS_PATH = '../assets'

connection_string = 'Driver={SQL Server};''Server=P1-J3-SQL-01.EXT.IPGNETWORK.COM;''Database=Coke_WIRE_Stage;''Trusted_Connection=yes;'

odd_chars = {'ช่อง one (YouTube Channel)':'???? one (YouTube Channel)'}

foldername = {'path':'00_pathmatics', 'kant':'01_kantar'}

um_media_type = {
    'Int Search':'Paid Search',
    'Local Radio':'Radio',
    'Outdoor':'OOH',
    'Newspaper':'Print',
    'TV':'TV',
    'Magazine':'Print',
    'Mobile Display':'Digital Display',
    'Mobile Video':'Digital Video',
    'Desktop Display':'Digital Display',
    'Desktop Video':'Digital Video',
    'Twitter':'Social',
    'Facebook':'Social',
    'Instagram': 'Social'}

remove_advertisers = ['Square Inc']

meta = {'00_pathmatics':
            {'short':'path',
             'ext':'*.csv',
             'freq':['D', 'Date'],
             'metrics':['Spend','Impressions'],
             'advertiser_rename': {
                'Visa':'Visa Usa Inc',
                'American Express Company':'American Express Co',
                'Citigroup, Inc.':'Citigroup Inc',
                'Bank of America':'Bank Of America Corp',
                'PayPal':'PayPal Holdings Inc',
                'MasterCard':'Mastercard Intl Inc',
                'JPMorgan Chase & Co.':'JP Morgan Chase & Co',
                'Square, Inc.':'Square Inc',
                'Discover Financial Services': 'Discover Financial Services',
                'Capital One Financial Corporation':'Capital One Financial Corp'}
            },

        '01_kantar':
            {'short':'kant',
             'ext':'*.xlsx',
             'freq':['MS', 'TIME_PERIOD'],
             'metrics':['UNITS', 'DOLS_000', 'DOLS'],
             'advertiser_rename': {
                'American Express Co':'American Express Co' ,
                'Bank Of America Corp':'Bank Of America Corp',
                'Capital One Financial Corp':'Capital One Financial Corp',
                'Citigroup Inc':'Citigroup Inc',
                'Discover Financial Services':'Discover Financial Services',
                'JP Morgan Chase & Co':'JP Morgan Chase & Co',
                'Mastercard Intl Inc':'Mastercard Intl Inc',
                'Visa Usa Inc':'Visa Usa Inc',
                'PayPal Holdings Inc':'PayPal Holdings Inc',
                'MasterCard Intl Inc':'Mastercard Intl Inc',
                'Square Inc':'Square Inc',
                'delete':'delete',
                'Delete':'delete'}
            }
       }

defined_dims = {'path': {'adv': ['Advertiser'],
  'site': ['Site'],
  'device': ['Device'],
  'dirind': ['Direct_Indirect'],
  'br_rt': ['Brand_Root'],
  'br_mjr': ['Brand_Major'],
  'br_mnr': ['Brand_Minor'],
  'br_lf': ['Brand_Leaf'],
  'cat': ['Category'],
  'region': ['Region'],
  'crea': ['Creative_ID',
   # 'Width',
   # 'Height',
   # 'Type',
   # 'Creative_Text',
   # 'Landing_Page',
   'Link_to_Creative']},
 'kant': {'cat': ['CATEGORY'],
  'micro_cat': ['MICROCATEGORY'],
  'parent': ['PARENT'],
  'adv': ['ADVERTISER'],
  'br': ['BRAND'],
  'prod': ['PRODUCT'],
  'amex_cat': ['AMEX_CATEGORY'],
  'amex_card_type': ['AMEX_CARD_TYPE'],
  'amex_cobr': ['AMEX_COBRAND'],
  'amex_parent': ['AMEX_PARENT'],
  'amex_prod_type': ['AMEX_PRODUCT_TYPE'],
  'amex_seg': ['AMEX_SEGMENT'],
  'media': ['MEDIA'],
  'media_type': ['MEDIA_TYPE']}}
