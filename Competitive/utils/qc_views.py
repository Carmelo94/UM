from datetime import datetime
import time
import pandas as pd
import numpy as np
import os
import pyodbc
import glob
from static import *
from hlpr import *

ASSETS_PATH = paths['ASSETS_PATH']
OUTPUTS_PATH = paths['OUTPUTS_PATH']

def path_summ_unmapped():
	'''
	Returns dataframe of latest unmapped pathmatics data.
		Parameters:
			None
		Returns:
			df_summ_unmapped (dataframe): unmapped pathmatics data
	'''

	# time check start
	start = time.time()

	# connect to db
	conn = pyodbc.connect(connection_string)
	cursor = conn.cursor()

	# get the max timestamp for each year
	qry = '''
		SELECT DISTINCT
		    c.Advertiser,
		    b.Year,
		    b.Month,
		    d.Device,
		    MAX(a.timestamp) OVER (PARTITION BY a.adv_ID, b.Year, b.Month, d.Device) timestamp
		FROM amex.path_fact a
		LEFT JOIN amex.path_dim_date b ON a.date_ID = b.date_ID
		LEFT JOIN amex.path_dim_adv c ON a.adv_ID = c.adv_ID
		LEFT JOIN amex.path_dim_device d on a.device_ID = d.device_ID
		GROUP BY
		    a.timestamp,
		    a.adv_ID,
		    c.Advertiser,
		    a.device_ID,
		    d.Device,
		    b.Year,
		    b.Month;'''

	max_date = pd.read_sql_query(qry, conn)

	# call the fact table
	qry = '''
		SELECT DISTINCT
			a.timestamp,
			a.adv_ID,
			a.br_lf_ID,
			a.br_mjr_ID,
			a.br_mnr_ID,
			a.br_rt_ID,
			a.device_ID,
			b.Month,
			b.Year,
			-- a.site_ID,
			a.cat_ID,
			SUM(a.Spend) Spend
		FROM amex.path_fact a
		LEFT JOIN amex.path_dim_date b
		ON a.date_ID = b.date_ID
		GROUP BY
			a.timestamp,
			a.adv_ID,
			a.br_lf_ID,
			a.br_mjr_ID,
			a.br_mnr_ID,
			a.br_rt_ID,
			a.device_ID,
			-- a.site_ID,
			a.cat_ID,
			b.Month,
			b.Year;'''
	df_raw_grouped = pd.read_sql_query(qry, conn)

	# dims we want to call based on the ID in the path_raw_grouped table
	dimensions = [c.replace('_ID', '') for c in df_raw_grouped.columns if 'ID' in c]
	qrys = [f"SELECT * FROM amex.path_dim_{d};" for d in dimensions]
	tbls = [pd.read_sql_query(qry, conn) for qry in qrys]

	# join to tbls to map dimension ids
	for t in tbls:
		df_raw_grouped = df_raw_grouped.merge(t, how='left')

	# filter to relevant timestamp <> year pair
	df_summ_unmapped = df_raw_grouped.merge(max_date, how='right')

	# set column order
	col_order = ['Year','Month','Advertiser','Brand_Root','Brand_Major','Brand_Minor','Brand_Leaf','Device', 'Category','Spend']
	# col_order = ['Year','Month','Advertiser','Brand_Root','Brand_Major','Brand_Minor','Brand_Leaf','Device','Site', 'Spend']
	df_summ_unmapped = df_summ_unmapped[col_order]

	df_summ_unmapped['Month'] = pd.to_datetime(df_summ_unmapped['Month'])

	# time check end
	print(f"Pathmatics Runtime: {np.round(time.time() - start, 2)}")

	return df_summ_unmapped

def kant_summ_unmapped():
    ''''''
    # time check start
    start = time.time()

    # connect to db
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # get the max timestamp by media
    qry = '''
        SELECT DISTINCT
            c.AMEX_PARENT,
            d.MEDIA,
            b.Year,
			b.Month,
            MAX(a.timestamp) OVER (PARTITION BY a.amex_parent_ID, b.Year, b.Month, d.MEDIA ) timestamp
        FROM amex.kant_fact a
        LEFT JOIN amex.kant_dim_date b ON a.date_ID = b.date_ID
        LEFT JOIN amex.kant_dim_amex_parent c ON a.amex_parent_ID = c.amex_parent_ID
        LEFT JOIN amex.kant_dim_media d ON a.media_ID = d.media_ID
		GROUP BY
			a.timestamp,
			c.AMEX_PARENT,
			a.amex_parent_ID,
			b.Year,
			b.Month,
			d.MEDIA;'''

    max_date = pd.read_sql_query(qry, conn)

    # make mastercard uniform
    max_date['AMEX_PARENT'] = max_date['AMEX_PARENT'].replace('MasterCard Intl Inc', 'Mastercard Intl Inc')
    max_date.drop_duplicates(inplace=True)
    max_date = max_date.groupby(['AMEX_PARENT', 'MEDIA', 'Year'])['timestamp'].max().to_frame().reset_index()

    qry = '''
        SELECT DISTINCT
            a.timestamp,
            b.Month,
            b.Year,
            a.amex_card_type_ID,
            a.amex_cat_ID,
            a.amex_cobr_ID,
            a.amex_parent_ID,
            a.amex_prod_type_ID,
            a.amex_seg_ID,
            a.media_ID,
            a.media_type_ID,
            a.prod_ID,
            a.cat_ID,
            a.micro_cat_ID,
            a.parent_ID,
            a.adv_ID,
            a.br_ID,
			DOLS
            --SUM(a.DOLS) DOLS
        FROM amex.kant_fact a
        LEFT JOIN amex.kant_dim_date b ON a.date_ID = b.date_ID
        /*GROUP BY
            a.timestamp,
            b.Month,
            b.Year,
            a.amex_card_type_ID,
            a.amex_cat_ID,
            a.amex_cobr_ID,
            a.amex_parent_ID,
            a.amex_prod_type_ID,
            a.amex_seg_ID,
            a.media_ID,
            a.media_type_ID,
            a.prod_ID,
            a.cat_ID,
            a.micro_cat_ID,
            a.parent_ID,
            a.adv_ID,
            a.br_ID*/;
    '''
    df_raw_grouped = pd.read_sql_query(qry, conn)

    # dims we want to call based on the ID in the path_raw_grouped table
    dimensions = [c.replace('_ID', '') for c in df_raw_grouped.columns if 'ID' in c]
    qrys = [f"SELECT * FROM amex.kant_dim_{d};" for d in dimensions]
    tbls = [pd.read_sql_query(qry, conn) for qry in qrys]

    # join to tbls to map dimension ids
    for t in tbls:
        df_raw_grouped = df_raw_grouped.merge(t, how='left')

    df_raw_grouped['AMEX_PARENT'] = df_raw_grouped['AMEX_PARENT'].replace('MasterCard Intl Inc', 'Mastercard Intl Inc')

    # filter to relevant timestamp <> year <> media pair
    df_summ_unmapped = df_raw_grouped.merge(max_date, how='right')

    # set column order
#     df_summ_unmapped = df_raw_grouped.copy()
    col_order = ['Year', 'Month', 'PRODUCT', 'CATEGORY','MICROCATEGORY','PARENT','ADVERTISER', 'BRAND','AMEX_PARENT', 'AMEX_COBRAND','AMEX_CATEGORY', 'AMEX_PRODUCT_TYPE','AMEX_CARD_TYPE', 'AMEX_SEGMENT', 'MEDIA', 'MEDIA_TYPE','DOLS']
    df_summ_unmapped = df_summ_unmapped[col_order]

    df_summ_unmapped['Month'] = pd.to_datetime(df_summ_unmapped['Month'])

    # time check end
    print(f"Kantar Runtime: {np.round(time.time() - start, 2)}")

    return df_summ_unmapped

def summ_unmapped():
	'''
	Returns dictionary dataframe of unmapped kantar and pathmatics data
		Parameters:
			None
		Returns:
			dict_ (dictionary): dictionary object with path and kant as keys and respective dataframes as values
		Examples:
		>>> var = summ_unmapped()
		dict_keys(['path', 'kant'])
	'''
	dict_ = dict()
	dict_['path'] = path_summ_unmapped()
	dict_['kant'] = kant_summ_unmapped()

	return dict_

def final_view_kantar(results):
    # kantar final
    df = results['kant'].copy()

    # add source
    df['Source'] = 'Kantar'

    # rename, keep original
    df['Media_Type'] = df['MEDIA_TYPE']
    df['Amex_Category'] = df['AMEX_CATEGORY']
    df['Dols'] = df['DOLS']
    df['Product'] = df['PRODUCT']

    # filter out: internet display, mobile app, mobile web, online video
    df['MEDIA'] = df['MEDIA'].str.strip()
    df['MEDIA'] = df['MEDIA'].str.lower()
    df = df[-df['MEDIA'].isin(['int display', 'mobile web', 'mobile app', 'online video'])].reset_index(drop=True)

    # rename AMEX PARENT
    df['Amex_Parent'] = df['AMEX_PARENT'].apply(lambda x: meta['01_kantar']['advertiser_rename'][x] if x in meta['01_kantar']['advertiser_rename'].keys() else None)
    df['Amex_Parent'] = df['Amex_Parent'].fillna('<NA>')
    check_amex_parent_mapping(df)

    # map product
    df_map = master_map()
    pre_join_len = len(df)
    df_mapped = df.merge(df_map, how='left', on=['Source', 'Amex_Parent', 'Product'])
    post_join_len = len(df_mapped)

    # check for dupes
    if pre_join_len != post_join_len:
        print(f"Kantar Data Duplication")
        print(f"Pre-Join Length: {pre_join_len}")
        print(f"Post-Join Length: {post_join_len}")
        print(f"Check product mapping for following duplicate fields: ['Source', 'Amex_Parent', 'Product']\n")

    # repalce UM null with <NA>
    df_mapped[['UM_Segment_Split', 'UM_Message', 'UM_CardProduct']] = df_mapped[['UM_Segment_Split', 'UM_Message', 'UM_CardProduct']].fillna('<NA>')
    check_product_mapping(df_mapped)

    # map media type
    df_mapped['UM_Media_Type'] = df_mapped['Media_Type'].apply(lambda x: um_media_type[x] if x in um_media_type.keys() else None)
    df_mapped['UM_Media_Type'] = df_mapped['UM_Media_Type'].fillna('<NA>')
    check_media_type_mapping(df_mapped)

    # final
    df_final = df_mapped.copy()
    group_cols = ['Source', 'Year', 'Month', 'Amex_Parent', 'Amex_Category', 'Media_Type', 'UM_Media_Type', 'Product', 'UM_Segment_Split', 'UM_Message', 'UM_CardProduct']
    df_final = df_final.groupby(group_cols)['Dols'].sum().to_frame().reset_index()

    return df_final

def final_view_pathmatics(results):
    df = results['path'].copy()

    df['Source'] = 'Pathmatics'

    # rename
    df['Dols'] = df['Spend']
    df['Media_Type'] = df['Device']
    df['Amex_Category'] = df['Category']

    # rename
    df['Amex_Parent'] = df['Advertiser'].apply(lambda x: meta['00_pathmatics']['advertiser_rename'][x] if x in meta['00_pathmatics']['advertiser_rename'].keys() else None)
    df['Amex_Parent'] = df['Amex_Parent'].fillna('<NA>')
    check_amex_parent_mapping(df)

    # add and map product
    df['Product'] = df['Brand_Major'].map(str) + ' ' + df['Brand_Leaf'].map(str)

    df_map = master_map()
    pre_join_len = len(df)
    df_mapped = df.merge(df_map, how='left', on=['Source', 'Amex_Parent', 'Product'])
    post_join_len = len(df_mapped)

    # check for dupes
    if pre_join_len != post_join_len:
        print(f"Pathmatics Data Duplication")
        print(f"Pre-Join Length: {pre_join_len}")
        print(f"Post-Join Length: {post_join_len}")
        print(f"Check product mapping for following duplicate fields: ['Source', 'Amex_Parent', 'Product']\n")

    # repalce UM null with <NA>
    df_mapped[['UM_Segment_Split', 'UM_Message', 'UM_CardProduct']] = df_mapped[['UM_Segment_Split', 'UM_Message', 'UM_CardProduct']].fillna('<NA>')
    check_product_mapping(df_mapped)

    # map media type
    df_mapped['UM_Media_Type'] = df_mapped['Media_Type'].apply(lambda x: um_media_type[x] if x in um_media_type.keys() else None)
    df_mapped['UM_Media_Type'] = df_mapped['UM_Media_Type'].fillna('<NA>')
    check_media_type_mapping(df_mapped)

    # final
    df_final = df_mapped.copy()
    group_cols = ['Source', 'Year', 'Month', 'Amex_Parent', 'Amex_Category', 'Media_Type', 'UM_Media_Type', 'Product', 'UM_Segment_Split', 'UM_Message', 'UM_CardProduct']
    df_final = df_final.groupby(group_cols)['Dols'].sum().to_frame().reset_index()

    return df_final

def final_view():
    dt = datetime.strftime(datetime.now(), '%Y%m%d')
    results = summ_unmapped()

    print(f"\nProcessing Kantar\n{'-'*25}")
    df_final_kantar = final_view_kantar(results)

    print(f"\nProcessing Pathmatics\n{'-'*25}")
    df_final_pathmatics = final_view_pathmatics(results)

    print(f"Combining Data\n")
    df_combined = pd.concat([df_final_kantar, df_final_pathmatics], sort=False)
    df_combined['report_date'] = datetime.now().date()

    # export missing products
    export_missing_products(df_combined)

    # keep REMOVE
    df_combined = special_case_adjustments(df_combined)

    # export
    filename = os.path.join(OUTPUTS_PATH, '02_combined', f'Amex_Competitive_{dt}.xlsx')
    df_combined.to_excel(filename, index=False)

    print('\nFile Exported')
