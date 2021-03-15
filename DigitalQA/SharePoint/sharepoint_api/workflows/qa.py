from datetime import datetime, timedelta
from itertools import chain
import pandas as pd
import openpyxl
import glob
import os

import sys
sys.path.append('../utils')
from static import *
from hlpr import *
from qa_by_media import *
from format_qa import *
from SharePoint import *

def archive_data():
    app = SharePoint('AmericanExpressUSGABM')

    print(f"\n archiving data\n{'='*50}")

    for media in medias:
        SHAREPOINT_DATA_PATH = media_args[media]['sharepoint']['data']
        app.list_contents(SHAREPOINT_DATA_PATH)
        app.download_files(DATA_PATH)
        app.archive_files()

        if media == 'digital':
            app.list_contents(media_args[media]['sharepoint']['ancillary_data'])
            app.download_files(DATA_PATH)
            app.archive_files()

    delete_local_data(False)

    return None

def qa():
    for media in medias:
        print(f"\n{'='*50}\nProcessing {media}\n{'='*50}")
        SHAREPOINT_DATA_PATH = media_args[media]['sharepoint']['data']
        SHAREPOINT_MAPPING_PATH = media_args[media]['sharepoint']['mapping']
        SHAREPOINT_QA_PATH = media_args[media]['sharepoint']['qa']
        SHAREPOINT_FLAT_PATH = media_args[media]['sharepoint']['flat']
        REDSHIFT_METRICS_QUERY = media_args[media]['redshift']['all_metrics']
        REDSHIFT_METRICS_FILENAME = media_args[media]['redshift']['metric_filename']
        REDSHIFT_METRICS_SHEETNAME = media_args[media]['redshift']['metric_sheetname']
        REDSHIFT_QA_QUERY = media_args[media]['redshift']['qa_qry']

        start = time.time()

        # initiate SharePoint
        app = SharePoint('AmericanExpressUSGABM')
        app.list_contents(SHAREPOINT_DATA_PATH)

        # skip loop if empty
        if len(app.file_paths)==0:
            print('no files to download')
            continue

        app.download_files(DATA_PATH)
        print(f"    Downloading UI data to: {DATA_PATH}")

        # download dcm data to add to twitter
        ancillary_data(app, media)

        # download floodlight mapping
        print(f"    Downloading mapping to: {DATA_PATH}")
        app.list_contents(SHAREPOINT_MAPPING_PATH)
        app.download_files(ASSETS_PATH)

        # create qa file
        qa_results = media_qa(media, REDSHIFT_METRICS_FILENAME, REDSHIFT_QA_QUERY)
        qa_filename = create_qa_file(media, app.dt, qa_results)
        qa_filepath = os.path.join(OUTPUTS_PATH, qa_filename)
        print(f"    QA file saved: {qa_filepath}")
        format_qa(media, qa_filepath)
        print(f"    QA file formatted")

        # upload to SharePoint
        # local_outputs_path = max(glob.glob(os.path.join(OUTPUTS_PATH, 'QA*')), key=os.path.getmtime)
        # app.upload_files(SHAREPOINT_QA_PATH, local_outputs_path)

        # create flat file
        # flat file function

        # delete data in data, outputs
        delete_local_data(False)

        runtime = np.round(time.time() - start, 2)
        print(f"\nRuntime: {runtime} seconds")

        update_log(media, qa_filename, runtime)

if __name__ == '__main__':
    medias = choose_medias()
    qa()
    archive_data()
