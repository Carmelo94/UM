from datetime import datetime
from hlpr import *
import pandas as pd
import numpy as np
import time
import glob
import os

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


current_directory = os.getcwd()

class SharePoint():
    create_folders()
    current_directory = os.getcwd()
    dt = datetime.strftime(datetime.now(), '%Y%m%d')

    def __init__(self, sharepoint_site_name):
        '''
        Initiate with the SharePoint site name.
        '''
        APP_SETTINGS = get_app_settings(sharepoint_site_name)
        self.ctx = ctx_hlpr(APP_SETTINGS['url'], APP_SETTINGS['client_id'], APP_SETTINGS['client_secret'])
        self.base_site_path = APP_SETTINGS['base_site_path']
        self.file_paths = []
        self.folder_paths = []
        self.local_data_path = []
        # self.local_outputs_path = []

    def list_contents(self, sharepoint_folder_path):
        '''Get the list of files from a folder'''

        # print(f"listing files from: {sharepoint_folder_path}\n")

        if sharepoint_folder_path is None:
            print('\nno mapping file to reference')
        else:
            contents = printAllContents(self.ctx, sharepoint_folder_path)
            self.folder_paths = [f for f in contents['folders']]

            # append file paths to self.file_paths
            if len(contents['files']) > 0:
                self.file_paths = [f for f in contents['files']]

    def download_files(self, local_download_path=current_directory):
        '''Download files from SharePoint to a local path'''

        # print(f"Downloading UI data to: {local_download_path}\n")

        if len(self.file_paths) > 0:
            self.local_data_path = local_download_path
            for f in self.file_paths:
                # print(f"downloading: {f}\n")
                response = File.open_binary(self.ctx, f)
                response.raise_for_status

                filename = os.path.split(f)[-1]
                local_path = os.path.join(local_download_path, filename)

                with open(local_path, 'wb') as local_file:
                    local_file.write(response.content)
                    # print(f"file downloaded: {local_path}")
        # else:
        #     print(f"no file paths to reference")

    def archive_files(self, date=dt):
        '''Archive files to a dated folder'''

        print('archiving data in SharePoint\n')

        # create folder
        for f in self.file_paths:
            folder = os.path.split(f)[0]
            dated_archive = os.path.join(folder, date)
            target_folder = self.ctx.web.folders.add(dated_archive)
            self.ctx.execute_query()

        # try:
        for f in self.file_paths:
            archive_name = f"{os.path.split(f)[-1]}"
            archive_path = os.path.join(os.path.split(f)[0], date, archive_name)

            source_file = self.ctx.web.get_file_by_server_relative_url(f)
            # source_file.moveto(archive_path, 1)

            try:
                source_file.moveto(archive_path, 1)
                self.ctx.execute_query()
            except:
                if archive_name in os.listdir(self.local_data_path):
                    file_to_delete = self.ctx.web.get_file_by_server_relative_url(f)
                    file_to_delete.delete_object()
                    self.ctx.execute_query()

                    info = FileCreationInformation()
                    idx = findn(archive_path, '/', 3)
                    archive_path = archive_path[idx+1:]
                    save_to_path = os.path.split(archive_path)[0]
                    upload_files_hlpr(self.ctx, save_to_path, os.path.join(self.local_data_path, archive_name), info)

        # # clear list
        self.file_paths = []

    def upload_files(self, sharepoint_upload_path, local_file_path):
        '''
        Upload file(s) to SharePoint.
            Parameters:
                sharepoint_upload_path (path): destination SharePoint link path
                    example: /sites/<site_name>/<folder>/<sub_folder>

                local_file_path (path): local source paths, input can be path or list of paths
                    example (1): /<local_foler>/<file.ext>
                    example (2): [/<local_foler>/<file1.ext>, /<local_foler>/<file2.ext>]
        '''

        print(f"    Uploading to SharePoint")
        print(f"    from: {local_file_path}\nto: {sharepoint_upload_path}\n")

        info = FileCreationInformation()

        if type(local_file_path) is list:
            for path in local_file_path:
                upload_files_hlpr(self.ctx, sharepoint_upload_path, path, info)
        else:
            upload_files_hlpr(self.ctx, sharepoint_upload_path, local_file_path, info)
