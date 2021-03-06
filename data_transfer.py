#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import time
from utils import print_divider, mkdir

REMOTE_USERNAME = 'jhanks'
WEBSERVER_ADDRESS = 'kepler.berkeley.edu'
# Default paths
LOCAL_DATA_DIR = os.path.join(os.getcwd(), 'tmp/')
REMOTE_DATA_DIR = '/var/www/html/htdocs-nuc-groups/radwatch/sites/default/files/'
# Default geojson paths
LOCAL_GEOJSON_DIR = LOCAL_DATA_DIR
REMOTE_GEOJSON_DIR = REMOTE_DATA_DIR
# Default CSV paths
LOCAL_CSV_DIR = os.path.join(LOCAL_DATA_DIR, 'dosenet/')
REMOTE_CSV_DIR = os.path.join(REMOTE_DATA_DIR, 'dosenet/')
# Default geojson base filename
GEOJSON_FNAME_BASE = 'output.geojson'
# Make dirs
mkdir(LOCAL_DATA_DIR)
mkdir(LOCAL_GEOJSON_DIR)
mkdir(LOCAL_CSV_DIR)


def get_byte_size(fname):
    nbytes = float(os.path.getsize(fname))
    byte_units = ['Bytes', 'kB', 'MB', 'GB', 'TB']
    for i, u in enumerate(byte_units):
        if nbytes < (1000. ** (i + 1)):
            return '{:.2f} {}'.format(nbytes / (1024. ** i), u)
    raise TypeError('File larger than 1000 TB?: {}'.format(fname))


def send_to_webserver(local_fnames, remote_dir=REMOTE_DATA_DIR,
                      username=REMOTE_USERNAME,
                      server_address=WEBSERVER_ADDRESS, testing=False):
    """
    Transfer files to webserver (DECF KEPLER). Should be run under 'dosenet'
    linux user so that the SSH keypair setup between DOSENET & DECF Kepler
    works without login

    Inputs:
        local_fnames(iterable or str) : local file paths
        remote_dir(str) : remote directory
        username(str) : remote username
    """
    tic = time.time()
    print_divider()
    print('Webserver transfer:')
    if isinstance(local_fnames, (list, tuple)):
        local_fnames_to_send = []
        for fname in local_fnames:
            if not os.path.isfile(fname):
                print('Cannot locate:', fname)
            else:
                local_fnames_to_send.append(fname)
        if len(local_fnames_to_send) == 0:
            print('No files to send, exiting ...')
            return None
        fname_str = ' '.join(local_fnames_to_send)
    elif isinstance(local_fnames, str):
        fname_str = local_fnames
    else:
        raise TypeError('`local_fnames` should be iterable or string:',
                        local_fnames)

    cmd = 'rsync -rzvh '
    cmd += fname_str + ' '
    cmd += '{}@{}:'.format(username, server_address)
    cmd += '{}'.format(remote_dir.rstrip('/') + '/')
    print(cmd)
    if testing:
        print('Testing mode, not sending ...')
    else:
        try:
            # Run the transfer cmd and wait until it returns
            os.system(cmd)
            print('Success!')
        except Exception as e:
            print('Error!')
            print(e)
    print('DONE ({:.2f} s)'.format(time.time() - tic))
    print_divider()


def nickname_to_remote_csv_fname(nickname, **kwargs):
    """Shortcut to get remote fname from nickname"""
    csvfile = DataFile.csv_from_nickname(nickname, **kwargs)
    return csvfile.base_fname


class DataFile(object):

    def __init__(self, base_fname, local_dir, remote_dir, **kwargs):
        """
        Inputs:
            base_fname : Filename without directory
            local_dir : Path of local directory
            remote_dir : Path of remote directory
            username : Kepler (webserver) username
        """
        self.base_fname = base_fname
        self.local_dir = local_dir
        self.remote_dir = remote_dir
        mkdir(self.local_dir)

    @classmethod
    def csv_from_nickname(cls, nickname):
        obj = cls(
            base_fname=nickname + '.csv',
            local_dir=LOCAL_CSV_DIR,
            remote_dir=REMOTE_CSV_DIR)
        return obj

    @classmethod
    def default_geojson(cls):
        obj = cls(
            base_fname=GEOJSON_FNAME_BASE,
            local_dir=LOCAL_GEOJSON_DIR,
            remote_dir=REMOTE_GEOJSON_DIR)
        return obj

    @classmethod
    def json_from_nickname(cls, nickname):
        obj = cls(
            base_fname=nickname + '.json',
            local_dir=LOCAL_CSV_DIR,
            remote_dir=REMOTE_CSV_DIR)
        return obj

    def send_to_webserver(self, testing=False):
        send_to_webserver(
            local_fnames=[self.local_fname],
            remote_dir=self.remote_dir,
            testing=testing)

    @property
    def local_fname(self):
        """Full local path to file"""
        return os.path.join(self.local_dir, self.base_fname)

    @property
    def remote_fname(self):
        """Full remote path to file"""
        return os.path.join(self.remote_dir, self.base_fname)

    def open_file(self):
        self.file = open(self.local_fname, 'w')

    def get_file(self):
        return self.file

    def close_file(self):
        self.file.close()

    def write_to_file(self, data_string):
        self.open_file()
        try:
            self.get_file().write(data_string)
            self.print_local_file_saved()
        except Exception as e:
            print('Cannot write here:', self.local_fname)
            print(e)
        finally:
            self.close_file()

    def df_to_file(self, df):
        try:
            df.to_csv(self.local_fname, index=None)
            self.print_local_file_saved()
        except Exception as e:
            print('Cannot write here: {}'.format(self.local_fname))
            print(e)

    def df_to_json(self, df):
        try:
            df.to_json(self.local_fname)
            self.print_local_file_saved()
        except Exception as e:
            print('Cannot write here: {}'.format(self.local_fname))
            print(e)

    def print_local_file_saved(self):
        print('Saved ({}): {}'.format(
            get_byte_size(self.local_fname), self.local_fname))
