from __future__ import print_function
import os
import time
from utils import print_divider


REMOTE_USERNAME = 'jccurtis'
WEBSERVER_ADDRESS = 'kepler.berkeley.edu'
# Default geojson paths
LOCAL_GEOJSON_PATH = os.path.join(os.getcwd(), 'tmp/geojson/')
REMOTE_GEOJSON_PATH = \
    '/var/www/html/htdocs-nuc-groups/radwatch/sites/default/files/'
# Default geojson base filename
GEOJSON_FNAME_BASE = 'output.geojson'
# Default CSV paths
LOCAL_CSV_PATH = os.path.join(os.getcwd(), 'tmp/csv/')
REMOTE_CSV_PATH = os.path.join(REMOTE_GEOJSON_PATH, 'dosenet/')


def mkdir(path):
    if not os.path.isdir(path):
        print('MAKING DIRECTORY:', path)
        os.makedirs(path)
    else:
        pass


def get_byte_size(fname):
    nbytes = float(os.path.getsize(fname))
    byte_units = ['Bytes', 'kB', 'MB', 'GB', 'TB']
    for i, u in enumerate(byte_units):
        if nbytes < (1000. ** (i + 1)):
            return '{:.2f} {}'.format(nbytes / (1024. ** i), u)
    raise TypeError('File larger than 1000 TB?: {}'.format(fname))


def send_to_webserver(local_fnames, remote_path, username=REMOTE_USERNAME,
                      server_address=WEBSERVER_ADDRESS, testing=False):
    """
    Transfer files to webserver (DECF KEPLER). Should be run under 'dosenet'
    linux user so that the SSH keypair setup between DOSENET & DECF Kepler
    works without login

    Inputs:
        local_fnames(iterable) : local file paths
        remote_path(str) : remote directory
        username(str) : remote username
    """
    tic = time.time()
    print_divider()
    print('Webserver transfer:')
    local_fnames_to_send = []
    for fname in local_fnames:
        if not os.path.isfile(fname):
            print('Cannot locate:', fname)
        else:
            local_fnames_to_send.append(fname)
    if len(local_fnames_to_send) == 0:
        print('No files to send, exiting ...')
        return None
    cmd = 'rsync -azvh '
    cmd += ' '.join(local_fnames_to_send) + ' '
    cmd += '{}@{}:'.format(username, server_address)
    cmd += '{}'.format(remote_path.rstrip('/') + '/')
    print(cmd)
    if testing:
        print('Testing mode, not sending ...')
    else:
        try:
            # Run the transfer cmd and wait until it returns
            os.system(cmd)
            print('Success!')
        except Exception as e:
            print('Network Error')
            print(e)
    print('DONE ({:.2f} s)'.format(time.time() - tic))
    print_divider()


def nickname_to_remote_csv_fname(nickname, **kwargs):
    """Shortcut to get remote fname from nickname"""
    csvfile = DataFile.csv_from_nickname(nickname, **kwargs)
    return csvfile.remote_fname


class DataFile(object):

    def __init__(self, base_fname, local_path, remote_path, **kwargs):
        """
        Inputs:
            base_fname : Filename without directory
            local_path : Path of local directory
            remote_path : Path of remote directory
            username : Kepler (webserver) username
        """
        self.base_fname = base_fname
        self.local_path = local_path
        self.remote_path = remote_path
        mkdir(self.local_path)

    @classmethod
    def csv_from_nickname(cls, nickname, local_path=LOCAL_CSV_PATH,
                          remote_path=REMOTE_CSV_PATH, **kwargs):
        obj = cls(
            base_fname=nickname + '.csv',
            local_path=local_path,
            remote_path=remote_path,
            **kwargs)
        return obj

    @classmethod
    def default_geojson(cls):
        obj = cls(
            base_fname=GEOJSON_FNAME_BASE,
            local_path=LOCAL_GEOJSON_PATH,
            remote_path=REMOTE_GEOJSON_PATH)
        return obj

    def send_to_webserver(self, testing=False):
        send_to_webserver(
            local_fnames=[self.local_fname],
            remote_path=self.remote_path,
            testing=testing)

    @property
    def local_fname(self):
        """Full local path to file"""
        return os.path.join(self.local_path, self.base_fname)

    @property
    def remote_fname(self):
        """Full remote path to file"""
        return os.path.join(self.remote_path, self.base_fname)

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
            print('Cannot write here:', self.local_fname)
            print(e)

    def print_local_file_saved(self):
        print('Saved ({}): {}'.format(
            get_byte_size(self.local_fname), self.local_fname))
