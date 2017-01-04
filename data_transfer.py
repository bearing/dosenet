from __future__ import print_function
import os
import time

REMOTE_USERNAME = 'jccurtis'
# Default geojson paths
LOCAL_GEOJSON_PATH = os.path.join(os.getcwd(), 'tmp/geojson/')
REMOTE_GEOJSON_PATH = \
    '/var/www/html/htdocs-nuc-groups/radwatch/sites/default/files/'
# Default geojson filename
GEOJSON_FNAME = 'output.geojson'
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


def scp_to_webserver(fname_local, fname_remote, username=REMOTE_USERNAME,
                     testing=False):
    """
    Transfer file to webserver (DECF KEPLER)
    Must be run under 'dosenet' linux user so that the SSH keypair setup
    between DOSENET & DECF Kepler works without login
    Not ideal: uses Joseph Curtis' account (jccurtis) for the SCP
    """
    cmd = 'scp '
    cmd += '{} '.format(fname_local)
    cmd += '{}@kepler.berkeley.edu:'.format(username)
    cmd += '{}'.format(fname_remote)
    print('Webserver transfer:')
    print('    {}'.format(cmd))
    if testing:
        print('    Not executed (testing)')
    else:
        try:
            tic = time.time()
            os.system(cmd)
            print('    Success! ({:.2f} s)'.format(time.time() - tic))
        except Exception as e:
            print('    Network Error ({:.2f} s)'.format(time.time() - tic))
            print(e)


class FileForWebserver(object):

    def __init__(self, local_path, remote_path, username=REMOTE_USERNAME,
                 **kwargs):
        """
        Default username is here but can be editted from descedants with kwargs

        Inputs:
            local_path : Path of local directory
            remote_path : Path of remote directory
            username : Kepler (webserver) username
        """
        self.username = username
        self.local_path = local_path
        self.remote_path = remote_path
        mkdir(self.local_path)
        self.username = username

    def send_to_webserver(self, testing=False):
        scp_to_webserver(
            fname_local=self.local_fname,
            fname_remote=self.remote_fname,
            username=self.username,
            testing=testing)

    @property
    def local_fname(self):
        """Full local path to file"""
        return os.path.join(self.local_path, self.fname)

    @property
    def remote_fname(self):
        """Full remote path to file"""
        return os.path.join(self.remote_path, self.fname)

    def set_fname(self, fname):
        """Base filename"""
        self.fname = fname

    def get_fname(self):
        """Get base filename"""
        return self.fname

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
        print('Saved ({}):\n    {}'.format(
            get_byte_size(self.local_fname), self.local_fname))


class CsvForWebserver(FileForWebserver):

    def __init__(self, local_path=LOCAL_CSV_PATH, remote_path=REMOTE_CSV_PATH,
                 **kwargs):
        """Defaults for local and remote are contained in this init"""
        super(CsvForWebserver, self).__init__(
            local_path=local_path, remote_path=remote_path, **kwargs)

    @classmethod
    def from_nickname(cls, nickname, **kwargs):
        obj = cls(**kwargs)
        obj.set_fname(nickname + '.csv')
        return obj

    @staticmethod
    def get_remote_csv_fname_from_nickname(nickname, **kwargs):
        """Shortcut to get remote fname from nickname"""
        csvfile = CsvForWebserver.from_nickname(nickname, **kwargs)
        return csvfile.remote_fname


class GeoJsonForWebserver(FileForWebserver):

    def __init__(self, local_path=LOCAL_GEOJSON_PATH,
                 remote_path=REMOTE_GEOJSON_PATH, **kwargs):
        """Defaults for local and remote are contained in this init"""
        super(GeoJsonForWebserver, self).__init__(
            local_path=local_path, remote_path=remote_path, **kwargs)

    @classmethod
    def from_fname(cls, fname=GEOJSON_FNAME, **kwargs):
        obj = cls(**kwargs)
        obj.set_fname(fname)
        return obj
