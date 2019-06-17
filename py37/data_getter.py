"""This module has methods to get data from the Raspberry Pis and
decrypt the data for storage.
"""
from socketserver import TCPServer

from typing import Optional, Sequence, Union
from typing_extensions import Protocol


PRIVATE_KEY = os.path.expanduser('~/.ssh/id_rsa_lbl')
PUBLIC_KEY = os.path.expanduser('~/.ssh/id_rsa_lbl.pub')
SYMMETRIC_KEY = os.path.expanduser('~/secret.aes')

UDP_PORT = 5005
TEST_UDP_PORT = 5006    # for -s mode

TCP_PORT = 5100
TEST_TCP_PORT = 5101    # for -s mode

HASH_LENGTH = 32
PREPEND_LENGTH = 5      # for AES encryption from D3S

ANSI_RESET = '\033[0m'    # reset / default
ANSI_BOLD = '\033[1m'
ANSI_RED = '\033[31m' + ANSI_BOLD
ANSI_GR = '\033[32m' + ANSI_BOLD
ANSI_YEL = '\033[33m' + ANSI_BOLD
ANSI_CYAN = '\033[36m' + ANSI_BOLD
ANSI_MG = '\033[35m' + ANSI_BOLD


def get_data():
    """The function that gets called by `injector.py` to get data.
    This function is NOT pure. It has side-effects, namely, listening
    for incoming data. It 
    Parameters
    ----------
    ...
    Returns
    -------
    ...
    """
    

def decrypt_packet(encrypted_packet, ...):
    """Decrypts one `encrypted_packet` using  AES encryption.

    Parameters
    ----------
    encrypted_packet:  the packet to be decrypted

    Returns
    -------
    A dictionary containing the decrypted data that can then be stored in
    a .csv file.
    """

