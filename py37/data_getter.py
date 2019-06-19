"""This module has methods to get data from the Raspberry Pis and
decrypt the data for storage.


"""
from Crypto import Random
from Crypto.Cipher import AES
from data_types import *
import socket
from socketserver import TCPServer

from typing import List, Optional, Sequence, Tuple, Union
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

# type aliases
DataWithEncryption = Tuple[bool, bytes]

def handle(conn: socket.socket) -> None:
    """The target function called to handle one connection from an RPi."""
    Random.atfork()  # allows decryption from child process
    encrypted = get_data(conn)
    decrypted = decrypt_message(encrypted)

def get_data(conn: socket.socket) -> DataWithEncryption:
    """Takes in a socket (connection), fetches the data, and returns it as
    a tuple of boolean and string, where the boolean is True if the message
    is encrypted with AES encryption, and the string contains the actual
    message. The string is a bytes string.
    """
    is_aes: bool
    data: bytes
    with conn:
        firstdata = conn.recv(PREPEND_LENGTH)
        try:
            msg_len = int(firstdata)
        except ValueError:
            # not AES
            is_aes = False
            # so get the remainder of the RSA-encrypted message
            remainder = conn.recv(256)
            data = firstdata + remainder
        else:
            # AES-encrypted data from the D3S needs to be received in parts
            is_aes = True
            bytes_recvd = 0
            buffer_size = 1024
            datalist: List[str] = []
            while bytes_recvd < msg_len:
                request_size = min(msg_len - bytes_recvd, buffer_size)
                datalist.append(conn.recv(request_size))
                bytes_recvd += len(datalist[-1])
            data = b''.join(datalist)
    
        return (is_aes, data)

def decrypt_message(encrypted: DataWithEncryption) -> CSVAble:


#################################################
#################################################
###      THE STUFF BELOW MIGHT BE STUPID      ###
#################################################
#################################################

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

