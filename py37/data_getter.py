"""This module has methods to get data from the Raspberry Pis and
decrypt the data for storage.


"""
from Crypto import Random
from Crypto.Cipher import AES
from data_types import (
        AQSensorData,
        CO2SensorData,
        D3SSensorData,
        PGSensorData,
        WeatherSensorData,
        SensorData,
        CSVAble
)
import datetime
import os
from rsa_enc_dec import RSAEncDec
import socket
from socketserver import TCPServer

from typing import List, NewType, Optional, Sequence, Tuple, Union
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
Log = NewType('Log', str)

# const single object instances to be used
RSA_HANDLER = RSAEncDec([PRIVATE_KEY])
AES_HANDLER = get_aes(SYMMETRIC_KEY)

def handle(conn: socket.socket) -> None:
    """The target function called to handle one connection from an RPi."""
    # allows decryption from child process; mypy gives incorrect type error
    Random.atfork()  # type: ignore
    encrypted = get_data(conn)
    decrypted = decrypt_message(encrypted)
    field_list = get_fields(decrypted)
    csv_able = parse_packet(field_list)

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
            datalist: List[bytes] = []
            while bytes_recvd < msg_len:
                request_size = min(msg_len - bytes_recvd, buffer_size)
                datalist.append(conn.recv(request_size))
                bytes_recvd += len(datalist[-1])
            data = b''.join(datalist)
    
        return is_aes, data

def decrypt_message(encrypted: DataWithEncryption) -> Optional[str]:
    """Decrypts the message according to the encryption method and returns
    a string that needs to be parsed to get a CSVAble.
    """
    is_aes, ciphertext = encrypted
    try:
        if is_aes:
            return decrypt_packet_aes(ciphertext).decode('utf8')
        else:
            return decrypt_packet_rsa(ciphertext).decode('utf8')
    except UnencryptedPacket:
        # print to screen. this could be a test message
        print_status(f'Unencrypted packet: {encrypted}', ansi=ANSI_CYAN)
        return None
    except BadPacket:
        print_status(
            'Bad packet (cannot resolve into standard ASCII)', ansi=ANSI_RED)
        return None
    except ValueError:
        # AES: message length not a multiple of block size
        print_status('Bad AES TCP packet (blocksize error)', ansi=ANSI_RED)
        return None

def decrypt_packet_rsa(encrypted: bytes) -> bytes:
    """
    Decrypt packet using RSA private key. Returns decoded bytes.

    May raise UnencryptedPacket or BadPacket.
    """
    # In standard text, all character values should be <128.
    # Encrypted text, or text decrypted with the wrong key, will have
    #   character values up to 255.
    # Also, the length of encrypted stuff should be 256 characters.
    length_encrypted = len(encrypted)
    ascii_values_encrypted = [c for c in encrypted]

    decrypted = RSA_HANDLER.decrypt_message((encrypted,))

    length_decrypted = len(decrypted)
    ascii_values_decrypted = [c for c in decrypted]

    if (length_encrypted != 256 and length_decrypted == 256 and
            all(v < 128 for v in ascii_values_encrypted)):
        raise UnencryptedPacket(
            'Packet lengths suggest that the packet was not encrypted')
    elif (all(v < 128 for v in ascii_values_encrypted) and
            any(v > 127 for v in ascii_values_decrypted)):
        raise UnencryptedPacket(
            'Character codes suggest that the packet was not encrypted')
    elif any(v > 127 for v in ascii_values_decrypted):
        raise BadPacket(
            'Bad character values in decrypted packet (>127): {}'.format(
                ascii_values_decrypted))

    return decrypted

def decrypt_packet_aes(encrypted: bytes) -> bytes:
    """Decrypt packet using AES symmetric key. This is for D3S data.
    Returns decrypted bytes.
    """
    decrypted = AES_HANDLER.decrypt(encrypted)
    ascii_values_decrypted = [c for c in decrypted]

    if any(v > 127 for v in ascii_values_decrypted):
        raise BadPacket(
            'Bad character values in decrypted AES packet: {}'.format(
                ascii_values_decrypted))

    return decrypted

def get_fields(packet: Optional[str]) -> Optional[List[str]]:
    """Split incoming packet on commas. No error checking here.
    Using the Optional as a monad (allows for seamless function composition).
    """
    if packet is None:
        return None
    return packet.split(',')

def parse_packet(field_list: Optional[List[str]]) -> Optional[CSVAble]:
    """Parse the field_list and return something that can be turned into
    a CSV file.
    """

def classify_packet(field_list: List[str]) -> Optional[Union[Log, SensorData]]:
    """Classify the field_list."""
    num_log_fields = 5
    num_data_fields_old = 5
    num_data_fields_new = 6
    num_d3s_fields = 5
    num_AQ_fields = 5
    num_co2_fields = 5
    num_weather_fields = 5

    if (len(field_list) != num_log_fields and
        len(field_list) != num_data_fields_old and
        len(field_list) != num_data_fields_new and
        len(field_list) != num_d3s_fields and
        len(field_list) != num_AQ_fields and
        len(field_list) != num_co2_fields and
        len(field_list) != num_weather_fields):
        raise PacketLengthError(
            'Found {} fields instead of {}, {}, {}, {}, or {}'.format(
                len(field_list),
                num_log_fields, num_data_fields_old, num_data_fields_new,
                num_d3s_fields, num_co2_fields, num_weather_fields))
    elif field_list[2] == 'LOG' and len(field_list) == num_log_fields:
        request_type = 'log'
    elif (len(field_list) == num_AQ_fields and
            field_list[3].startswith('[') and
            len(field_list[3]) >= 40 and
            len(field_list[3]) <= 80):
        request_type = SensorData.AQSensorData
    elif (len(field_list) == num_d3s_fields and
            field_list[3].startswith('[') and
            len(field_list[3]) > 4096):
        request_type = SensorData.D3SSensorData
    elif (len(field_list) == num_co2_fields and
            field_list[3].startswith('[') and
            len(field_list[3]) >= 10 and
            len(field_list[3]) <= 18):
        request_type = SensorData.CO2SensorData
    elif (len(field_list) == num_weather_fields and
            field_list[3].startswith('[') and
            len(field_list[3]) >= 19 and
            len(field_list[3]) <= 25):
        request_type = SensorData.WeatherSensorData
    elif len(field_list) == num_data_fields_old:
        request_type = 'data_old'
    elif len(field_list) == num_data_fields_new:
        request_type = 'data'
    else:
        # currently, this is impossible
        #   unless num_log_fields is different than num_data_fields_*,
        #   and the log is submitted without 'LOG' identifier in position 2
        raise UnknownRequestType()

    return request_type


class InjectorError(Exception):
    pass

class PacketLengthError(InjectorError):
    pass

class HashLengthError(InjectorError):
    pass

class UnknownRequestType(InjectorError):
    pass

class UnencryptedPacket(InjectorError):
    pass

class BadPacket(InjectorError):
    pass

class ExcessiveCountrate(InjectorError):
    pass


def print_status(status_text: str, ansi: Optional[str] = None) -> None:
    """Print a status message:
    [datetime] string
    using ANSI color code, if provided.
    """
    ansi = ansi if ansi is not None else ''
    print(ansi + f'[{datetime.datetime.now()}] ' + status_text + ANSI_RESET)


def get_aes(keyfile) -> AES.AESCipher:
    """Returns a new AESCipher object to be used for AES de/encryption."""
    with open(keyfile, 'r') as f:
        key = f.read()
        assert len(key) == 32, f'Incorrect AES key length. Key:\n{key}'
        return AES.new(key, mode=AES.MODE_ECB)


#################################################
#################################################
###      THE STUFF BELOW MIGHT BE STUPID      ###
#################################################
#################################################

def not_get_data():
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
    

def decrypt_packet(encrypted_packet):
    """Decrypts one `encrypted_packet` using  AES encryption.

    Parameters
    ----------
    encrypted_packet:  the packet to be decrypted

    Returns
    -------
    A dictionary containing the decrypted data that can then be stored in
    a .csv file.
    """

