"""This module has methods to get data from the Raspberry Pis and
decrypt the data for storage.
"""

def get_data(...):
    """The function that gets called by `injector.py` to get data.
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

