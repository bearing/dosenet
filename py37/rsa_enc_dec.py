# -*- coding: utf-8 -*-
from Crypto.PublicKey import RSA as rsa

class RSAEncDec:
    """A custom wrapper class to offer a useful interface for
    Crypto.PublicKey.RSA from the pyCrypto module.
    """

    def __init__(self, key_file_lst=None):
        if key_file_lst is None:
            key_file_lst = []
        for key_file in key_file_lst:
            key = self.read_key_file(key_file)
            if not key:
                continue
            if key.has_private():
                self.private_key = key
            elif key.can_encrypt():
                self.public_key = key

    def encrypt_message(self, message):
        return self.public_key.encrypt(message, 32)

    def read_key_file(self, key_file):
        try:
            with open(key_file, 'r') as f:
                key = f.read()
        except:
            print('\t\t\t\t\t ERROR')
            print('\t\t\t\t ~~~~ Could not find key file - where is it? ~~~~')
            return []

        return rsa.importKey(key)

    def decrypt_message(self,message):
        return self.private_key.decrypt(message)

