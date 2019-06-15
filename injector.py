"""This script uses the module `data_getter.py` to acquire and decrypt
data coming from the Raspberry Pis, and then writes them to `.csv` files.
"""
from data_types import *
from typing import Sequence, Union
from typing_extensions import Protocol

def inject_data(data: Sequence[CSVAble],
                filename: str):
    """
    Writes the data in `field_dict` to `filename`.
    
    Parameters
    ----------
    field_dict:  dictionary containing data
    filename:    location to write the files at; should be .csv
    """
    with open(filename, 'w+') as f:
        # TODO: get field names from ../injector.format_packet(...)
        for line in data:
            f.write(line.to_csv())
