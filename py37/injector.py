"""This script uses the module `data_getter.py` to acquire and decrypt
data coming from the Raspberry Pis, and then writes them to `.csv` files.
"""

def inject_data(field_dict, filename):
    """
    Writes the data in `field_dict` to `filename`.
    
    Parameters
    ----------
    field_dict:  dictionary containing data
    filename:    location to write the files at; should be .csv
    """
    with open(filename, 'w+') as f:
        # TODO: get field names from ../injector.format_packet(...)
