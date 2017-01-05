#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import argparse
from data_transfer import send_to_webserver, LOCAL_DATA_DIR


def main(testing=False, verbose=False):
    files = os.path.join(LOCAL_DATA_DIR, '*')
    send_to_webserver(files, testing=testing)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send DoseNet data to webserver")
    parser.add_argument('-t', '--testing', action='store_true', default=False,
                        help='Testing mode to not send data to KEPLER')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print more output')
    args = parser.parse_args()
    main(**vars(args))
