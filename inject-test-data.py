#!/usr/bin/env python
"""
July 2016 Rewrite:
Joseph Curtis

Original Authors:
Navrit Bal
Tigran Ter-Stepanyan
"""
from __future__ import print_function
import time
import numpy as np
from mysql import SQLObject


def main(stationID, hours=1., interval_min=5., random_cpm=False):
    dosenet = SQLObject()
    currentTime = time.time()
    deviceTimes = np.arange(
        currentTime,
        currentTime - hours * 60. * 60.,
        -interval_min * 60.)
    if random_cpm:
        # Scale normal dist with mean of 2 and std of 1
        cpms = np.random.randn(len(deviceTimes)) + 2.
    else:
        cpms = np.zeros(len(deviceTimes))
    for deviceTime, cpm in zip(deviceTimes, cpms):
        print('{}'.format(deviceTime))
        dosenet.insertIntoDosenet(
            stationID=stationID,
            cpm=cpm,
            cpm_error=np.sqrt(cpm),
            error_flag=0,
            deviceTime=deviceTime)
        time.sleep(0.01)
    dosenet.close()
    print('Successfully injected {} points for station {}'.format(
        len(deviceTimes), stationID))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--stationID', type=int, default=0,
                        help='Input the station integer id (default=0)')
    parser.add_argument('--hours', type=float, default=1.,
                        help='Number of past hours to inject')
    parser.add_argument('-i', '--interval_min', type=float, default=5.,
                        help='Time interval (minutes) between injection points')
    parser.add_argument('-r', '--random_cpm', default=False, action='store_true',
                        help='Inject random cpm instead of zeros')
    args = parser.parse_args()

    main(**vars(args))
