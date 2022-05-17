import pandas as pd
import numpy as np
from datetime import datetime as dt
from base.rad_base import RadiometerBase


class Panthyr_File(RadiometerBase):

    def __init__(self, path_csv):
        self.base = super(Panthyr_File, self).__init__()
        dfmain = pd.read_csv(path_csv)

        self.path = path_csv
        self.name = path_csv.split('/')[-1][:-3]


        self.rrs = dfmain['rhow'] / np.pi

        self.azimuth = dfmain['azimuth'][0]
        timestamp = dfmain['timestamp'][0]
        self.time = dt.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

        self.validation_criteria = []
        self.outliers = None
        self.wavelenght = dfmain['wavelength']
        self.nwl = len(self.wavelenght)

    # # Defining thershold to consider a spectra as not valid.
    # # thtype: gt (no valid if rrs>thvalue) or lw(no valid if rrs<thvalue)
    def add_th_validation_criteria(self, wlmin, wlmax, thvalue, thtype):
        self.validation_criteria = super().add_th_validation_criteria(self.validation_criteria,self.wavelenght,self.nwl,wlmin,wlmax,thvalue,thtype)

    def add_time_validation_criteria(self, hour, minute, diffmax):
        self.validation_criteria = super().add_time_validation_criteria(self.validation_criteria,hour,minute,diffmax)

    def add_validation_criteria(self, criteria):
        self.validation_criteria = super().add_validation_criteria(self.validation_criteria,self.wavelenght,self.nwl,criteria)


    def add_outliers_df(self, dfoutliers, fpath):
        if dfoutliers is None and fpath is not None:
            dfoutliers = pd.read_csv(fpath)
        self.outliers = dfoutliers

    def check_validation_criteria(self):
        valid = True
        for index in range(self.nwl):
            wl_here = self.wavelenght[index]
            rrs_here = self.rrs[index]

            if self.outliers is not None:
                if rrs_here < self.outliers.iloc[index].at['MinTh']:
                    valid = False
                if rrs_here > self.outliers.iloc[index].at['MaxTh']:
                    valid = False

            if not valid:
                break

            for criteria in self.validation_criteria:
                if criteria['type'] == 'th' and criteria['wlmin'] <= wl_here <= criteria['wlmax']:
                    if criteria['thtype'] == 'gt' and rrs_here > criteria['thvalue']:
                        valid = False
                    if valid and criteria['thtype'] == 'lt' and rrs_here < criteria['thvalue']:
                        valid = False
                if criteria['type'] == 'time':
                    time_ref = self.time
                    time_ref = time_ref.replace(minute=criteria['minute'])
                    if criteria['hour'] is not None:
                        time_ref = time_ref.replace(minute=criteria['hour'])
                    diff_here = abs((self.time - time_ref).total_seconds())
                    if diff_here > criteria['diffmax']:
                        valid = False
            if not valid:
                break
        return valid
