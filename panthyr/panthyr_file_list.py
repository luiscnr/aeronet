import os
import pandas as pd
import numpy as np
from panthyr.panthyr_file import Panthyr_File
from datetime import timedelta
from base.rad_base import RadiometerBase


class Panthyr_List(RadiometerBase):

    def __init__(self):
        # super(Panthyr_List, self).__init__()

        self.list_pfiles = {}

        self.nfilesvalid = 0
        self.valid_nwl = True

        self.nwl = -1
        self.wavelength = None
        self.validation_criteria = []
        self.outliers = None

    def add_panthyr_file(self, pfile):
        if pfile.name in self.list_pfiles.keys():
            return
        for criteria in self.validation_criteria:
            pfile.add_validation_criteria(criteria)
        if self.outliers is not None:
            pfile.add_outliers_df(self.outliers, None)

        if self.nwl == -1 and self.wavelength is None:
            self.nwl = pfile.nwl
            self.wavelength = pfile.wavelenght
        else:
            if self.nwl != pfile.nwl:
                self.valid_nwl = False

        valid_here = pfile.check_validation_criteria()
        if not self.valid_nwl:
            valid_here = False
        if valid_here:
            self.nfilesvalid = self.nfilesvalid + 1

        self.list_pfiles[pfile.name] = {
            'valid': valid_here,
            'time': pfile.time,
            'path': pfile.path
        }

    # Defining thershold to consider a spectra as not valid.
    # thtype: gt (no valid if rrs>thvalue) or lw(no valid if rrs<thvalue)
    def add_th_validation_criteria(self, wlmin, wlmax, thvalue, thtype):
        self.validation_criteria = super().add_th_validation_criteria(self.validation_criteria, self.wavelength,
                                                                      self.nwl, wlmin, wlmax, thvalue, thtype)

    # Spectra is valid if (hour:minute - time) < diffmax (in seconds)
    # if hour is None use hour defined in time
    def add_time_validation_criteria(self, hour, minute, diffmax):
        self.validation_criteria = super().add_time_validation_criteria(self.validation_criteria, hour, minute, diffmax)

    def add_outliers_df(self, dfoutliers, fpath):
        if dfoutliers is None and fpath is not None:
            dfoutliers = pd.read_csv(fpath,sep=';')
        self.outliers = dfoutliers

    def get_dfvalid_spectra(self, ref):
        if ref is None:
            ref = ''
        colnames = ['ID', 'TIME', 'AZIMUTH']
        for wl in self.wavelength:
            wls = f'{ref}{str(wl)}'
            colnames.append(wls)
        df_valid_spectra = pd.DataFrame(columns=colnames, index=range(self.nfilesvalid))

        ncol = len(colnames)
        index = 0
        for l in self.list_pfiles:
            if self.list_pfiles[l]['valid']:
                pfile = Panthyr_File(self.list_pfiles[l]['path'])
                df_valid_spectra.iloc[index, 3:ncol] = np.transpose(np.array(pfile.rrs))
                df_valid_spectra.iloc[index, 0] = index
                df_valid_spectra.iloc[index, 1] = pfile.time.strftime('%Y-%m-%d %H:%M:%S')
                df_valid_spectra.iloc[index, 2] = pfile.azimuth
                index = index + 1

        return df_valid_spectra

    def compute_statistics_from_valid_spectra(self, df_valid_spectra):
        stats = {}
        for index in range(self.nwl):
            wls = str(self.wavelength[index])
            iwl = index + 3
            stats[wls] = {
                'index': index,
                'avg': np.mean(df_valid_spectra.iloc[:, iwl]),
                'std': np.std(df_valid_spectra.iloc[:, iwl]),
                'min': np.min(df_valid_spectra.iloc[:, iwl]),
                'max': np.max(df_valid_spectra.iloc[:, iwl]),
                'median': np.median(df_valid_spectra.iloc[:, iwl])
            }
        return stats

    def create_list_from_folder(self, folder, azimuth_list):
        for f in os.listdir(folder):
            if f.endswith('data.csv'):
                fpath = os.path.join(folder, f)
                pfile = Panthyr_File(fpath)
                if azimuth_list is not None and pfile.azimuth in azimuth_list:
                    self.add_panthyr_file(pfile)
                if azimuth_list is None:
                    self.add_panthyr_file(pfile)

    def create_list_from_folder_dates(self, path_base, date_ini, date_fin, azimuth_list):
        date = date_ini
        while date <= date_fin:
            year_str = date.strftime('%Y')
            month_str = date.strftime('%m')
            day_str = date.strftime('%d')
            folder = os.path.join(path_base, year_str, month_str, day_str)
            print(f'[INFO] Data: {date}')
            if os.path.exists(folder) and os.path.isdir(folder):
                self.create_list_from_folder(folder, azimuth_list)
            date = date + timedelta(hours=24)

    # Outliers for each wavelength, computed as avg+-(param*std)
    def save_outliers_asfile(self, param, file):
        df_valid = self.get_dfvalid_spectra('')
        stats = self.compute_statistics_from_valid_spectra(df_valid)
        df_outliers = pd.DataFrame(columns=['MinTh', 'MaxTh'], index=self.wavelength)
        for wls in stats:
            index = stats[wls]['index']
            df_outliers.iloc[index, 0] = stats[wls]['avg'] - (param * stats[wls]['std'])
            df_outliers.iloc[index, 1] = stats[wls]['avg'] + (param * stats[wls]['std'])
        df_outliers.to_csv(file)
