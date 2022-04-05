import datetime

import pandas as pd
import os
from datetime import datetime as dt


class SKIE_CSV():
    def __init__(self, fcsv):
        self.df_main = None
        self.col_id = 'id'
        self.col_time = 'time'
        self.col_lat = 'lat'
        self.col_lon = 'lon'
        if os.path.exists(fcsv):
            self.df_main = pd.read_csv(fcsv)

        self.dates = {}

        self.df_sub = None

    def start_list_dates(self):
        if self.df_main is None:
            print('[ERROR] Main data frame is not defined')
            return
        if self.col_time not in self.df_main.columns:
            print(f'[ERROR] Col time: {self.col_time} is not available')
            return
        ndates = 0
        for index, row in self.df_main.iterrows():
            try:
                datehere = dt.strptime(row[self.col_time], '%Y-%m-%d %H:%M:%S')
                dateval = datehere.strftime('%Y%m%d')
                if not dateval in self.dates.keys():
                    self.dates[dateval] = {
                        'indices': [index]
                    }
                    ndates = ndates + 1
                else:
                    self.dates[dateval]['indices'].append(index)
            except ValueError:
                print(f'[WARNING] {row[self.col_time]} is not a valid date/time object')

        return ndates

    # maxtimedif in seconds
    def get_subdf(self, dateref, sattime, maxtimedif):
        if len(self.dates) == 0:
            ndates = self.start_list_dates()
            if ndates == 0:
                print(f'[ERROR] Dates are note available in the file')
                return False

        if isinstance(dateref, dt) or isinstance(dateref, datetime.date):
            daterefstr = dateref.strftime('%Y%m%d')
        elif isinstance(dateref, str):
            daterefstr = dateref
            try:
                dt.strptime(daterefstr, '%Y%m%d')
            except ValueError:
                print(f'[ERROR] {daterefstr} is not in the correct date format (%Y%m%d)')
                return False

        if daterefstr not in self.dates.keys():
            print(f'[WARNING] Date: {daterefstr} is not available in the file')
            return False

        indices = self.dates[daterefstr]['indices']
        df_limited = self.df_main.iloc[indices]

        # self.df_sub = pd.DataFrame(columns=df_limited.columns)
        # started = False
        list_id = []
        indices_good = []
        ngood = 0
        for index, row in df_limited.iterrows():
            idhere = row[self.col_id]
            if not idhere in list_id:
                valid = True
                if sattime is not None:
                    datehere = dt.strptime(row[self.col_time], '%Y-%m-%d %H:%M:%S')
                    timedifhere = abs((datehere - sattime).total_seconds())
                    if timedifhere > maxtimedif:
                        valid = False
                if valid:
                    list_id.append(idhere)
                    indices_good.append(index)
                    ngood = ngood + 1

        if ngood == 0:
            print(f'[WARNING] No spectra found within the time window')
            return False

        self.df_sub = df_limited.loc[indices_good]
        self.df_sub = self.df_sub.sort_values(self.col_time)

        return True

    def get_lat_lon_from_subdb(self):
        lat_list = []
        lon_list = []
        if self.df_sub is None:
            return lat_list, lon_list
        for index, row in self.df_sub.iterrows():
            #print(row[self.col_time], row[self.col_lat], row[self.col_lon])
            lat_list.append(row[self.col_lat])
            lon_list.append(row[self.col_lon])
        return lat_list, lon_list
