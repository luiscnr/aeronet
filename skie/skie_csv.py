import datetime

import pandas as pd
import numpy as np
import os
from datetime import datetime as dt

import haversine as hs


class SKIE_CSV():
    def __init__(self, fcsv):
        self.df_main = None
        self.col_id = 'id'
        self.col_time = 'time'
        self.col_lat = 'lat'
        self.col_lon = 'lon'
        self.wavelengths = []
        self.col_wl = []
        self.col_wl_prefix = 'Rrs_'
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

    def extract_wl_colnames(self):
        started = False
        if len(self.col_wl) > 0:
            return
        for icol in range(len(self.df_main.columns)):
            col = self.df_main.columns[icol]
            if col.startswith(self.col_wl_prefix):
                val_ref = self.df_main.iloc[0].at[col]
                if not started and not np.isnan(val_ref):
                    started = True
                if started and not np.isnan(val_ref):
                    self.col_wl.append(icol)
                    wls = col[len(self.col_wl_prefix):len(col)]
                    wl = float(wls.replace('_', '.'))
                    self.wavelengths.append(wl)

    def get_complete_spectra_frommain(self, irow):
        return self.df_main.iloc[irow, self.col_wl]

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

        index_col = len(self.df_sub.columns)
        n = len(self.df_sub.index)
        dist = [0] * n
        time = [0] * n
        speed = [0] * n
        for i in range(1, n):
            lat1 = self.df_sub.iloc[i - 1].at[self.col_lat]
            lon1 = self.df_sub.iloc[i - 1].at[self.col_lon]
            lat2 = self.df_sub.iloc[i].at[self.col_lat]
            lon2 = self.df_sub.iloc[i].at[self.col_lon]
            point1 = (lat1, lon1)
            point2 = (lat2, lon2)
            dist[i] = hs.haversine(point1, point2, hs.Unit.METERS)
            time1 = dt.strptime(self.df_sub.iloc[i - 1].at[self.col_time], '%Y-%m-%d %H:%M:%S')
            time2 = dt.strptime(self.df_sub.iloc[i].at[self.col_time], '%Y-%m-%d %H:%M:%S')
            time[i] = (time2 - time1).total_seconds() / 60
            speed[i] = hs.haversine(point1, point2, hs.Unit.NAUTICAL_MILES) / (time[i] / 60)

        self.df_sub.insert(index_col, "Dist_metres", dist)
        self.df_sub.insert(index_col + 1, 'Time_minutes', time)
        self.df_sub.insert(index_col + 2, "Speed_knots", speed)

        return True

    def get_n_df_sub(self):
        n = -1
        if self.df_sub is not None:
            n = len(self.df_sub.index)
        return n

    def get_index_at_subdb(self, irow):
        index = -1
        if self.df_sub is None:
            return index
        if 0 <= irow < self.get_n_df_sub():
            idhere = self.df_sub.iloc[irow].at[self.col_id]
            indices = self.df_sub.index[self.df_sub[self.col_id] == idhere].tolist()
            if len(indices) == 1:
                return indices[0]

    def get_lat_lon_at_subdb(self, irow):
        latp = -1
        lonp = -1
        if self.df_sub is None:
            return latp, lonp
        if 0 <= irow < self.get_n_df_sub():
            latp = self.df_sub.iloc[irow].at[self.col_lat]
            lonp = self.df_sub.iloc[irow].at[self.col_lon]
        return latp, lonp

    #format: 'dt': datetime;
    def get_time_at_subdb(self, irow, format):
        if format is None:
            format = 'STR'
        timep = None
        if self.df_sub is None:
            return timep
        if 0 <= irow < self.get_n_df_sub():
            timep = self.df_sub.iloc[irow].at[self.col_time]
            if format=='DT':
                return dt.strptime(timep,'%Y-%m-%d %H:%M:%S')
        return timep



    def get_dist_timedif_speed_at_subdb(self, irow):
        dist = None
        timed = None
        speed = None
        if self.df_sub is None:
            return dist, timed, speed
        if 0 <= irow < self.get_n_df_sub():
            dist = self.df_sub.iloc[irow]['Dist_metres']
            timed = self.df_sub.iloc[irow]['Time_minutes']
            speed = self.df_sub.iloc[irow]['Speed_knots']
        return dist, timed, speed

    def get_lat_lon_from_subdb(self):
        lat_list = []
        lon_list = []
        if self.df_sub is None:
            return lat_list, lon_list
        for index, row in self.df_sub.iterrows():
            # print(row[self.col_time], row[self.col_lat], row[self.col_lon])
            lat_list.append(row[self.col_lat])
            lon_list.append(row[self.col_lon])
        return lat_list, lon_list

    def get_spectra_at_subdb(self, irow):
        spectra = None
        if len(self.col_wl) == 0:
            self.extract_wl_colnames()
        if self.df_sub is None:
            return spectra
        if 0 <= irow < self.get_n_df_sub():
            spectra = self.df_sub.iloc[irow, self.col_wl]
        return spectra

    def get_n_bands(self):
        if len(self.col_wl) == 0:
            self.extract_wl_colnames()
        return len(self.col_wl)

