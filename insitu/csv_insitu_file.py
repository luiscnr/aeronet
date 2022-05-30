import pandas as pd
import numpy as np
from datetime import datetime as dt
import configparser


class CSVInsituFile():
    def __init__(self, csv_path):
        self.dforig = pd.read_csv(csv_path, sep=';')
        self.valid_dates = {}

        # paramaters to be implemented using a configuration file
        # self.date_variable = None #if date_variable and date_format are None, date/time are in the same variable
        # self.date_format = None
        # self.time_variable = 'date'
        # self.time_format = '%Y-%m-%dT%H:%M'
        # self.lat_variable = 'latitude'
        # self.lon_variable = 'longitude'
        # self.invalid_value = None
        # self.variables = ['chl']

        # self.date_variable = 'Date'  # if date_variable and date_format are None, date/time are in the same variable
        # self.date_format = '%d/%m/%Y'
        # self.time_variable = 'Time'
        # self.time_format = '%H:%M:%S'
        # self.lat_variable = 'Lat'
        # self.lon_variable = 'Lon'
        # self.invalid_value = -9999
        # self.variables = ['CHLA']

        self.date_variable = 'DATE'  # if date_variable and date_format are None, date/time are in the same variable
        self.date_format = '%d/%m/%Y'
        self.time_variable = 'HOUR'
        self.time_format = '%H:%M:%S'
        self.lat_variable = 'LATITUDE'
        self.lon_variable = 'LONGITUDE'
        self.invalid_value = -9999
        self.variables = ['CHLA']
        self.flag_variables = ['SOURCE']
        self.flag_info = self.start_flag_variables()

    def start_flag_variables(self):
        flag_info = {}
        if self.flag_variables is not None:
            for flag_var in self.flag_variables:
                flag_array = np.array(self.dforig.loc[:, flag_var])
                flag_meanings = list(np.unique(flag_array))
                flag_values = list(range(1, len(flag_meanings) + 1))
                flag_masks = str(flag_values[0])
                flag_meanings_str = flag_meanings[0]
                for idx in range(len(flag_meanings)):
                    flag_masks = flag_masks + ' , ' + str(flag_values[idx])
                    flag_meanings_str = flag_meanings_str + ' ' + flag_meanings[idx]
                flag_info = {
                    flag_var: {
                        'flag_meanings': flag_meanings,
                        'flag_values': flag_values,
                        'flag_masks': flag_masks,
                        'flag_meanings_str': flag_meanings_str
                    }
                }
        return flag_info

    def get_flag_value(self, flag_var, flag_meaning):
        val = -1
        flag_meanings = self.flag_info[flag_var]['flag_meanings']
        if flag_meaning in flag_meanings:
            idx = flag_meanings.index(flag_meaning)
            if 0 <= idx < len(flag_meanings):
                flag_values = self.flag_info[flag_var]['flag_values']
                val = flag_values[idx]
        return val

    def set_params_fromconfig_file(self, file_config):
        options = configparser.ConfigParser()
        options.read(file_config)
        if not options.has_section('CSV'):
            return
        if options.has_option('CSV', 'date_variable'):
            self.date_variable = options['CSV']['date_variable'].strip()
            if self.date_variable.lower() == 'none':
                self.date_variable = None

        if options.has_option('CSV', 'date_format'):
            self.date_format = self.get_datetime_format(options['CSV']['date_format'].strip())

        if options.has_option('CSV', 'time_variable'):
            self.time_variable = options['CSV']['time_variable'].strip()

        if options.has_option('CSV', 'time_format'):
            self.time_format = self.get_datetime_format(options['CSV']['time_format'].strip())

        if options.has_option('CSV', 'lat_variable'):
            self.lat_variable = options['CSV']['lat_variable'].strip()

        if options.has_option('CSV', 'lon_variable'):
            self.lon_variable = options['CSV']['lon_variable'].strip()

        if options.has_option('CSV', 'invalid_value'):
            self.invalid_value = float(options['CSV']['invalid_value'].strip())

        if options.has_option('CSV', 'variables'):
            varnames = options['CSV']['variables'].split()
            self.variables = []
            for var in varnames:
                self.variables.append(var.strip())

    def get_datetime_format(self, str):
        if str.lower() == 'none':
            return None
        str = str.replace('YYYY', '%Y')
        str = str.replace('mm', '%m')
        str = str.replace('dd', '%d')
        str = str.replace('HH', '%H')
        str = str.replace('MM', '%M')
        str = str.replace('SS', '%S')
        return str

    def get_valid_dates(self, valid_hours, start_date, end_date):
        # print(self.lat_variable,self.lon_variable)
        if valid_hours is None:
            valid_hours = [0, 24]
        datearray = None
        if self.date_variable is not None:
            datearray = np.array(self.dforig.loc[:, self.date_variable])
        timearray = np.array(self.dforig.loc[:, self.time_variable])
        latarray = np.array(self.dforig.loc[:, self.lat_variable])
        lonarray = np.array(self.dforig.loc[:, self.lon_variable])

        # if self.flag_variable is not None:
        #     flag = self.nc.variables[self.flag_variable]
        #     flagclass = FLAG_LOIS(flag.flag_values, flag.flag_meanings)

        for idx in range(len(timearray)):

            time_here = None
            if datearray is None:
                time_here = dt.strptime(timearray[idx].strip(), self.time_format)
            else:
                format = f'{self.date_format}T{self.time_format}'
                time_here_str = f'{datearray[idx].strip()}T{timearray[idx].strip()}'
                time_here = dt.strptime(time_here_str, format)

            if time_here is None:
                print(f'[WARNING] Time is not valid. Skipping...')
                continue

            if time_here < start_date or time_here > end_date:
                continue

            flag_valid = True
            if self.invalid_value is not None:
                flag_valid = False
                for var in self.variables:
                    vararray = self.dforig.loc[:, var]
                    if vararray[idx] != self.invalid_value:
                        flag_valid = True
                        break

            if (valid_hours[0] <= time_here.hour < valid_hours[1]) and flag_valid:
                date_here = time_here.strftime('%Y-%m-%d')
                hour_here = time_here.strftime('%H:%M:%S')
                if not date_here in self.valid_dates.keys():
                    self.valid_dates[date_here] = {
                        hour_here: {
                            'lat': float(latarray[idx]),
                            'lon': float(lonarray[idx])
                        }
                    }
                else:
                    self.valid_dates[date_here][hour_here] = {
                        'lat': float(latarray[idx]),
                        'lon': float(lonarray[idx])
                    }
                for var in self.variables:
                    vararray = self.dforig.loc[:, var]
                    self.valid_dates[date_here][hour_here][var] = float(vararray[idx])
                for var in self.flag_variables:
                    meaning = self.dforig.loc[idx,var]
                    self.valid_dates[date_here][hour_here][var] = int(self.get_flag_value(var,meaning))
        return self.valid_dates

    def compute_geo_limits(self):
        for d in self.valid_dates:
            lat_min = 90
            lat_max = -90
            lon_min = 180
            lon_max = -180
            for h in self.valid_dates[d]:
                if self.valid_dates[d][h]['lat'] < lat_min:
                    lat_min = self.valid_dates[d][h]['lat']
                if self.valid_dates[d][h]['lat'] > lat_max:
                    lat_max = self.valid_dates[d][h]['lat']
                if self.valid_dates[d][h]['lon'] < lon_min:
                    lon_min = self.valid_dates[d][h]['lon']
                if self.valid_dates[d][h]['lon'] > lon_max:
                    lon_max = self.valid_dates[d][h]['lon']
            self.valid_dates[d]['lat_min'] = lat_min
            self.valid_dates[d]['lat_max'] = lat_max
            self.valid_dates[d]['lon_min'] = lon_min
            self.valid_dates[d]['lon_max'] = lon_max

        return self.valid_dates
