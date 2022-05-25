from netCDF4 import Dataset
from datetime import datetime as dt
from datetime import timedelta
from base.flag_class import FLAG_LOIS

class NCInsituFile():

    def __init__(self, ncpath):
        self.nc = Dataset(ncpath)
        self.valid_dates = {}

        ##parameters to be implemented using a configuration file
        self.time_variable = 'TIME'
        self.time_ref = dt(1950, 1, 1)
        self.time_ref_units = 'days'

        self.lat_variable = 'LATITUDE'
        self.lon_variable = 'LONGITUDE'

        self.variables = ['CPHL']
        self.flag_variable = 'CPHL_QC'
        self.flag_valid_list = ['good_data']

    def get_valid_dates(self, valid_hours, start_date, end_date):
        if valid_hours is None:
            valid_hours = [0, 24]
        time = self.nc.variables[self.time_variable]
        lat = self.nc.variables[self.lat_variable]
        lon = self.nc.variables[self.lon_variable]
        if self.flag_variable is not None:
            flag = self.nc.variables[self.flag_variable]
            flagclass = FLAG_LOIS(flag.flag_values, flag.flag_meanings)

        for idx in range(len(time)):
            time_here = None
            if self.time_ref_units == 'days':
                time_here = self.time_ref + timedelta(days=float(time[idx]))
            if time_here is None:
                print(f'[WARNING] Time is not valid. Skipping...')
                continue
            if time_here < start_date or time_here > end_date:
                    continue

            flag_valid = True
            if self.flag_variable is not None:
                flag_value = flag[idx]
                flag_valid = flagclass.is_any_flag_valid(self.flag_valid_list, flag_value)

            if (valid_hours[0] <= time_here.hour <= valid_hours[1]) and flag_valid:
                date_here = time_here.strftime('%Y-%m-%d')
                hour_here = time_here.strftime('%H:%M:%S')
                if not date_here in self.valid_dates.keys():
                    self.valid_dates[date_here] = {
                        hour_here: {
                            'lat': float(lat[idx]),
                            'lon': float(lon[idx])
                        }
                    }
                else:
                    self.valid_dates[date_here][hour_here] = {
                        'lat': float(lat[idx]),
                        'lon': float(lon[idx])
                    }
                for var in self.variables:
                    self.valid_dates[date_here][hour_here][var] = float(self.nc.variables[var][idx])
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
