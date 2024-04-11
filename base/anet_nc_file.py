import datetime

from netCDF4 import Dataset

class ANETNCFile:

    def __init__(self, file_path):
        self.outf = file_path
        self.dimension_names = ['TimeIndex','Central_wavelenghts']
        self.nc = None
        self.variables_names = {
            'Solar_Zenith_Angle': {
                'short_name': 'SZA',
                'long_name': 'Solar Zenith Angle',
                'units': 'degrees'
            },
            'Solar_Azimuth_Angle': {
                'short_name': 'SAA',
                'long_name': 'Solar Azimuth Angle',
                'units': 'degrees'
            },
            'Lt_mean': {
                'short_name': 'Lt mean',
                'long_name': 'Lt mean',
                'units': 'degrees'
            },


        }

    def start_file(self,ntimes,nws):
        self.nc =Dataset(self.outf, 'w', format='NETCDF4')
        self.nc.createDimension(self.dimension_names[0],ntimes)
        self.nc.createDimension(self.dimension_names[1],nws)

    def add_time_variable(self,array):
        time = self.nc.createVariable('Time', 'f8', ('TimeIndex',), fill_value=-999, zlib=True, complevel=6)
        time.units = "Seconds since 1970-1-1"
        time[:] = array

    def add_nominalws_variable(self,array):
        nominalws = self.nc.createVariable('Nominal_Wavelenghts','f4',('Central_wavelenghts',),fill_value=-999, zlib=True, complevel=6)
        nominalws.units ="nm"
        nominalws[:] = array

    def add_2D_variable(self,var_name,array):
        var = self.nc.createVariable(var_name,'f4',('TimeIndex','Central_wavelenghts'),fill_value=-999, zlib=True, complevel=6)
        ##Units and long name
        var[:] = array

    def add_1D_variable(self,var_name,array):
        var = self.nc.createVariable(var_name,'f4',('TimeIndex',),fill_value=-999, zlib=True, complevel=6)
        ##Units and long name
        var[:] = array

    def close_file(self):
        self.nc.close()




