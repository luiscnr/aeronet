

from netCDF4 import Dataset

class RESTONCFile:

    def __init__(self, file_path):
        self.outf = file_path
        self.dimension_names = ['TimeIndex','Central_wavelenghts']
        self.nc = None

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

    def add_global_atributtes(self,start_date,end_date,instrument):
        # global atributes
        self.nc.latitude = 43.12278
        self.nc.longitude = 12.13306
        self.nc.site = 'Trasimeno Lake'
        self.nc.siteid = 'TAIT'
        self.nc.start_date = start_date.strftime('%Y-%m-%d %H:%M')
        self.nc.end_date = end_date.strftime('%Y-%m-%d %H:%M')
        self.nc.instrument = instrument

    def close_file(self):
        self.nc.close()