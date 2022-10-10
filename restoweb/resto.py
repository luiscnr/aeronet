from urllib.parse import urlencode
from datetime import datetime as dt
import io

import numpy as np
import requests
from restoweb.resto_nc_file import RESTONCFile


class RESTO_WEB():
    def __init__(self, verbose):
        self.verbose = verbose
        self.user = 'cnr_irea'
        self.pwd = 'W1spcloud4cnr_irea'

        self.urlbase = 'https://wispcloud.waterinsight.nl/api/query?'

        self.query_parameters = {
            'SERVICE': 'Data',
            'VERSION': '1.0',
            'REQUEST': 'GetData',
            'TIME': '',
            'INSTRUMENT': 'Wispstation012',
            'INCLUDE': 'measurement.id, measurement.date, instrument.name, level2.reflectance'
        }
        self.start_date  = None
        self.end_date = None
        self.instrument = None
        self.dtref = dt(1970, 1, 1, 0, 0, 0).replace(microsecond=0)
        self.times = []
        self.rrs = []
        self.nominal_wavelenghts = []
        self.nws = -1
        self.ntimes = -1
        self.outstatus = False

    def retrive_data(self, startDate, endDate, instrument):
        self.start_date = startDate
        self.end_date = endDate
        if instrument is None:
            if startDate <= dt(2021, 5, 4):
                instrument = 'Wispstation001'
            else:
                instrument = 'Wispstation012'
        self.instrument = instrument
        if self.verbose:
            print(f'[INFO] Retrieving data from {startDate} to {endDate}')
            print(f'[INFO] Instrument: {instrument}')

        sdate = startDate.strftime('%Y-%m-%dT%H:%M')
        edate = endDate.strftime('%Y-%m-%dT%H:%M')
        string_date = f'{sdate},{edate}'
        self.query_parameters['TIME'] = string_date
        self.query_parameters['INSTRUMENT'] = instrument
        query_data = urlencode(self.query_parameters)
        url = self.urlbase + query_data
        datastring = io.StringIO(requests.get(url, auth=(self.user, self.pwd)).content.decode('utf-8')).read()

        lines = datastring.split('\n')
        nline = 0
        for line in lines:
            if line.startswith('#'):
                continue
            if len(line) == 0:
                continue
            dataline = line.split('\t')
            if nline == 0:
                col_names = dataline
            elif nline == 1:
                units = dataline
            else:
                time = self.get_time_as_seconds_from_1970(dataline[1])
                self.times.append(time)
                farray = []
                sarray = dataline[3].split(',')
                for val in sarray:
                    val = val.replace('[', '')
                    val = val.replace(']', '')
                    farray.append(float(val.strip()))
                self.rrs.append(farray)
                self.nws = len(farray)
            nline = nline + 1

        self.ntimes = len(self.times)
        self.get_nominal_ws_from_unit(units[3])
        if self.nominal_wavelenghts is not None:
            if len(self.nominal_wavelenghts) == self.nws:
                self.outstatus = True
            else:
                print(f'[ERROR] Number of nominal wavelenghts if different from rrs data length')
        else:
            print(f'[ERROR] Nominal wavelenghts are not defined')

        if self.verbose:
            print(f'[INFO] Number of meauserements retrived: {self.ntimes}')


    def save_data_as_ncfile(self, fileout):
        if not self.outstatus:
            print('[ERROR] Data were not correctly retrieved')
        if self.verbose:
            print(f'[INFO] Saving output file: {fileout}')
        rfile = RESTONCFile(fileout)
        rfile.start_file(self.ntimes, self.nws)
        rfile.add_nominalws_variable(self.nominal_wavelenghts)
        rfile.add_time_variable(np.array(self.times, dtype=np.float))
        rrsa = np.array(self.rrs, dtype=np.float)
        rfile.add_2D_variable('RRS',rrsa)
        rfile.add_global_atributtes(self.start_date,self.end_date,self.instrument)
        rfile.close_file()
        if self.verbose:
            print(f'[INFO] Completed')

    def get_nominal_ws_from_unit(self, units):
        self.nominal_wavelenghts = None
        if units.strip().startswith('[1/sr for wavelength [') and units.strip().endswith('in 1nm steps]'):
            unitst = units.split('[')[2].split(']')[0].split('..')
            if len(unitst) == 2:
                try:
                    wlmin = float(unitst[0])
                    wlmax = float(unitst[1])
                    self.nominal_wavelenghts = np.arange(wlmin, wlmax + 1, dtype=np.float)
                except:
                    pass

    def get_time_as_seconds_from_1970(self, dataval):
        datehere = dt.strptime(dataval, '%Y-%m-%d %H:%M:%S.%f')
        sec = float((datehere - self.dtref).total_seconds())
        return sec
