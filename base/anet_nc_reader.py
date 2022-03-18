from netCDF4 import Dataset
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
from base.AERONET_Constants import AERONETConstants
from matplotlib import pyplot as plt


class AERONETReader:
    def __init__(self, file_path):
        self.ac = AERONETConstants()
        self.dataset = Dataset(file_path)
        self.ntimes = len(self.dataset.dimensions[self.ac.dim_time])
        self.nwl = len(self.dataset.dimensions[self.ac.dim_wl])

        # Parameters to define the data
        self.valid_wl = None
        self.row_ini = 0
        self.row_fin = self.ntimes - 1
        self.time_list = None
        self.date_ini = None
        self.date_fin = None

        # Variable to take spectral data
        self.param_wl = None
        self.data_wl = None

    # Defining valid wl indices for a dataset between row_ini and row_fin (inclusive)
    # Indices are only defined if all the rows have the same wl indices
    def prepare_data(self, row_ini, row_fin):
        self.row_ini = row_ini
        self.row_fin = row_fin
        if row_ini == -1 and row_fin == -1:
            return False
        ref_data = self.dataset['Lw'][row_ini:row_fin + 1]
        nvalues = (row_fin - row_ini) + 1
        self.valid_wl = np.zeros(self.nwl, dtype=bool)
        # print(type(ref_data), ref_data.shape)
        for i in range(self.nwl):
            nvalid = nvalues - np.ma.count_masked(ref_data[:, i])
            if nvalid == nvalues:
                self.valid_wl[i] = True
            if 0 < nvalid < nvalues:
                print('WARNING: different wl were detected')
                return False
        return True

    # Get start and end row for a given date defined as: yyyy-mm-dd
    def prepare_data_fordate(self, datestr):
        rini, rfin = self.get_indices_date(datestr)
        return self.prepare_data(rini, rfin)

    # Get data for a specific wl variable
    def extract_spectral_data(self, var, onlyvalid):
        self.param_wl = var
        self.data_wl = self.dataset[var][self.row_ini:self.row_fin + 1]
        if onlyvalid:
            self.data_wl = self.data_wl[:, self.valid_wl]
        return self.data_wl

    def extract_rrs(self, onlyvalid):
        Lwn_fQ = self.extract_spectral_data('Lwn_f_Q', onlyvalid)
        f0 = self.extract_spectral_data('F0', onlyvalid)
        self.data_wl = np.divide(Lwn_fQ, f0)
        self.param_wl = 'Rrs'
        return self.data_wl

    def extract_time_list(self):
        self.time_list = []
        self.date_fin = datetime(1970, 1, 1)
        self.date_ini = datetime.now()
        for r in range(self.row_ini, self.row_fin + 1):
            dthere = self.get_datetime(r)
            self.time_list.append(dthere)
            if dthere < self.date_ini:
                self.date_ini = dthere
            if dthere > self.date_fin:
                self.date_fin = dthere
        return self.time_list

    def out_spectral_data(self):
        nominal_wl = self.dataset['Nominal_Wavelenghts'][self.valid_wl]
        if self.time_list is None:
            self.extract_time_list()
        ndata = (self.row_fin - self.row_ini) + 1
        nwl = len(nominal_wl)
        first_line = 'Wavelenght(nm)'
        for wl in nominal_wl:
            first_line = first_line + ';' + self.param_wl + '_' + str(wl)
        print(first_line)
        for i in range(ndata):
            line = self.time_list[i].strftime('%d-%m-%Y %H:%M:%S')
            for j in range(nwl):
                line = line + ';' + f'{self.data_wl[i][j]:.6f}'
            print(line)

    def plot_spectra(self, hfig):
        print('Plot spectra...')
        nominal_wl = self.dataset['Nominal_Wavelenghts'][self.valid_wl]
        if self.time_list is None:
            self.extract_time_list()
        ndata = (self.row_fin - self.row_ini) + 1
        nwl = len(nominal_wl)
        legend = []
        for i in range(ndata):
            legend.append(self.time_list[i].strftime('%d-%m-%Y %H:%M:%S'))
        row_names = []
        for w in nominal_wl:
            wn = f'{w}'
            row_names.append(wn)

        ydata = self.data_wl
        df = pd.DataFrame(np.transpose(ydata), columns=legend, index=row_names)
        title = f"Aeronet {self.time_list[0].strftime('%d-%m-%Y')}"
        if hfig is None:
            hfig = plt.figure()
        df.plot(lw=2, marker='.', markersize=10)
        rindex = list(range(nwl))
        plt.xticks(rindex, row_names, rotation=45, fontsize=12)
        plt.xlabel('Wavelength(nm)', fontsize=14)
        plt.ylabel('R$_{rs}$[1/sr]', fontsize=14)
        plt.title(title, fontsize=16)
        plt.gcf().subplots_adjust(bottom=0.18)
        plt.gcf().subplots_adjust(left=0.20)
        plt.gcf().canvas.draw()

        return hfig

    # Get start and end row for a given date defined as: yyyy-mm-dd
    def get_indices_date(self, datestr):
        dateini = datetime.strptime(datestr, '%Y-%m-%d').replace(hour=0, minute=0, second=0)
        datefin = dateini + timedelta(hours=24)
        dateinis = (dateini - self.ac.DATEREF).total_seconds()
        datefins = (datefin - self.ac.DATEREF).total_seconds()
        timevar = self.dataset['Time']
        row_ini = -1
        row_fin = -1
        for i in range(self.ntimes):
            if dateinis <= timevar[i] <= datefins:
                if row_ini == -1:
                    row_ini = i
                row_fin = i

        return row_ini, row_fin

    def get_datetime(self, row):
        timevalue = int(self.dataset['Time'][row])
        dt = self.ac.DATEREF + timedelta(seconds=timevalue)
        return dt

    def get_date(self, row):
        timevalue = int(self.dataset['Time'][row])
        dt = self.ac.DATEREF + timedelta(seconds=timevalue)
        return dt.date()

    # dateini and datefin: None or date with format dd-mm-yyyy
    def get_available_dates(self, dateinis, datefins):
        self.time_list = []
        dtprev = datetime.now().date()
        if dateinis is None:
            dateini = datetime(2000, 1, 1).date()
        else:
            dateini = datetime.strptime(dateinis, '%Y-%m-%d').date()
        if datefins is None:
            datefin = datetime.now().date()
        else:
            datefin = datetime.strptime(datefins, '%Y-%m-%d').date()
        for r in range(self.row_ini, self.row_fin + 1):
            dt = self.get_date(r)
            if dt != dtprev and dateini <= dt <= datefin:
                self.time_list.append(dt)
            dtprev = dt
        return self.time_list

    def get_time_ini_fin(self):
        if self.date_ini is None or self.date_fin is None:
            self.extract_time_list()
        return self.date_ini, self.date_fin
