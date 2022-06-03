import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from base.anet_nc_file import ANETNCFile
from base.AERONET_Constants import AERONETConstants
from thuillier import focomputation as foc

class ANETFile:
    # If file_path is not none, dataframe is read from csv file. If not, dataframe is passed directly
    def __init__(self, file_path, completedf,check_variables):
        if not file_path is None:
            self.completedf = pd.read_csv(file_path, sep=',', skiprows=6)
        else:
            self.completedf = completedf
        self.VALID_FILE = self.check_timevar_names()
        self.THULIER_METHOD = AERONETConstants().ThuillierMethodDefault

        print('CHECKING DATES...')
        self.idate = self.completedf.columns.get_loc('Date(dd-mm-yyyy)')
        self.itime = self.completedf.columns.get_loc('Time(hh:mm:ss)')
        self.format_date = '%d-%m-%Y'
        if ':' in self.completedf.iat[0, self.idate]:
            self.format_date = '%d:%m:%Y'
        self.format_time = '%H:%M:%S'
        self.format_dt = f'{self.format_date} {self.format_time}'

        self.abs_start_date = self.get_date(0)
        self.abs_end_date = self.get_date(-1)
        self.ntimes = len(self.completedf.index)
        print('START DATE: ',self.abs_start_date, 'END DATE: ',self.abs_end_date)

        if check_variables:
            print('CHECKING VARIABLES AND WAVELENGHTS...')
            self.ws_var_names, self.nominal_ws, self.nws, valid = self.extract_ws_var_names()
            if not valid:
                self.VALID_FILE = False

            # print(self.nws)
            # print(self.nominal_ws)

            if 'Data_Quality_Level' in self.completedf.columns:
                self.quality_level = self.completedf.at[0, "Data_Quality_Level"]

            self.s_exact_ws = -1
            self.e_exact_ws = -1
            for ic in range(len(self.completedf.columns)):
                c = self.completedf.columns[ic]
                if 'Exact_Wavelengths' in c:
                    if self.s_exact_ws == -1:
                        self.s_exact_ws = ic
                    self.e_exact_ws = ic
            nexact = (self.e_exact_ws - self.s_exact_ws) + 1
            if nexact != self.nws:
                print('WARNING: Number of exacts wavelenghs differs from the number of nominal wavelengths')


            self.nwsvalid, self.valid_ws, check_valid = self.check_valid_ws()
            # print(self.nwsvalid)
            # print(self.valid_ws)
            # for i in range(self.nws):
            #     print(self.nominal_ws[i],'->',self.valid_ws[i])
            if not check_valid:
                self.VALID_FILE = False
            if self.nwsvalid < self.nws:
                self.nominal_ws = self.nominal_ws[np.where(self.valid_ws)]
            else:
                self.valid_ws = None  ##All the nominal wl have data

            print('CHECKING INFO VARIABLES...')
            self.info_var_names = ['Total_Ozone(Du)', 'Total_NO2(DU)', 'Total_Precipitable_Water(cm)', 'Chlorophyll-a',
                                   'Wind_Speed(m/s)', 'Pressure(hPa)', 'Number_of_Wavelengths']
            check_info_var = self.check_info_var_names()
            if not check_info_var:
                self.VALID_FILE = False

        print('DONE')
        # time_array = self.get_time_as_float_array()

    def extract_ws_var_names(self):
        ws_var_names = {}
        nominal_ws = []
        first_var = None
        ref_ws_start = None
        for index in range(len(self.completedf.columns)):
            c = self.completedf.columns[index]
            if '[' in c:
                str_ws = c[c.find('['):c.find(']') + 1]
                str_var = c[0:c.find('[')]
                if first_var is None:
                    first_var = str_var
                if ref_ws_start is None and str_ws != '[Empty]':
                    ref_ws_start = str_ws
                if str_ws == ref_ws_start:
                    ws_var_names[str_var] = {
                        'index_ini': index,
                        'index_fin': index
                    }
                else:
                    ws_var_names[str_var]['index_fin'] = index

                if str_var == first_var:
                    fill_value = -999
                    if str_ws == '[Empty]':
                        nominal_ws.append(fill_value)
                    else:
                        nominal_ws.append(int(str_ws[1:str_ws.find('nm')]))
            # else:
            #     print(c)

        nws = len(nominal_ws)

        valid = True
        for var in ws_var_names:
            nvar = (ws_var_names[var]['index_fin'] - ws_var_names[var]['index_ini']) + 1
            if nvar != nws:
                print(['WARNING: Number of wavelenght is not equall for all bands'])
                valid = False
        nominal_ws = np.array(nominal_ws)
        return ws_var_names, nominal_ws, nws, valid

    def check_valid_ws(self):
        first_var = list(self.ws_var_names.keys())[0]
        valid_ws = np.zeros((self.nws,), dtype=bool)
        check_valid = True
        for var in self.ws_var_names:
            array = self.get_ws_variable(self.ws_var_names[var]['index_ini'], self.ws_var_names[var]['index_fin'])
            for i in range(self.nws):
                nvalid = np.count_nonzero(array[:, i] != -999)
                if nvalid > 0:
                    valid_ws[i] = True
                if nvalid > 0 and valid_ws[i] == False and var != first_var:
                    print('Warning: valid wavelenght differs from different variables')
                    check_valid = False

        nwsvalid = np.count_nonzero(valid_ws)

        return nwsvalid, valid_ws, check_valid

    ##FUNCTIONS MANAGING DATE_TIME
    def check_timevar_names(self):
        valid_time = True
        time_ref_cols = ['Date(dd-mm-yyyy)', 'Time(hh:mm:ss)', 'Day_of_Year', 'Day_of_Year(Fraction)',
                         'Last_Date_Processed']
        for t in time_ref_cols:
            if not t in self.completedf.columns:
                valid_time = False

        if valid_time:
            return True
        else:
            return False

    def get_date_time(self, irow):
        dts = f'{self.completedf.iat[irow, self.idate]} {self.completedf.iat[irow, self.itime]}'
        dt = datetime.strptime(dts, self.format_dt)
        return dt

    def get_date(self, irow):
        return datetime.strptime(self.completedf.iat[irow, self.idate], self.format_date)

    # Time stamps as seconds from 1-1-1970
    def get_time_as_float_array(self):
        time_array = np.arange(self.ntimes, dtype=float)
        dtref = datetime(1970, 1, 1, 0, 0, 0).replace(microsecond=0)
        for irow in range(self.ntimes):
            time_array[irow] = (self.get_date_time(irow) - dtref).total_seconds()
        return time_array

    def get_nws_as_float_array(self):
        nws_array = np.array(self.nominal_ws, dtype=float)
        nws_array = np.nan_to_num(nws_array, nan=-999)
        return nws_array

    def check_info_var_names(self):
        valid_extra = True
        for v in self.info_var_names:
            if not v in self.completedf.columns:
                valid_extra = False
        return valid_extra

    ##Variables retrieval
    def get_ws_variable(self, iini, ifin):
        ifin = ifin + 1  # last index is not included
        array = np.zeros((self.ntimes, self.nws))
        for index, row in self.completedf.iterrows():
            array[index, :] = row[iini:ifin]
        array = np.nan_to_num(array, nan=-999)
        return array

    def get_ws_variable_valid(self, iini, ifin):
        ifin = ifin + 1  # last index is not included
        array = np.zeros((self.ntimes, self.nwsvalid))
        for index, row in self.completedf.iterrows():
            allvalues = row[iini:ifin].array
            array[index, :] = allvalues[np.where(self.valid_ws)]
        array = np.nan_to_num(array, nan=-999)
        return array

    def get_simple_variable(self, varname):
        array = np.zeros((self.ntimes,))
        array[:] = self.completedf[varname].array
        array = np.nan_to_num(array, nan=-999)
        return array

    def get_fo_variable(self,exact_wl_array):
        array = np.zeros((self.ntimes, self.nwsvalid),dtype=float)
        if exact_wl_array is None:
            if self.valid_ws is None:
                exact_wl_array = self.get_ws_variable(self.s_exact_ws, self.e_exact_ws)
            else:
                exact_wl_array = self.get_ws_variable_valid(self.s_exact_ws, self.e_exact_ws)
            exact_wl_array[np.where(exact_wl_array>0)] = exact_wl_array[np.where(exact_wl_array>0)] * 1000
        for irow in range(self.ntimes):
            array[irow,:] = foc.get_F0_array(exact_wl_array[irow,:],self.THULIER_METHOD)

        return array


    def create_aeorent_ncfile(self, file_out):
        ncfile = ANETNCFile(file_out)
        nws = self.nws
        if self.valid_ws is not None:
            nws = self.nwsvalid
        ncfile.start_file(self.ntimes, nws)
        ncfile.add_time_variable(self.get_time_as_float_array())
        ncfile.add_nominalws_variable(self.get_nws_as_float_array())
        for var in self.ws_var_names:
            print('Reading variable: ', var)
            if self.valid_ws is None:
                array = self.get_ws_variable(self.ws_var_names[var]['index_ini'], self.ws_var_names[var]['index_fin'])
            else:
                array = self.get_ws_variable_valid(self.ws_var_names[var]['index_ini'],
                                                   self.ws_var_names[var]['index_fin'])
            if '/' in var:
                var = var.replace('/', '_')
            ncfile.add_2D_variable(var, array)

        print('Reading variable: Exact_Wavelenghts')
        if self.valid_ws is None:
            ewl_array = self.get_ws_variable(self.s_exact_ws, self.e_exact_ws)
        else:
            ewl_array = self.get_ws_variable_valid(self.s_exact_ws, self.e_exact_ws)
        ewl_array[np.where(ewl_array>0)] = ewl_array[np.where(ewl_array>0)]*1000
        ncfile.add_2D_variable('Exact_Wavelengths', ewl_array)
        print('Getting variable: F0')
        fo_array = self.get_fo_variable(ewl_array)
        ncfile.add_2D_variable('F0', fo_array)

        print('Getting simple variables..')
        for var in self.info_var_names:
            array = self.get_simple_variable(var)
            if '/' in var:
                var = var.replace('/', '_')
            ncfile.add_1D_variable(var, array)

        ncfile.close_file()
        print('DONE')

    def get_rec_df_using_dates(self, dateini, datefin):
        row_ini = -1
        row_fin = -1
        for irow in range(self.ntimes):
            dthere = self.get_date(irow)
            if dateini <= dthere <= datefin:
                if row_ini == -1:
                    row_ini = irow
                row_fin = irow
        dfrec = None
        if 0 <= row_ini <= row_fin:
            dfrec = self.completedf.iloc[row_ini:row_fin, :]
        return dfrec


    def get_complete_df(self):
        return self.completedf

    def check_and_append_df(self, afile_append):
        df_combined = None
        if isinstance(afile_append, ANETFile):
            if len(self.completedf.columns.intersection(afile_append.completedf.columns)) == self.completedf.shape[1]:
                dfl_append = afile_append.get_rec_df_using_dates(self.abs_end_date + timedelta(hours=24),
                                                                 datetime.now())
                df_combined = pd.concat([self.completedf, dfl_append],ignore_index=True)

        return df_combined
