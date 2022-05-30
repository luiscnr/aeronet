import argparse
import datetime
import os
import tarfile

import numpy as np
import numpy.ma as ma
import pandas as pd
import requests
import subprocess

parser = argparse.ArgumentParser(
    description="Generation of NC Aeronet Files using CSV files downloaded from the Aeronet web site")
parser.add_argument("-d", "--debug", help="Debugging mode.", action="store_true")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument('-i', "--inputpath", help="Input (download) directory")
parser.add_argument('-o', "--outputpath", help="Output directory")
parser.add_argument('-t', "--thuillier", help="Thulier method", choices=['interp'])
args = parser.parse_args()

from base.anet_file import ANETFile

def only_test_two():
    file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/CHLA_DATA/Baltic_CHLA_Valid.csv'
    from insitu.csv_insitu_file import CSVInsituFile
    ipv = CSVInsituFile(file)
    return True

def main():
    b = only_test_two()
    if b:
        return
    print('STARTED...')  # Press Ctrl+F8 to toggle the breakpoint
    dir_base = args.inputpath
    dir_output = args.outputpath

    if dir_base is None or dir_output is None:
        print('Input and output path are required...')

    # #dir_base = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/prueba'
    # # dir_output = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES'

    # print('Downloading level 2')
    # url = 'https://aeronet.gsfc.nasa.gov/data_push/V3/LWN/LWN_Level20_All_Points_V3.tar.gz'
    # r = requests.get(url, allow_redirects=True)
    # file_out = os.path.join(dir_base, 'LWN_Level20_All_Points_V3.tar.gz')
    # open(file_out, 'wb').write(r.content)
    # print('Uncompressing level 2')
    # file_tar = tarfile.open(file_out)
    # file_tar.extractall(dir_base)
    #
    # print('Downloading level 1.5')
    # url = 'https://aeronet.gsfc.nasa.gov/data_push/V3/LWN/LWN_Level15_All_Points_V3.tar.gz'
    # r = requests.get(url, allow_redirects=True)
    # file_out = os.path.join(dir_base, 'LWN_Level15_All_Points_V3.tar.gz')
    # open(file_out, 'wb').write(r.content)
    # print('Uncompressing level 1.5')
    # file_tar = tarfile.open(file_out)
    # file_tar.extractall(dir_base)

    dir_level15 = os.path.join(dir_base, 'LWN', 'LWN15', 'ALL_POINTS')
    dir_level20 = os.path.join(dir_base, 'LWN', 'LWN20', 'ALL_POINTS')
    files_level20 = os.listdir(dir_level20)
    for f in files_level20:
        f20 = os.path.join(dir_level20, f)
        f15 = os.path.join(dir_level15, f.replace('lev20', 'lev15'))
        site = f[f.find('_') + 1:f.find('.')]
        site = site[site.find('_') + 1:len(site)]

        if site == 'Gustav_Dalen_Tower':
            print('DOING SITE:', site, '-----------------------------------------')
            afilel20 = ANETFile(f20, None, False)
            afilel15 = ANETFile(f15, None, False)
            dfcombined = afilel20.check_and_append_df(afilel15)
            aeronet_combined = ANETFile(None, dfcombined, True)
            file_out = os.path.join(dir_output, f.replace('lev20', 'lev20_15') + '.nc')
            aeronet_combined.create_aeorent_ncfile(file_out)


def only_test():
    print(['TEST'])
    path_folder = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/PANTHYR'
    path_out = os.path.join(path_folder, 'VALID_SPECTRA')
    # path_file = 'AAOT_20190923_025406_20210416_110051_AZI_225_data.csv'
    # from panthyr.panthyr_file import Panthyr_File
    from panthyr.panthyr_file_list import Panthyr_List

    # pfile = Panthyr_File(path_file)
    # pfile.add_th_validation_criteria(None,500,0,'lt')
    # print(pfile.check_validation_criteria())

    # plist.add_time_validation_criteria(None,0,300)

    # plist.create_list_from_folder(path_folder,[135, 225])
    # for l in plist.list_pfiles:
    #     if plist.list_pfiles[l]['valid']:
    #         print(plist.list_pfiles[l]['time'].strftime('%H:%M'))
    # plist.get_dfvalid_spectra('')

    # plist.create_list_from_folder_dates(path_folder, datetime.datetime(2021, 4, 1), datetime.datetime(2021, 12, 21),
    #                                     [225])
    # last_day = [0,31,28,31,30,31,30,31,31,30,31,30,31]
    # for month in range(4,5):
    #     print('Month',month)
    #     plist = Panthyr_List()
    #     plist.add_th_validation_criteria(None, 500, 0, 'lt')
    #     path_outliers = os.path.join(path_folder, 'Outliers_225.csv')
    #     plist.add_outliers_df(None, path_outliers)
    #     start_date = datetime.datetime(2021, month, 1)
    #     end_date = datetime.datetime(2021,month,last_day[month])
    #     plist.create_list_from_folder_dates(path_folder,start_date, end_date,[225])
    #     print('Nvalidos: ', plist.nfilesvalid)
    #     df = plist.get_dfvalid_spectra(None)
    #     start_date_str = start_date.strftime('%Y%m%d')
    #     end_date_str = end_date.strftime('%Y%m%d')
    #     file_out = os.path.join(path_out,f'ValidSpectra_225_{start_date_str}_{end_date_str}.csv')
    #     df.to_csv(file_out,sep=';')

    # CONCATENAMOS SPECTRA DE PANTHYR
    # import numpy as np
    # wce = '_270'
    # dffin = None
    # for fname in os.listdir(path_out):
    #     if fname.find(wce)>=0:
    #         print(fname)
    #         dfhere = pd.read_csv(os.path.join(path_out,fname),sep=';')
    #         nrows = len(dfhere.index)
    #         valid = [False]*nrows
    #         for index,row in dfhere.iterrows():
    #             try:
    #                 date_here = datetime.datetime.strptime(row['TIME'], '%Y-%m-%d %H:%M:%S')
    #             except:
    #                 print('ERRROR: ', row['TIME'])
    #             date_here_h = date_here.replace(minute=0)
    #             dif = (date_here-date_here_h).total_seconds()/60
    #             #print(date_here,date_here_h,dif)
    #             if dif<5:
    #                 valid[index] = True
    #
    #         dfhere_limited = dfhere[np.array(valid)==True][:]
    #         if dffin is None:
    #             dffin = dfhere_limited
    #         else:
    #             dffin = pd.concat([dffin,dfhere_limited],ignore_index=True)
    # dffin.to_csv(os.path.join(path_out,f'Valid_spectra{wce}.csv'),sep=';')

    # ASOCIAMOS PANTHYR WITH HYPERNETS
    import numpy as np
    file_panthyr = os.path.join(path_out, 'Valid_spectra_270.csv')
    file_out = os.path.join(path_out, 'Validation_Hypstar_Panthir_270.csv')
    dfpanthyr = pd.read_csv(file_panthyr, sep=';')
    wlvalues = list(dfpanthyr.columns[5:])
    wllist = []
    iwl_start = -1
    iwl_end = -1
    for iwl in range(0, len(wlvalues)):
        if float(wlvalues[iwl]) >= 400 and iwl_start == -1:
            iwl_start = iwl
        if iwl_start >= 0:
            wllist.append(float(wlvalues[iwl]))
        if iwl_end == -1:
            if float(wlvalues[iwl]) == 800:
                iwl_end = iwl
                break
            elif float(wlvalues[iwl]) > 800:
                iwl_end = iwl
                break

    iwl_start = iwl_start + 5
    iwl_end = iwl_end + 6
    wllist = np.array(wllist)
    df_fin = None
    idx = 0
    for i in range(len(dfpanthyr.index)):
        date_here = datetime.datetime.strptime(dfpanthyr.iloc[i].at['TIME'], '%Y-%m-%d %H:%M:%S')
        print(i,date_here)
        rrs_panthyr_values = np.array(dfpanthyr.iloc[i, iwl_start:iwl_end])
        rrs_hysptar_values, hypstar_time = get_nearest_hypernets_spectra(date_here, wllist)
        if rrs_hysptar_values is not None:
            df_here = pd.DataFrame(columns=['ID','PanthyrTime','HypstarTime','Wavelength','PanthyrRRS','HypstarRRS'],index=range(len(wllist)))
            indices = np.linspace(idx,idx+len(wllist),num = len(wllist),endpoint=False)
            #print(indices)
            idx = idx + len(wllist)
            df_here.loc[:, 'ID'] = indices
            df_here.loc[:, 'PanthyrTime'] = date_here.strftime('%Y-%m-%d %H:%M')
            df_here.loc[:, 'HypstarTime'] = hypstar_time.strftime('%Y-%m-%d %H:%M')
            df_here.loc[:,'Wavelength'] = wllist
            df_here.loc[:,'PanthyrRRS'] = rrs_panthyr_values
            df_here.loc[:,'HypstarRRS'] = rrs_hysptar_values
            #print(rrs_hysptar_values, len(rrs_hysptar_values))
            if df_fin is None:
                df_fin = df_here
            else:
                df_fin = pd.concat([df_fin,df_here])
    df_fin.to_csv(file_out,sep=';')
    return True


def get_nearest_hypernets_spectra(date_here, wllist):
    from netCDF4 import Dataset
    path_hypernets = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/HYPSTAR'

    path_day = os.path.join(path_hypernets, date_here.strftime('%Y'), date_here.strftime('%m'),
                            date_here.strftime('%d'))
    if not os.path.exists(path_day):
        return None, None
    wce = 'L2A'
    hyp_rrs_fin = None
    hyp_time_fin = None
    for f in os.listdir(path_day):

        if f.find(wce) >= 0 and f.endswith('.nc'):
            hyp_rrs_fin = np.zeros(len(wllist))
            date_hypstar_str = f.split('_')[5]
            date_hypstar = datetime.datetime.strptime(date_hypstar_str, '%Y%m%dT%H%M')
            time_dif_min = abs((date_hypstar - date_here).total_seconds()) / 60
            if time_dif_min < 5:
                file_hypstar = os.path.join(path_day, f)
                nchyp = Dataset(file_hypstar)
                hyp_wl = np.array(nchyp.variables['wavelength'])
                hyp_rhow_vec = [x for x, in nchyp.variables['reflectance'][:]]
                hyp_rrs = ma.array(hyp_rhow_vec) / np.pi
                hyp_valid = True
                for ival in range(len(hyp_rrs)):
                    val = hyp_rrs[ival]
                    wlhere = hyp_wl[ival]
                    if ma.is_masked(val):
                        hyp_valid = False
                        break
                    if val<0 and wlhere<500:
                        hyp_valid = False
                        break
                    if val>0.01:
                        hyp_valid = False
                        break
                if hyp_valid:
                    print('VALIDO')
                    for idx in range(len(wllist)):
                        wlref = wllist[idx]
                        inear = np.argmin(np.abs(wlref - hyp_wl))
                        hyp_rrs_fin[idx] = hyp_rrs[inear]
                    hyp_time_fin = date_hypstar
                    return hyp_rrs_fin,hyp_time_fin
                else:
                    hyp_rrs_fin = None

            else:
                hyp_rrs_fin = None

    return hyp_rrs_fin,hyp_time_fin

    # fout = os.path.join(path_folder,'Outliers_135.csv')
    # plist.save_outliers_asfile(1.5,fout)

    # folder = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/MDBs/MDB_S3A_OLCI_WFR_STANDARD_L2_HYPERNETS_VEIT_all'
    # for f in os.listdir(folder):
    #     print(f)

    # from skie.skie_csv import SKIE_CSV
    # path_skie = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/SKIE/Rflex_datadump_2019_2021_filterQin.csv'
    # skie_file = SKIE_CSV(path_skie)
    # skie_file.extract_wl_colnames()
    # print(skie_file.col_wl)

    # path_home = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/HYPSTAR/'7
    #
    # cmd = 'scp -P 9022 hypstar@enhydra.naturalsciences.be:/waterhypernet/HYPSTAR/Processed/VEIT/'
    #
    # for m  in range(1,4):
    #     for d in range(1,32):
    #         if m==3 and d>20:
    #             continue
    #         try:
    #             fecha = datetime.datetime(2022,m,d)
    #         except:
    #             continue
    #         yearstr = fecha.strftime(('%Y'))
    #         messtr = fecha.strftime('%m')
    #         diastr = fecha.strftime('%d')
    #         path_year = os.path.join(path_home,yearstr)
    #         if not os.path.exists(path_year):
    #             os.mkdir(path_year)
    #         path_month = os.path.join(path_year, messtr)
    #         if not os.path.exists(path_month):
    #             os.mkdir(path_month)
    #         path_day = os.path.join(path_month, diastr)
    #         if not os.path.exists(path_day):
    #             os.mkdir(path_day)
    #
    #         cmdfin = f'{cmd}{yearstr}/{messtr}/{diastr}/*L2* {path_day}'
    #
    #         print(cmdfin)
    #         prog = subprocess.Popen(cmdfin, shell=True, stderr=subprocess.PIPE)
    #         out, err = prog.communicate()
    #         if err:
    #             print(err)

    # wl = np.array([450.25, 480.5, 510.66, 620.145])
    # wlout = foc.get_F0_array(wl,'interp')
    # print(wlout,type(wlout))

    # aeronet_path = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS'
    # aeronet_file = os.path.join(aeronet_path, '02020101_20211120_Venise.LWN_lev20')

    # aeronet_filel2 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS/20020101_20211120_Venise.LWN_lev20'
    # aeronet_filel15 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN15/ALL_POINTS/20020101_20211127_Venise.LWN_lev15'
    # afilel2 = ANETFile(aeronet_filel2,None,False)
    # afilel15 = ANETFile(aeronet_filel15,None,False)
    # dfcombined = afilel2.check_and_append_df(afilel15)
    # aeronet_combined = ANETFile(None,dfcombined,True)
    # file_out = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # aeronet_combined.create_aeorent_ncfile(file_out)

    # file_nc = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # areader = AERONETReader(file_nc)
    # areader.prepare_data_fordate('2020-08-09')
    # #areader.extract_spectral_data('Lwn_f_Q')
    # areader.extract_rrs()
    # areader.out_spectral_data()
    # #areader.plot_spectra(None)

    # from base.anet_nc_reader import AERONETReader
    # file_nc = '/mnt/d/LUIS/OCTAC_WORK/BALTIC/20020101_20220129_Gustav_Dalen_Tower.LWN_lev20_15.nc'
    # areader = AERONETReader(file_nc)
    # date_list = areader.get_available_dates('01-04-2016', None)
    # for d in date_list:
    #     print(d)

    # wl = np.array([450.25, 480.5, 510.66, 620.145])
    # wlout = foc.get_F0_array(wl,'interp')
    # print(wlout,type(wlout))

    # aeronet_path = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS'
    # aeronet_file = os.path.join(aeronet_path, '02020101_20211120_Venise.LWN_lev20')

    # aeronet_filel2 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS/20020101_20211120_Venise.LWN_lev20'
    # aeronet_filel15 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN15/ALL_POINTS/20020101_20211127_Venise.LWN_lev15'
    # afilel2 = ANETFile(aeronet_filel2,None,False)
    # afilel15 = ANETFile(aeronet_filel15,None,False)
    # dfcombined = afilel2.check_and_append_df(afilel15)
    # aeronet_combined = ANETFile(None,dfcombined,True)
    # file_out = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # aeronet_combined.create_aeorent_ncfile(file_out)

    # file_nc = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # areader = AERONETReader(file_nc)
    # areader.prepare_data_fordate('2020-08-09')
    # #areader.extract_spectral_data('Lwn_f_Q')
    # areader.extract_rrs()
    # areader.out_spectral_data()
    # #areader.plot_spectra(None)

    # from base.anet_nc_reader import AERONETReader
    # file_nc = '/mnt/d/LUIS/OCTAC_WORK/BALTIC/20020101_20220129_Gustav_Dalen_Tower.LWN_lev20_15.nc'
    # areader = AERONETReader(file_nc)
    # date_list = areader.get_available_dates('01-04-2016', None)
    # for d in date_list:
    #     print(d)
    return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
