import argparse
import datetime
import os
import tarfile
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


def main():
    b = only_test()
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

        if site=='Gustav_Dalen_Tower':
            print('DOING SITE:', site, '-----------------------------------------')
            afilel20 = ANETFile(f20, None, False)
            afilel15 = ANETFile(f15, None, False)
            dfcombined = afilel20.check_and_append_df(afilel15)
            aeronet_combined = ANETFile(None, dfcombined, True)
            file_out = os.path.join(dir_output, f.replace('lev20', 'lev20_15') + '.nc')
            aeronet_combined.create_aeorent_ncfile(file_out)


def only_test():
    print(['TEST'])

    # folder = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/MDBs/MDB_S3A_OLCI_WFR_STANDARD_L2_HYPERNETS_VEIT_all'
    # for f in os.listdir(folder):
    #     print(f)

    from skie.skie_csv import SKIE_CSV
    path_skie = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/SKIE/Rflex_datadump_2019_2021_filterQin.csv'
    skie_file = SKIE_CSV(path_skie)
    skie_file.extract_wl_colnames()
    print(skie_file.col_wl)



    # path_home = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/HYPSTAR/'
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
