import argparse
import os
import shutil
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
import stat
import subprocess

parser = argparse.ArgumentParser(
    description="General utils")

parser.add_argument('-m', "--mode", help="Mode",
                    choices=['concatdf', 'removerep', 'checkextractsdir', 'dhusget', 'printscp', 'removencotmp',
                             'removefiles', 'copyfile', 'copys3folders', 'comparison_bal_multi_olci',
                             'comparison_multi_olci'])
parser.add_argument('-i', "--input", help="Input", required=True)
parser.add_argument('-o', "--output", help="Output", required=True)
parser.add_argument('-wce', "--wce", help="Wild Card Expression")
parser.add_argument('-r', "--region", help="Region")
parser.add_argument('-sd', "--start_date", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--end_date", help="The End Date - format YYYY-MM-DD ")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


def main():
    print('[INFO] Started')
    if args.mode == 'concatdf':
        input_path = args.input
        output_file = args.output
        wce = '.'
        if args.wce:
            wce = args.wce
        dffin = None
        for f in os.listdir(input_path):

            if f.endswith('.csv') and f.find(wce) > 0:
                if args.verbose:
                    print(f'Input file: {f}')
                input_file = os.path.join(input_path, f)
                dfhere = pd.read_csv(input_file, sep=';')
                if dffin is None:
                    dffin = dfhere
                else:
                    dffin = pd.concat([dffin, dfhere], ignore_index=True)
        dffin.to_csv(output_file, sep=';')
        if args.verbose:
            print(f'Results saved to: {output_file}')

    if args.mode == 'removerep':
        input_path = args.input
        date_var = 'DATE'
        date_format = '%d/%m/%Y'
        time_var = 'HOUR'
        time_format = '%H:%M:%S'
        group_var = 'SOURCE'
        check_var = 'CHLA'

        datetime_format = f'{date_format}T{time_format}'

        dforig = pd.read_csv(input_path, sep=';')

        ndata = len(dforig.index)
        valid = [True] * ndata
        data_dict = None

        for idx in range(ndata):
            group = dforig.iloc[idx].at[group_var]
            datestr = dforig.iloc[idx].at[date_var]
            timestr = dforig.iloc[idx].at[time_var]
            checkvalue = dforig.iloc[idx].at[check_var]
            if data_dict is None:
                data_dict = {
                    group: {
                        datestr: {
                            'time': timestr,
                            'idx': idx,
                            'ref': checkvalue
                        }
                    }
                }
                # print(data_dict)
            elif group not in data_dict.keys():
                data_dict[group] = {
                    datestr: {
                        'time': timestr,
                        'idx': idx,
                        'ref': checkvalue
                    }
                }
            else:
                data_dict[group][datestr] = {
                    'time': timestr,
                    'idx': idx,
                    'ref': checkvalue
                }

        groups = np.unique(dforig.loc[:, group_var])
        nrepeated = 0
        for idx in range(ndata):
            if not valid[idx]:
                continue
            group = dforig.iloc[idx].at[group_var]
            datestr = dforig.iloc[idx].at[date_var]
            timestr = dforig.iloc[idx].at[time_var]
            checkvalueorig = dforig.iloc[idx].at[check_var]
            datetimeherestr = f'{datestr}T{timestr}'
            datetimehere = dt.strptime(datetimeherestr, datetime_format)

            for g in groups:
                if g == group:
                    continue
                if datestr in data_dict[g].keys():
                    timestrother = data_dict[g][datestr]['time']
                    datetimeotherstr = f'{datestr}T{timestrother}'
                    datetimeother = dt.strptime(datetimeotherstr, datetime_format)
                    time_dif = abs((datetimehere - datetimeother).total_seconds())
                    checkvalueother = data_dict[g][datestr]['ref']
                    checkdif = abs(checkvalueorig - checkvalueother)

                    if time_dif < 30 and checkdif < 0.01:
                        if args.verbose:
                            idxother = data_dict[g][datestr]['idx']
                            if args.verbose:
                                print(f'[INFO] ORIG: {group} {datetimehere} {checkvalueorig}')
                                print(f'[INFO] OTHER {g} {datetimeother} {time_dif} {checkvalueother} {checkdif}')
                                print('------------------------------------------------------------')
                            valid[idxother] = False
                            nrepeated = nrepeated + 1

        if args.verbose:
            print(f'[INFO] Repeated records {nrepeated}')
        dfnew = dforig[valid]

        dfnew.to_csv(args.output, sep=';')
        print(f'[INFO] New dataset without repeated records saved to: {args.output}')

    if args.mode == 'checkextractsdir':
        check_extracts_directories()

    if args.mode == 'dhusget':
        get_dhusget_paths()

    if args.mode == 'printscp':
        print_scp()

    if args.mode == 'removencotmp':
        remove_nco()

    if args.mode == 'removefiles':
        remove_files()

    if args.mode == 'copyfile':
        copy_files()

    if args.mode == 'copys3folders':
        copy_s3_folders()

    if args.mode == 'comparison_bal_multi_olci':
        do_comparison_bal_multi_olci()

    if args.mode == 'comparison_multi_olci':
        do_comparison_multi_olci()


def do_comparison_multi_olci():
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np

    region = 'med'
    if args.region:
        region = args.region
    dir_out_base = args.output

    do_grid = False
    if args.input=='GRID':
        do_grid = True

    if do_grid:
        #input grid multi
        file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_COMPARISON_OLCI_MULTI/MULTI/X2022153-chl-bs-hr.nc'
        file_out = os.path.join(dir_out_base,f'GridMulti{region.capitalize()}.csv')
        dataset = Dataset(file_input)
        lat_array = np.array(dataset.variables['lat'])
        lon_array = np.array(dataset.variables['lon'])
        nlat = len(lat_array)
        nlon = len(lon_array)
        index = 1
        f1 = open(file_out,'w')
        line = f'Index;YMulti;XMulti;Latitude;Longitude'
        f1.write(line)
        for y in range(0,nlat,10):
            for x in range(0,nlon,10):
                lat_here = lat_array[y]
                lon_here = lon_array[x]
                line = f'{index};{y};{x};{lat_here};{lon_here}'
                index = index +1
                print(line)
                f1.write('\n')
                f1.write(line)
        f1.close()

        #input grid olci
        file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_COMPARISON_OLCI_MULTI/OLCI/O2022153-chl-med-fr.nc'
        file_grid = os.path.join(dir_out_base,f'GridMulti{region.capitalize()}.csv')
        file_out = os.path.join(dir_out_base,f'GridOlci{region.capitalize()}.csv')

        grid = pd.read_csv(file_grid,sep=';')
        lat_grid = grid['Latitude'].to_numpy()
        lon_grid = grid['Longitude'].to_numpy()
        dataset = Dataset(file_input)
        lat_array = np.array(dataset.variables['lat'])
        lon_array = np.array(dataset.variables['lon'])
        f1 = open(file_out,'w')
        line = f'Index;YOlci;XOlci;Latitude;Longitude'
        f1.write(line)
        for idx in range(len(lat_grid)):
            index = idx +1
            lat_here = lat_grid[idx]
            lon_here = lon_grid[idx]
            y = np.argmin(np.abs(lat_array-lat_here))
            x = np.argmin(np.abs(lon_array-lon_here))
            line = f'{index};{y};{x};{lat_here};{lon_here}'
            print(line)
            f1.write('\n')
            f1.write(line)
        f1.close()

        file_end = os.path.join(dir_out_base, f'Grid{region.capitalize()}.csv')
        dfmulti = pd.read_csv(file_grid,sep=';')
        dfolci = pd.read_csv(file_out,sep=';')
        dfmulti['YOlci'] = dfolci['YOlci']
        dfmulti['XOlci'] = dfolci['XOlci']
        dfmulti['MultiVal'] = -999.0
        dfmulti['OlciVal'] = -999.0
        dfmulti['Valid'] = 0
        return

    # exampling of comparison
    # file_grid = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/Grid.csv'
    # file_olci = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/OLCI/O2016117-chl-bal-fr.nc'
    # file_multi = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/MULTI/C2016117-chl-bal-hr.nc'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI//Comparison_chla_2016117.csv'
    # make_comparison_impl(file_grid,file_multi,file_olci,file_out,'CHL','CHL')

    ##comparison
    print('[INFO] STARTED  COMPARISON...')
    from datetime import datetime as dt
    #dir_olci_orig = '/dst04-data1/OC/OLCI/daily_3.01'
    #dir_multi_orig = '/store3/OC/MULTI/daily_v202311_x'
    dir_olci_orig = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_COMPARISON_OLCI_MULTI/OLCI'
    dir_multi_orig = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_COMPARISON_OLCI_MULTI/MULTI'
    # FOLDERS: CHLA, RRS443, RRS490, RRS510, RRS560, RRS670
    if args.input == 'ALL':
        params = ['CHL','KD490']
    elif args.input == 'RRS':
        params = ['RRS']
        wl_multi = ['443', '490', '510', '555', '670']
        wl_olci = ['442_5', '490', '510', '560','665']
    else:
        param = args.input
        params = [param]


    dir_outs = []
    for param in params:
        dir_out = os.path.join(dir_out_base, f'COMPARISON_{param}')
        if not os.path.exists(dir_out):
            os.mkdir(dir_out)
        dir_outs.append(dir_out)
    file_grid = os.path.join(dir_out_base, f'Grid{region.capitalize()}.csv')
    if args.input == 'RRS':
        col_names_multi = []
        col_names_olci = []
        file_new_grid = os.path.join(dir_out_base,f'Grid{region.capitalize()}_BandShifting.csv')
        dfgrid = pd.read_csv(file_grid,sep=';')

        for wl in wl_multi:
            new_col = f'MultiVal_RRS_{wl}'
            col_names_multi.append(new_col)
            dfgrid[new_col]=-999.0
        for wl in wl_multi: #BAND SHIFTING FROM OLCI TO MULTI
            new_col = f'OlciVal_RRS_{wl}'
            col_names_olci.append(new_col)
            dfgrid[new_col]=-999.0
        dfgrid = dfgrid.drop('MultiVal',axis=1)
        dfgrid = dfgrid.drop('OlciVal',axis=1)
        dfgrid.to_csv(file_new_grid,sep=';')
        file_grid = file_new_grid

    start_date = dt.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.strptime(args.end_date, '%Y-%m-%d')
    date_here = start_date

    while date_here <= end_date:
        for param, dir_out in zip(params, dir_outs):
            param_multi = param
            param_olci = param
            if param_multi == 'RRS443':
                param_olci = 'RRS442_5'
            if param_multi == 'RRS555':
                param_olci = 'RRS560'
            year = date_here.strftime('%Y')
            jday = date_here.strftime('%j')
            dir_olci = os.path.join(dir_olci_orig, year, jday)
            dir_multi = os.path.join(dir_multi_orig, year, jday)
            if os.path.exists(dir_olci) and os.path.exists(dir_multi):
                if param_multi == 'RRS':  ##different RRS applying band shif from OLCI to MULTI

                    files_olci = []
                    files_multi = []
                    do_proc = True
                    for wlm, wlo in zip(wl_multi, wl_olci):
                        file_olci = os.path.join(dir_olci, f'O{year}{jday}-rrs{wlo}-{region}-fr.nc')
                        file_multi = os.path.join(dir_multi, f'X{year}{jday}-rrs{wlm}-{region}-hr.nc')
                        if os.path.exists(file_olci) and os.path.exists(file_multi):
                            files_olci.append(file_olci)
                            files_multi.append(file_multi)
                        else:
                            do_proc = False
                            break
                    if do_proc:
                        file_out = os.path.join(dir_out, f'Comparison_{param}_{year}{jday}.csv')
                        make_comparison_band_shifting_impl(file_grid, files_multi, files_olci, file_out, wl_multi,
                                                           wl_olci,col_names_multi,col_names_olci)
                else:
                    file_olci = os.path.join(dir_olci, f'O{year}{jday}-{param_olci.lower()}-{region}-fr.nc')
                    file_multi = os.path.join(dir_multi, f'X{year}{jday}-{param_multi.lower()}-{region}-hr.nc')
                    if os.path.exists(file_multi) and os.path.exists(file_olci):
                        print(f'[INFO] Making date: {date_here}')
                        file_out = os.path.join(dir_out, f'Comparison_{param}_{year}{jday}.csv')
                        make_comparison_impl(file_grid, file_multi, file_olci, file_out, param_multi, param_olci)
        date_here = date_here + timedelta(hours=240)

    # getting global points
    # val = 'CHLA'
    # dir_comparison = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/{val}'
    # val = val.lower()
    # file_out = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/{val}_points.csv'
    #
    # file_ref = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/rrs510_points.csv'
    # #file_ref = None
    # first_line = f'Date;Index;MultiVal;OlciVal'
    # f1 = open(file_out,'w')
    # f1.write(first_line)
    # nfiles = 0
    # if file_ref is None:
    #     start_date = dt(2016, 5, 1)
    #     end_date = dt(2022,12,31)
    #     date_here = start_date
    #
    #     while date_here <= end_date:
    #         year = date_here.strftime('%Y')
    #         jday = date_here.strftime('%j')
    #         file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
    #         date_here_str = date_here.strftime('%Y-%m-%d')
    #         print(date_here_str)
    #         if os.path.exists(file_c):
    #             nfiles = nfiles +1
    #             points_here = pd.read_csv(file_c,sep=';')
    #             for index,row in points_here.iterrows():
    #                 multi_val = row['MultiVal']
    #                 olci_val = row['OlciVal']
    #                 index_here = row['Index']
    #                 line=f'{date_here_str};{index_here};{multi_val};{olci_val}'
    #                 f1.write('\n')
    #                 f1.write(line)
    #         date_here = date_here + timedelta(hours=240)  # 10 days
    # else:
    #     df_ref = pd.read_csv(file_ref,sep=';')
    #     dates_ref = df_ref['Date']
    #     index_ref = df_ref['Index']
    #     nref = len(df_ref.index)
    #     date_check = dt(2016,5,1)
    #     print(date_check)
    #     year = date_check.strftime('%Y')
    #     jday = date_check.strftime('%j')
    #     date_check_str = date_check.strftime('%Y-%m-%d')
    #     file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
    #     points_check_here = pd.read_csv(file_c, sep=';')
    #     indices_check_here = points_check_here['Index'].to_numpy(dtype=np.int32).tolist()
    #
    #     nnodata = 0
    #     ndata = 0
    #     nfiles = 1
    #     for idx in range(nref):
    #         date_here = dt.strptime(str(dates_ref[idx]),'%Y-%m-%d')
    #         index_here = int(index_ref[idx])
    #         if date_here!=date_check:
    #             print(date_check)
    #             date_check = date_here
    #             year = date_check.strftime('%Y')
    #             jday = date_check.strftime('%j')
    #             date_check_str = date_check.strftime('%Y-%m-%d')
    #             file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
    #             points_check_here = pd.read_csv(file_c, sep=';')
    #             indices_check_here = points_check_here['Index'].to_numpy(dtype=np.int32).tolist()
    #             nfiles = nfiles +1
    #         if index_here in indices_check_here:
    #             idx = indices_check_here.index(index_here)
    #             multi_val = points_check_here.iloc[idx].at['MultiVal']
    #             olci_val = points_check_here.iloc[idx].at['OlciVal']
    #             line = f'{date_check_str};{index_here};{multi_val};{olci_val}'
    #             f1.write('\n')
    #             f1.write(line)
    #             ndata = ndata + 1
    #         else:
    #             nnodata = nnodata +1
    #
    #
    #
    #     print('NFILES: ',nfiles)
    #     print('NDATA: ', ndata)
    #     print('NNODATA: ', nnodata)
    #
    # f1.close()
    # print('NFILES: ',nfiles)


def do_comparison_bal_multi_olci():
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np

    # input grid multi
    # file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/MULTI/C2016117-chl-bal-hr.nc'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/GridMulti.csv'
    # dataset = Dataset(file_input)
    # lat_array = np.array(dataset.variables['lat'])
    # lon_array = np.array(dataset.variables['lon'])
    # nlat = len(lat_array)
    # nlon = len(lon_array)
    # index = 1
    # f1 = open(file_out,'w')
    # line = f'Index;YMulti;XMulti;Latitude;Longitude'
    # f1.write(line)
    # for y in range(0,nlat,10):
    #     for x in range(0,nlon,10):
    #         lat_here = lat_array[y]
    #         lon_here = lon_array[x]
    #         line = f'{index};{y};{x};{lat_here};{lon_here}'
    #         index = index +1
    #         print(line)
    #         f1.write('\n')
    #         f1.write(line)
    # f1.close()

    # input grid olci
    # file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/OLCI/O2016117-chl-bal-fr.nc'
    # file_grid = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/GridMulti.csv'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/GridOlci.csv'
    #
    # grid = pd.read_csv(file_grid,sep=';')
    # lat_grid = grid['Latitude'].to_numpy()
    # lon_grid = grid['Longitude'].to_numpy()
    # dataset = Dataset(file_input)
    # lat_array = np.array(dataset.variables['lat'])
    # lon_array = np.array(dataset.variables['lon'])
    # f1 = open(file_out,'w')
    # line = f'Index;YOlci;XMOlci;Latitude;Longitude'
    # f1.write(line)
    # for idx in range(len(lat_grid)):
    #     index = idx +1
    #     lat_here = lat_grid[idx]
    #     lon_here = lon_grid[idx]
    #     y = np.argmin(np.abs(lat_array-lat_here))
    #     x = np.argmin(np.abs(lon_array-lon_here))
    #     line = f'{index};{y};{x};{lat_here};{lon_here}'
    #     print(line)
    #     f1.write('\n')
    #     f1.write(line)
    # f1.close()

    # exampling of comparison
    # file_grid = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/Grid.csv'
    # file_olci = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/OLCI/O2016117-chl-bal-fr.nc'
    # file_multi = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/MULTI/C2016117-chl-bal-hr.nc'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI//Comparison_chla_2016117.csv'
    # make_comparison_impl(file_grid,file_multi,file_olci,file_out,'CHL','CHL')

    ##comparison chla
    # print('[INFO] STARTED HARDCORED COMPARISON...')
    # from datetime import datetime as dt
    # dir_olci_orig = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    # dir_multi_orig = '/store3/OC/CCI_v2017/daily_v202207'
    # #FOLDERS: CHLA, RRS443, RRS490, RRS510, RRS560, RRS670
    # dir_out = '/store/COP2-OC-TAC/BAL_Evolutions/COMPARISON_MULTI_OLCI/RRS670'
    # file_grid = '/store/COP2-OC-TAC/BAL_Evolutions/COMPARISON_MULTI_OLCI/Grid.csv'
    # start_date = dt(2016,5,1)
    # end_date = dt(2016,5,2)
    # #end_date = dt(2022,12,31)
    # date_here = start_date
    # while date_here<=end_date:
    #     #date_here_str = date_here.strftime('%Y%m%d')
    #     year = date_here.strftime('%Y')
    #     jday = date_here.strftime('%j')
    #     dir_olci = os.path.join(dir_olci_orig,year,jday)
    #     dir_multi = os.path.join(dir_multi_orig,year,jday)
    #     #print(date_here_str)
    #     if os.path.exists(dir_olci) and os.path.exists(dir_multi):
    #
    #         #chla,rrs442_5,rrs490,rrs510,rrs560,rrs665
    #         file_olci =os.path.join(dir_olci,f'O{year}{jday}-rrs665-bal-fr.nc')
    #         # chla,rrs443,rrs490,rrs510,rrs560,rrs665
    #         file_multi = os.path.join(dir_multi,f'C{year}{jday}-rrs665-bal-hr.nc')
    #
    #         if os.path.exists(file_multi) and os.path.exists(file_olci):
    #             print(f'[INFO] Making date: {date_here}')
    #             file_out = os.path.join(dir_out,f'Comparison_RRS670_{year}{jday}.csv')
    #             make_comparison_impl(file_grid,file_multi,file_olci,file_out,'RRS665','RRS665')
    #     date_here = date_here + timedelta(hours=240)

    # getting global points
    val = 'CHLA'
    dir_comparison = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/{val}'
    val = val.lower()
    file_out = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/{val}_points.csv'

    file_ref = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/rrs510_points.csv'
    # file_ref = None
    first_line = f'Date;Index;MultiVal;OlciVal'
    f1 = open(file_out, 'w')
    f1.write(first_line)
    nfiles = 0
    if file_ref is None:
        start_date = dt(2016, 5, 1)
        end_date = dt(2022, 12, 31)
        date_here = start_date

        while date_here <= end_date:
            year = date_here.strftime('%Y')
            jday = date_here.strftime('%j')
            file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
            date_here_str = date_here.strftime('%Y-%m-%d')
            print(date_here_str)
            if os.path.exists(file_c):
                nfiles = nfiles + 1
                points_here = pd.read_csv(file_c, sep=';')
                for index, row in points_here.iterrows():
                    multi_val = row['MultiVal']
                    olci_val = row['OlciVal']
                    index_here = row['Index']
                    line = f'{date_here_str};{index_here};{multi_val};{olci_val}'
                    f1.write('\n')
                    f1.write(line)
            date_here = date_here + timedelta(hours=240)  # 10 days
    else:
        df_ref = pd.read_csv(file_ref, sep=';')
        dates_ref = df_ref['Date']
        index_ref = df_ref['Index']
        nref = len(df_ref.index)
        date_check = dt(2016, 5, 1)
        print(date_check)
        year = date_check.strftime('%Y')
        jday = date_check.strftime('%j')
        date_check_str = date_check.strftime('%Y-%m-%d')
        file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
        points_check_here = pd.read_csv(file_c, sep=';')
        indices_check_here = points_check_here['Index'].to_numpy(dtype=np.int32).tolist()

        nnodata = 0
        ndata = 0
        nfiles = 1
        for idx in range(nref):
            date_here = dt.strptime(str(dates_ref[idx]), '%Y-%m-%d')
            index_here = int(index_ref[idx])
            if date_here != date_check:
                print(date_check)
                date_check = date_here
                year = date_check.strftime('%Y')
                jday = date_check.strftime('%j')
                date_check_str = date_check.strftime('%Y-%m-%d')
                file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
                points_check_here = pd.read_csv(file_c, sep=';')
                indices_check_here = points_check_here['Index'].to_numpy(dtype=np.int32).tolist()
                nfiles = nfiles + 1
            if index_here in indices_check_here:
                idx = indices_check_here.index(index_here)
                multi_val = points_check_here.iloc[idx].at['MultiVal']
                olci_val = points_check_here.iloc[idx].at['OlciVal']
                line = f'{date_check_str};{index_here};{multi_val};{olci_val}'
                f1.write('\n')
                f1.write(line)
                ndata = ndata + 1
            else:
                nnodata = nnodata + 1

        print('NFILES: ', nfiles)
        print('NDATA: ', ndata)
        print('NNODATA: ', nnodata)

    f1.close()
    print('NFILES: ', nfiles)


def make_comparison_impl(file_grid, file_multi, file_olci, file_out, variable_multi, variable_olci):
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np
    import numpy.ma as ma

    grid = pd.read_csv(file_grid, sep=';')
    dataset_multi = Dataset(file_multi)
    dataset_olci = Dataset(file_olci)
    array_multi = np.array(dataset_multi.variables[variable_multi])
    array_olci = np.array(dataset_olci.variables[variable_olci])
    for index, row in grid.iterrows():
        ymulti = int(row['YMulti'])
        xmulti = int(row['XMulti'])
        yolci = int(row['YOlci'])
        xolci = int(row['XOlci'])
        valid = 0
        val_multi = array_multi[0, ymulti, xmulti]
        array_here = array_olci[0, yolci - 1:yolci + 2, xolci - 1:xolci + 2]
        array_here_good = array_here[array_here != -999]
        val_olci = -999
        if len(array_here_good) == 9:
            val_olci = np.mean(array_here[array_here != -999])
        if val_olci != -999 and val_multi != -999:
            valid = 1
        grid.loc[index, 'MultiVal'] = val_multi
        grid.loc[index, 'OlciVal'] = val_olci
        grid.loc[index, 'Valid'] = valid

    dataset_olci.close()
    dataset_multi.close()
    grid_valid = grid[grid['Valid'] == 1]
    grid_valid.to_csv(file_out, sep=';')


def make_comparison_band_shifting_impl(file_grid, files_multi, files_olci, file_out, wl_multi, wl_olci, col_names_multi,col_names_olci):
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np
    from BSC_QAA import bsc_qaa_EUMETSAT as qaa
    import warnings
    warnings.filterwarnings("ignore")
    wl_output = [float(x.replace('_','.')) for x in wl_multi]
    wl_input = [float(x.replace('_', '.')) for x in wl_olci]
    grid = pd.read_csv(file_grid, sep=';')

    num_m = len(files_multi)
    for idx in range(num_m):
        file_multi = files_multi[idx]
        wlm = wl_multi[idx]
        dataset_multi = Dataset(file_multi)
        variable_multi = f'RRS{wlm}'
        array_m = np.array(dataset_multi.variables[variable_multi])
        if idx == 0:
            s = array_m.shape
            array_multi = np.zeros((num_m, s[1], s[2]))
        array_multi[idx, :, :] = array_m[0, :, :]
        dataset_multi.close()

    num_o = len(files_olci)
    for idx in range(num_o):
        file_olci = files_olci[idx]
        wlo = wl_olci[idx]
        dataset_olci = Dataset(file_olci)
        variable_olci = f'RRS{wlo}'
        array_o = np.array(dataset_olci.variables[variable_olci])
        if idx == 0:
            s = array_o.shape
            array_olci = np.zeros((num_o, s[1], s[2]))
        array_olci[idx, :, :] = array_o[0, :, :]
        dataset_olci.close()
    num_olci_good = num_o * 9

    for index, row in grid.iterrows():
        ymulti = int(row['YMulti'])
        xmulti = int(row['XMulti'])
        yolci = int(row['YOlci'])
        xolci = int(row['XOlci'])
        valid = 0
        spectra_multi = array_multi[:, ymulti, xmulti]
        array_here = array_olci[:, yolci - 1:yolci + 2, xolci - 1:xolci + 2]
        array_here_good = array_here[array_here != -999]
        if len(array_here_good)==num_olci_good:
            array_here_good_res = np.reshape(array_here_good,(num_o,9))
            spectra_olci = np.mean(array_here_good_res,axis=1)
            if len(spectra_multi[spectra_multi!=-999])==num_m:
                valid = 1
                spectra_olci = qaa.bsc_qaa(spectra_olci,wl_input,wl_output)
        else:
            spectra_olci = np.array([-999.0]*num_o)

        grid.loc[index, 'Valid'] = valid
        if valid==1:
            grid.loc[index, col_names_multi] = spectra_multi
            grid.loc[index, col_names_olci] = spectra_olci

    grid_valid = grid[grid['Valid'] == 1]
    grid_valid.to_csv(file_out, sep=';')

def copy_files():
    # copy files with the dates indicated in the text file inputpath in output path
    input_dir = '/store3/OC/MULTI/daily_v202012'
    input_fname = 'X$DATE$-chl-med-hr.nc'
    date_format_fname = '%Y%j'
    input_path = args.input
    output_path = args.output
    filedates = open(input_path, 'r')
    for line in filedates:
        datestr = line.strip()
        datep = dt.strptime(datestr, '%Y-%m-%d')
        input_dir_date = os.path.join(input_dir, datep.strftime('%Y'), datep.strftime('%j'))
        input_fname_date = input_fname.replace('$DATE$', datep.strftime(date_format_fname))
        input_file = os.path.join(input_dir_date, input_fname_date)
        output_file = os.path.join(output_path, input_fname_date)
        if os.path.exists(input_file):
            print(f'Copying: {input_file}')
            shutil.copy(input_file, output_file)


def copy_s3_folders():
    input_dir = '/dst04-data1/OC/OLCI/trimmed_sources/'
    # copy s3 folder files indicated in the text file inputpath in outputpath
    input_path = args.input
    output_path = args.output
    filedates = open(input_path, 'r')

    for line in filedates:
        file_name = line.strip().split('/')[-1]
        file_name = file_name[:-21]
        fs = file_name.split('_')
        datep = dt.strptime(fs[7], '%Y%m%dT%H%M%S')
        input_dir_date = os.path.join(input_dir, datep.strftime('%Y'), datep.strftime('%j'))
        dir_name = file_name + '.SEN3'
        input_dir_here = os.path.join(input_dir_date, dir_name)

        if os.path.exists(input_dir_here):
            print(f'Copying input dir: {input_dir_here}')
            output_dir = os.path.join(output_path, dir_name)
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            for f in os.listdir(input_dir_here):
                input_file = os.path.join(input_dir_here, f)
                output_file = os.path.join(output_dir, f)
                shutil.copy(input_file, output_file)
        else:
            print(f'[WARNING] Input dir: {input_dir_here} does not exist. Skiping...')


def remove_files():
    # remove files en output path with the names indicated in the text file inputpath
    input_path = args.input
    output_path = args.output
    fileidx = open(input_path, 'r')
    for line in fileidx:
        name = line.strip()
        foutput = os.path.join(output_path, name)
        if os.path.exists(foutput):
            print(f'Removing: {foutput}')
            os.remove(foutput)
    fileidx.close()


def remove_nco():
    input_path = args.input
    year_ini = 1997
    year_fin = dt.now().year + 1
    for iyear in range(year_ini, year_fin):
        ypath = os.path.join(input_path, str(iyear))
        if not os.path.exists(ypath):
            continue
        for imonth in range(1, 13):
            for iday in range(1, 32):
                try:
                    datehere = dt(iyear, imonth, iday)
                    jday = datehere.strftime('%j')
                    jpath = os.path.join(ypath, jday)
                    if not os.path.exists(jpath):
                        continue
                    # print(f'Checking path: {jpath}')
                    haytmp = False
                    for name in os.listdir(jpath):
                        if name.endswith('ncks.tmp'):
                            haytmp = True
                            break
                    if haytmp:
                        cmd = f'rm -f {jpath}/*.ncks.tmp'
                        print(cmd)
                        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
                        out, err = prog.communicate()
                        if err:
                            print(err)
                except:
                    continue


def print_scp():
    dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/INPUT_SOURCES/WFR_DOWNLOAD/pending'
    dir_output = '/dst04-data1/OC/OLCI/sources_baseline_3.01/bal'
    print('#!/bin/bash')
    for name in os.listdir(dir_base):
        if not name.endswith('.zip'):
            continue
        sat_time = get_sat_time_from_fname(name)
        year_path = os.path.join(dir_output, sat_time.strftime('%Y'))
        path_out = os.path.join(year_path, sat_time.strftime('%j'))
        path_in = os.path.join(dir_base, name)
        cmd = f'scp {path_in} Luis.Gonzalezvilas@artov.ismar.cnr.it@192.168.10.94:{path_out}'
        print(cmd)


def get_dhusget_paths():
    # finput = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Gustav_Dalen_Tower/OnlyWFR_S3B.csv'
    finput = args.input
    dateminabs = dt(2016, 1, 11)
    datemaxabs = dt(2022, 12, 31)
    path_output = '/dst04-data1/OC/OLCI/sources_baseline_2.23/'

    # foutput = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Gustav_Dalen_Tower/doolci.sh'
    foutput = args.output

    lines = ['#! /bin/bash']

    with open(finput, 'r') as fd:
        for line in fd:
            fpath = line.strip().split('/')[-1]
            lpath = fpath.split('_')
            datehere = dt.strptime(lpath[7], '%Y%m%dT%H%M%S')
            if datehere < dateminabs or datehere > datemaxabs:
                continue
            datemin = datehere - timedelta(hours=1)
            datemax = datehere + timedelta(hours=1)
            dateminstr = f'{datemin.isoformat()}Z'
            datemaxstr = f'{datemax.isoformat()}Z'
            yearstr = datehere.strftime('%Y')
            jday = datehere.strftime('%j')
            path_output_here = os.path.join(path_output, yearstr, jday)
            dateprename = datehere.strftime('%Y%m%d')
            prename = f'S3B_OL_1_EFR____{dateprename}'
            nfiles = 0
            for name in os.listdir(path_output_here):
                if name.startswith(prename):
                    file_check = os.path.join(path_output_here, name)
                    if os.path.getsize(file_check) > 0:
                        nfiles = nfiles + 1
            if nfiles == 2:
                continue

            cmdstart = f'/home/Luis.Gonzalezvilas/downloadolci/dhusget.sh -u luisgv -p GalaCoda2022! -m Sentinel-3 -i OLCI -S '
            cmdend = f' -T OL_1_EFR___ -c 9.25,53.25:30.25,65.85  -F \'timeliness:"Non Time Critical" AND filename:S3B*\' -o product -O '
            cmd = f'{cmdstart}{dateminstr} -E {datemaxstr}{cmdend}{path_output_here}'
            lines.append(cmd)

    with open(foutput, 'w') as fp:
        fp.write(lines[0])
        fp.write('\n')
        fp.write('')
        for idx in range(1, len(lines)):
            fp.write('\n')
            fp.write(lines[idx])

    st = os.stat(foutput)
    os.chmod(foutput, st.st_mode | stat.S_IEXEC)

    timecrontab = dt.now() + timedelta(minutes=3)
    crontabstr = f'{timecrontab.minute} {timecrontab.hour} {timecrontab.day} {timecrontab.month} * '
    cmd = f'{crontabstr} {foutput}'
    print('[INFO] CRONTAB:')
    print(cmd)


def check_extracts_directories():
    date_ini = dt(2016, 4, 29)
    date_fin = dt(2021, 12, 31)
    delta = date_fin - date_ini
    ndays = delta.days + 1
    acnames = ['WFR', 'POLYMER', 'C2RCC', 'FUB', 'IDEPIX']
    df = pd.DataFrame(index=list(range(ndays)), columns=['Date', 'WFR', 'POLYMER', 'C2RCC', 'FUB', 'IDEPIX', 'Total'])
    for idx in range(ndays):
        df.loc[idx, 'Date'] = (date_ini + timedelta(days=idx)).strftime('%Y-%m-%d')
        for ac in acnames:
            df.loc[idx, ac] = 0
        df.loc[idx, 'Total'] = 0

    platform = 'S3B'
    # dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Gustav_Dalen_Tower'
    dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Helsinki_Lighthouse'
    # dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Irbe_Lighthouse'

    for ac in acnames:
        dir_extracts = os.path.join(dir_base, ac, 'extracts')
        for name in os.listdir(dir_extracts):
            if not name.startswith(platform):
                continue
            fpath = os.path.join(dir_extracts, name)
            date_here = get_sat_time_from_fname(fpath)
            idx = (date_here - date_ini).days
            df.loc[idx, ac] = df.loc[idx, ac] + 1

    for idx in range(ndays):
        for ac in acnames:
            df.loc[idx, 'Total'] = df.loc[idx, 'Total'] + df.loc[idx, ac]

    df.to_csv(os.path.join(dir_base, f'ExtractsSummaryN_{platform}.csv'), sep=';')

    only_wfr = []
    dates_only_wfr = []
    dir_extracts = os.path.join(dir_base, 'WFR', 'extracts')
    for name in os.listdir(dir_extracts):
        if not name.startswith(platform):
            continue
        fpath = os.path.join(dir_extracts, name)
        date_here = get_sat_time_from_fname(fpath)
        idx = (date_here - date_ini).days
        if df.loc[idx, 'Total'] == 1:
            only_wfr.append(fpath)
            dates_only_wfr.append(date_here.strftime('%Y-%m-%d'))
    flist = os.path.join(dir_base, f'OnlyWFR_{platform}.csv')
    ftrimlist = os.path.join(dir_base, f'TrimDates_{platform}.csv')
    fop = open(flist, 'w')
    for line in only_wfr:
        fop.write(line)
        fop.write('\n')
    fop.close()
    fop = open(ftrimlist, 'w')
    for line in dates_only_wfr:
        fop.write(line)
        fop.write('\n')
    fop.close()
    print('DONE')


def get_sat_time_from_fname(fname):
    val_list = fname.split('_')
    sat_time = None
    for v in val_list:
        try:
            sat_time = dt.strptime(v, '%Y%m%dT%H%M%S')
            break
        except ValueError:
            continue
    return sat_time


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
