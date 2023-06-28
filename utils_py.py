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
                             'removefiles', 'copyfile', 'copys3folders', 'comparison_bal_multi_olci'])
parser.add_argument('-i', "--input", help="Input", required=True)
parser.add_argument('-o', "--output", help="Output", required=True)
parser.add_argument('-wce', "--wce", help="Wild Card Expression")
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

def do_comparison_bal_multi_olci():
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np


    #input grid multi
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

    #input grid olci
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

    # file_grid = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/Grid.csv'
    # file_olci = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/OLCI/O2016117-chl-bal-fr.nc'
    # file_multi = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/MULTI/C2016117-chl-bal-hr.nc'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI//Comparison_chla_2016117.csv'
    # make_comparison_impl(file_grid,file_multi,file_olci,file_out,'CHL','CHL')

    from datetime import datetime as dt
    dir_olci = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    dir_multi = '/store3/OC/CCI_v2017/daily_v202207'
    dir_out = '/store/COP2-OC-TAC/BAL_Evolutions/COMPARISON_MULTI_OLCI/CHLA'
    file_grid = '/store/COP2-OC-TAC/BAL_Evolutions/COMPARISON_MULTI_OLCI/Grid.csv'
    start_date = dt(2016,4,26)
    end_date = dt(2022,12,31)
    date_here = start_date
    while date_here<=end_date:
        date_here_str = date_here.strftime('%Y%m%d')
        year = date_here.strftime('%Y')
        jday = date_here.strftime('%j')
        dir_olci = os.path.join(dir_olci,year,jday)
        dir_multi = os.path.join(dir_multi,year,jday)
        if os.path.exists(dir_olci) and os.path.exists(dir_multi):
            file_olci =os.path.join(dir_olci,f'O{date_here_str}-chl-bal-fr.nc')
            file_multi = os.path.join(dir_multi,f'C{date_here_str}-chl-bal-hr.nc')
            if os.path.exists(file_multi) and os.path.exists(file_olci):
                print(f'[INFO] Making date: {date_here_str}')
                file_out = os.path.join(dir_out,f'Comparison_chla_{date_here_str}.csv')
                make_comparison_impl(file_grid,file_multi,file_olci,file_out,'CHL','CHL')





def make_comparison_impl(file_grid,file_multi,file_olci,file_out,variable_multi,variable_olci):
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np
    import numpy.ma as ma

    grid = pd.read_csv(file_grid, sep=';')
    dataset_multi = Dataset(file_multi)
    dataset_olci = Dataset(file_olci)
    array_multi = np.array(dataset_multi.variables[variable_multi])
    array_olci = np.array(dataset_olci.variables[variable_olci])
    for index,row in grid.iterrows():
        ymulti = int(row['YMulti'])
        xmulti = int(row['XMulti'])
        yolci = int(row['YOlci'])
        xolci = int(row['XOlci'])
        valid = 0
        val_multi = array_multi[0,ymulti,xmulti]
        array_here = array_olci[0,yolci-1:yolci+2,xolci-1:xolci+2]
        array_here_good = array_here[array_here!=-999]
        val_olci = -999
        if len(array_here_good)==9:
            val_olci = np.mean(array_here[array_here!=-999])
        if val_olci!=-999 and val_multi!=-999:
            valid = 1
        grid.loc[index,'MultiVal'] = val_multi
        grid.loc[index,'OlciVal'] = val_olci
        grid.loc[index,'Valid'] = valid

    dataset_olci.close()
    dataset_multi.close()
    grid_valid = grid[grid['Valid']==1]
    grid_valid.to_csv(file_out,sep=';')

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
