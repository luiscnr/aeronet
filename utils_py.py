import argparse
import os
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd

parser = argparse.ArgumentParser(
    description="General utils")

parser.add_argument('-m', "--mode", help="Mode", choices=['concatdf', 'removerep', 'checkextractsdir','dhusget'])
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

def get_dhusget_paths():
    finput = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Gustav_Dalen_Tower/OnlyWFR_S3B.csv'
    dateminabs = dt(2018,5,29)
    datemaxabs = dt(2018,12,31)
    path_output = '/dst04-data1/OC/OLCI/sources_baseline_2.23/'
    timecrontab = dt.now().replace(hour=14,minute=25,microsecond=0)
    with open(finput,'r') as fd:
        for line in fd:
            fpath = line.strip().split('/')[-1]
            lpath = fpath.split('_')
            datehere = dt.strptime(lpath[7],'%Y%m%dT%H%M%S')
            if datehere<dateminabs or datehere>datemaxabs:
                continue
            datemin = datehere-timedelta(hours=1)
            datemax = datehere+timedelta(hours=1)
            dateminstr = f'{datemin.isoformat()}Z'
            datemaxstr = f'{datemax.isoformat()}Z'
            yearstr = datehere.strftime('%Y')
            jday = datehere.strftime('%j')
            path_output_here = os.path.join(path_output,yearstr,jday)
            dateprename = datehere.strftime('%Y%m%d')
            prename = f'S3B_OL_1_EFR____{dateprename}'
            nfiles = 0
            for name in os.listdir(path_output_here):
                if name.startswith(prename):
                    file_check = os.path.join(path_output_here,file_check)
                    if os.path.getsize(file_check)>0:
                        nfiles = nfiles +1
            if nfiles==2:
                print('-------------------------------------------------------------------------------------CONTINUE')
                continue

            crontabstr = f'{timecrontab.minute} {timecrontab.hour} {timecrontab.day} {timecrontab.month} * '
            timecrontab = timecrontab + timedelta(minutes=35)
            cmdstart = f'/home/Luis.Gonzalezvilas/downloadolci/dhusget.sh -u luisgv -p GalaCoda2022! -m Sentinel-3 -i OLCI -S '
            cmdend = f' -T OL_1_EFR___ -c 9.25,53.25:30.25,65.85  -F \'timeliness:"Non Time Critical" AND filename:S3B*\' -o product -O '
            cmd = f'{crontabstr}{cmdstart}{dateminstr} -E {datemaxstr}{cmdend}{path_output_here}'
            cmd = f'{cmdstart}{dateminstr} -E {datemaxstr}{cmdend}{path_output_here}'
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
    dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Gustav_Dalen_Tower'

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
    dir_extracts = os.path.join(dir_base, 'WFR', 'extracts')
    for name in os.listdir(dir_extracts):
        if not name.startswith(platform):
            continue
        fpath = os.path.join(dir_extracts, name)
        date_here = get_sat_time_from_fname(fpath)
        idx = (date_here - date_ini).days
        if df.loc[idx, 'Total'] == 1:
            only_wfr.append(fpath)
    flist = os.path.join(dir_base,f'OnlyWFR_{platform}.csv')
    fop = open(flist,'w')
    for line in only_wfr:
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