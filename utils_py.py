import argparse
import os
import shutil
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
import stat
import subprocess
import warnings

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(
    description="General utils")

parser.add_argument('-m', "--mode", help="Mode",
                    choices=['concatdf', 'removerep', 'checkextractsdir', 'dhusget', 'printscp', 'removencotmp',
                             'removefiles', 'copyfile', 'copys3folders', 'comparison_bal_multi_olci',
                             'comparison_multi_olci', 'extract_csv', 'checksensormask', 'match-ups_from_extracts',
                             'doors_certo_msi_csv', 'aqua_check'])
parser.add_argument('-i', "--input", help="Input", required=True)
parser.add_argument('-o', "--output", help="Output", required=True)
parser.add_argument('-fr', "--file_ref", help="File ref")
parser.add_argument('-wce', "--wce", help="Wild Card Expression")
parser.add_argument('-r', "--region", help="Region")
parser.add_argument('-it', "--interval", help="Interval for comparison (in days)")
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
        # do_comparasion_multi_olci_byday()
        # do_comparison_daily_integrated()

    if args.mode == 'extract_csv':
        do_extract_csv()

    if args.mode == 'checksensormask':
        # do_check_sensor_mask()
        do_test()

    if args.mode == 'match-ups_from_extracts':
        do_match_ups_from_extracts()

    if args.mode == 'doors_certo_msi_csv':
        do_doors_certo_msi_csv()

    if args.mode == 'aqua_check':
        do_aqua_check()


def do_aqua_check():
    if not args.input:
        print(f'[ERROR] --input (-i) is required')
        return
    input_path = args.input
    if not os.path.isdir(input_path):
        print(f'[ERROR] {input_path} is not a valid directory')
        return
    if not args.start_date:
        print(f'[ERROR] --start_date (-sd) is required')
        return
    try:
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
    except:
        print(f'[ERROR] Start date: {args.start_date} is not in the valid format YYYY-mm-dd')
        return

    if args.end_date:
        try:
            end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        except:
            print(f'[ERROR] Start date: {args.end_date} is not in the valid format YYYY-mm-dd')
            return
    else:
        end_date = start_date

    if end_date > start_date:
        print(f'[ERROR] Start date {start_date} shoud be lower or equal to end date {end_date}')
        return

    region = 'bs'
    if args.region:
        region = args.region
    regions = ['bs', 'med']
    if region.lower() not in regions:
        print(f'[ERROR] Region {region} should be in the list: {regions}')
        return

    output_file = args.output
    if not output_file.endswith('.csv'):
        print(f'[ERROR] Output file {output_file} shoud be a csv file')
        return
    if not os.path.isdir(os.path.dirname(output_file)):
        print(f'[EROR] Output path {os.path.dirname(output_file)} is not a valid directory')
        return

    date_here = start_date
    fout = None
    while date_here <= end_date:
        path_date = os.path.join(input_path, date_here.strftime('%Y'), date_here.strftime('%j'))
        name_file = f'X{date_here.strftime("%Y%j")}-chl-{region.lower()}-hr.nc'
        input_file = os.path.join(path_date, name_file)
        res = None
        if os.path.exists(input_file):
            print(f'[INFO] Working with file: {input_file}')
            res = get_info_aqua(input_file)
        else:
            print(f'[WARNING] Input file: {input_file} does not exist. Skipping date...')

        if res is not None:
            if fout is None:
                fout = open(output_file, 'w')
                first_line = ';'.join(list(res.keys()))
                fout.write(first_line)
            line = ';'.join([str(x) for x in list(res.values())])
            fout.write('\n')
            fout.write(line)
        date_here = date_here + timedelta(hours=24)
    if fout is not None:
        fout.close()
    print('[INFO]COMPLETED')


def get_info_aqua(file):
    from netCDF4 import Dataset
    dataset = Dataset(file)
    comment = dataset.variables['SENSORMASK'].comment
    clist = comment[0:comment.find('.')].split(';')
    res = {}
    flag_names = []
    flag_values = []
    for c in clist:
        flag_names.append(c.split('=')[0].strip())
        flag_values.append(int(c.split('=')[1].strip()))
    smask = np.array(dataset.variables['SENSORMASK']).astype(np.int64)
    smask = smask[smask != -999]  ##only valid pixles
    res['ntotal'] = smask.shape[0]
    res['nvalid'] = np.count_nonzero(smask)
    nobs = np.zeros(smask.shape)
    for val in flag_values:
        bitval = np.bitwise_and(smask, val)
        bitval[bitval > 0] = 1
        if val == 2:  ##aqua:
            aqua_array = bitval
        nobs = nobs + bitval

    res['naqua'] = np.sum(aqua_array)
    aqua_combined = aqua_array + nobs
    aqua_combined[aqua_array == 0] = 0

    nobs_degraded = nobs.copy()
    nobs_degraded[aqua_array == 0] = 0
    nobs_degraded[aqua_combined == 2] = 0

    res['naqua_only'] = np.count_nonzero(aqua_combined == 2)
    res['naqua_and1'] = np.count_nonzero(aqua_combined == 3)
    res['naqua_and2'] = np.count_nonzero(aqua_combined == 4)
    res['naqua_and3'] = np.count_nonzero(aqua_combined == 5)
    res['naqua_and4'] = np.count_nonzero(aqua_combined == 6)
    res['ndegraded'] = res['naqua_and1'] + res['naqua_and2'] + res['naqua_and3'] + res['naqua_and4']
    res['sum_obs'] = np.sum(nobs)
    res['sum_obs_degraded_pixels'] = np.sum(nobs_degraded)
    res['sum_obs_aqua_degraded_pixels'] = res['naqua'] - res['naqua_only']
    res['percent_aqua_only'] = (res['naqua_only'] / res['nvalid']) * 100
    res['percent_degraded_pixels'] = (res['ndegraded'] / (res['nvalid']-res['naqua_only'])) * 100
    res['percent_aqua_obs_degraded_pixels'] = (res['sum_obs_aqua_degraded_pixels'] / res[
        'sum_obs_degraded_pixels']) * 100
    dataset.close()

    return res


def do_doors_certo_msi_csv():
    name_in = 'DOOR_insitu_BlackSea_AeronetOC_Galata_Platform_extract_CERTO_MSI_aeronetbulgaria.csv'
    name_out = 'DOOR_insitu_BlackSea_AeronetOC_Galata_Platform_extract_CERTO_MSI.csv'

    input_path = f'/mnt/c/DATA_LUIS/DOORS_WORK/in_situ_extracts/certo_msi/{name_in}'
    output_path = f'/mnt/c/DATA_LUIS/DOORS_WORK/in_situ_extracts/certo_msi/{name_out}'
    input_dir = os.path.dirname(input_path)
    shutil.copy(input_path, output_path)
    import pandas as pd
    df = pd.read_csv(output_path, sep=';')
    used = [0] * len(df.index)
    assigned = [0] * len(df.index)
    nmax = 0
    for name in os.listdir(input_dir):
        file_csv = os.path.join(input_dir, name)
        if file_csv == input_path or file_csv == output_path:
            continue
        if os.path.isdir(file_csv):
            continue
        df_here = pd.read_csv(file_csv, sep=';')
        for index, row in df_here.iterrows():
            if used[index] == 0 and df.loc[index, 'Index'] >= 0:
                print('Used index is: ', index)
                used[index] = 1
                assigned[index] = 1

            if row['Index'] >= 0:
                used[index] = used[index] + 1
                print('Nex index:', index, ' per file: ', name, '--->', used[index])
                if used[index] > nmax:
                    nmax = used[index]

    for ival in range(1, nmax):
        df[f'Extract_{ival}'] = 'NaN'
        df[f'Index_{ival}'] = -1
    print('Nmax: ', nmax)

    for name in os.listdir(input_dir):
        file_csv = os.path.join(input_dir, name)
        if file_csv == input_path or file_csv == output_path:
            continue
        if os.path.isdir(file_csv):
            continue
        print(name)
        df_here = pd.read_csv(file_csv, sep=';')
        for index, row in df_here.iterrows():
            if row['Index'] >= 0:
                if assigned[index] == 0:
                    iname = 'Index'
                    ename = 'Extract'
                else:
                    iname = f'Index_{assigned[index]}'
                    ename = f'Extract_{assigned[index]}'
                df.at[index, iname] = row['Index']
                df.at[index, ename] = row['Extract']

                assigned[index] = assigned[index] + 1

    df.loc[df['Index'] == -1, 'Extract'] = 'NaN'

    df.to_csv(output_path, sep=';')


def do_match_ups_from_extracts():
    print('Math-ups from extracts')

    file_csv = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPS-PFT/results_chla/Chl_per_group_sup_up_11m_chla_extracts.csv'

    dir_extracts = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPS-PFT/extracts_multi_chla'
    file_out = f'{file_csv[:-4]}_3x3.csv'
    # rc_ini = 12
    # rc_fin = 12
    rc_ini = 11
    rc_fin = 13
    from netCDF4 import Dataset
    df = pd.read_csv(file_csv, ';')
    # col_names = df.columns.tolist() + ['RRS_412','RRS_443','RRS_490','RRS_510','RRS_560','RRS_665']
    col_names = df.columns.tolist() + ['Chla']
    first_line = ';'.join(col_names)
    fw = open(file_out, 'w')
    fw.write(first_line)
    for index, row in df.iterrows():
        lrow = [str(x).strip() for x in list(row)]
        line = ';'.join(lrow)

        ##rrs
        # index_extract = row['Index_Extract_RRS']
        # values = [-999] * 6
        # if index_extract>=0:
        #     name_extract = row['Extract_RRS']
        #     extract_file = os.path.join(dir_extracts,f'extract_{name_extract}')
        #     dataset = Dataset(extract_file)
        #     for ivar in range(6):
        #         rrs_data = np.array(dataset.variables['satellite_Rrs'][0, ivar, rc_ini:rc_fin+1,rc_ini:rc_fin+1])
        #         rrs_data_c = rrs_data[rrs_data!=-999.0]
        #         if len(rrs_data_c)>0:
        #             values[ivar] = np.mean(rrs_data_c)
        #         else:
        #             values[ivar] = -999.0
        #     dataset.close()
        # chla
        index_extract = row['Index_Extract_CHLA']
        values = [-999.0]
        if index_extract >= 0:
            name_extract = row['Extract_CHLA']
            extract_file = os.path.join(dir_extracts, f'extract_{name_extract}')
            dataset = Dataset(extract_file)
            chl_data = np.array(dataset.variables['satellite_CHL'][0, rc_ini:rc_fin + 1, rc_ini:rc_fin + 1])
            chl_data_c = chl_data[chl_data != -999.0]
            if len(chl_data_c) > 0:
                values[0] = np.mean(chl_data_c)
            else:
                values[0] = -999.0
            dataset.close()
        for ivar in range(len(values)):
            line = f'{line};{values[ivar]}'
        fw.write('\n')
        fw.write(line)
        print(line)
    fw.close()
    return


def do_check_sensor_mask():
    print('DO CHECK SENSOR MASK....')
    from datetime import timedelta
    from netCDF4 import Dataset
    input_dir = '/store3/OC/MULTI/daily_v202311_x'
    fout = '/store/COP2-OC-TAC/sensor_mask_check.csv'
    # input_dir = '/mnt/c/DATA_LUIS/OCTAC_WORK/CHECK_SENSOR_MASK'
    # fout = '/mnt/c/DATA_LUIS/OCTAC_WORK/CHECK_SENSOR_MASK/sensor_mask_check.csv'

    start_date = dt(1997, 9, 16)
    end_date = dt(2022, 12, 31)
    date_here = start_date
    comment_prev = 'N/A'
    lines = []
    while date_here <= end_date:
        yearstr = date_here.strftime('%Y')
        jjjstr = date_here.strftime('%j')
        input_file = os.path.join(input_dir, yearstr, jjjstr, f'X{yearstr}{jjjstr}-chl-med-hr.nc')
        if os.path.exists(input_file):
            dataset = Dataset(input_file)
            comment = dataset.variables['SENSORMASK'].comment.split('.')[0]
            if comment != comment_prev:
                date_here_str = date_here.strftime('%Y-%m-%d')
                line_here = f'{date_here_str};{comment}'
                lines.append(line_here)
                print(line_here)
                comment_prev = comment
        date_here = date_here + timedelta(hours=24)
    f1 = open(fout, 'w')
    for line in lines:
        f1.write(line)
        f1.write('\n')
    f1.close()


def do_test():
    print('TEST')
    from netCDF4 import Dataset
    file = '/mnt/c/DATA_LUIS/OCTAC_WORK/CHECK_SENSOR_MASK/X2022130-chl-med-hr.nc'
    dataset = Dataset(file, 'r')
    smask = np.array(dataset.variables['SENSORMASK'][:])
    print(smask.min(), smask.max())

    import pandas as pd
    from datetime import datetime as dt
    # print('Step 1')
    # file_grid = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/GridArc.csv'
    # lon_indices = {}
    # df = pd.read_csv(file_grid,sep=';')
    # for idx,row in df.iterrows():
    #     index = str(int(row['Index']))
    #     longitude = row['Longitude']
    #     lon_indices[index] = longitude
    #
    #
    # print('Step 2')
    # file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/ALGORITHMS/rrs_points.csv'
    # file_output = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/ALGORITHMS/jlon.csv'
    # f1 = open(file_output,'w')
    # f1.write('JDay;Longitude')
    # dfi = pd.read_csv(file_input,sep=';')
    # for idx,row in dfi.iterrows():
    #     index = str(int(row['Index']))
    #     date_str = str(row['Date'])
    #     date_here = dt.strptime(date_str,'%Y-%m-%d')
    #     lonvalue = lon_indices[index]
    #     jvalue = date_here.strftime('%j')
    #     line = f'{jvalue};{lonvalue}'
    #     f1.write('\n')
    #     f1.write(line)
    # f1.close()

    # file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/ALGORITHMS/rrs_points.csv'
    # file_output = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/ALGORITHMS/chla_multi.csv'

    # from datetime import datetime as dt
    # from datetime import timedelta
    # import pandas as pd
    # start_date = dt(2016,5,1)
    # end_date = dt(2022,12,31)
    # date_here = start_date
    #
    # dir_base1 = '/store/COP2-OC-TAC/ARC_COMPARISON_MULTI_OLCI/'
    # dir_base2 = '/store/COP2-OC-TAC/ARC_COMPARISON_MULTI_OLCI/PREV_COMPARISON'
    #
    # dir_base1 = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/PREV_COMPARISON'
    # dir_base2 = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI'
    #
    # bands = ['RS443','RRS490','RRS510','RRS560','RRS665']
    # file_out = '/store/COP2-OC-TAC/ARC_COMPARISON_MULTI_OLCI/CheckComparison.csv'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/CheckComparison.csv'
    # f1 = open(file_out,'w')
    # f1.write('Date;RRS443;RRS490;RRS510;RRS560;RRS665')
    #
    #
    # while date_here <= end_date:
    #     date_here_str = date_here.strftime('%Y-%m-%d')
    #     date_yj = date_here.strftime('%Y%j')
    #     line = f'{date_here_str}'
    #     nfiles = 0
    #     for band in bands:
    #         print('----------->',date_here_str,band,)
    #         file1 = os.path.join(dir_base1,f'COMPARISON_{band}',f'Comparison_{band}_{date_yj}.csv')
    #         file2 = os.path.join(dir_base2,f'COMPARISON_{band}', f'Comparison_{band}_{date_yj}.csv')
    #         print(file1,file2)
    #         if os.path.exists(file1) and os.path.exists(file2):
    #             df1 = pd.read_csv(file1,sep=';')
    #             df2 = pd.read_csv(file2,sep=';')
    #             arraym1 = np.array(df1['OlciVal'])
    #             arraym2 = np.array(df2['OlciVal'])
    #             rm = np.mean(arraym1/arraym2)
    #             line = f'{line};{rm}'
    #             nfiles = nfiles + 1
    #     if nfiles==len(bands):
    #         f1.write('\n')
    #         f1.write(line)
    #
    #     date_here = date_here + timedelta(hours=240)
    #
    # f1.close()

    # import pandas as pd
    # file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/ALGORITHMS/rrs_points_kd.csv'
    # file_output = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/ALGORITHMS/kd_olci.csv'
    # file_ref = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/kd490_points.csv'
    # print('Getting values ref...')
    # values_ref = {}
    # dref = pd.read_csv(file_ref,sep=';')
    # for idx,row in dref.iterrows():
    #     datestr = str(row['Date'])
    #     index = str(int(row['Index']))
    #     dindex= f'{datestr}{index}'
    #     kdval = row['OlciVal']
    #     values_ref[dindex] = kdval
    # print('Creating file out')
    # f1 = open(file_output,'w')
    # f1.write('kd_olci')
    # df = pd.read_csv(file_input,sep=';')
    # for idx,row in df.iterrows():
    #     datestr = str(row['Date'])
    #     index = str(int(row['Index']))
    #     dindex = f'{datestr}{index}'
    #     if dindex in values_ref:
    #         value = values_ref[dindex]
    #     else:
    #         value = -999.0
    #     line = f'{value}'
    #     f1.write('\n')
    #     f1.write(line)
    # f1.close()


def do_extract_csv():
    import pandas as pd

    from netCDF4 import Dataset
    # file_csv = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPSv6/data_for_RRS_validation.csv'
    file_csv = '/store/COP2-OC-TAC/arc/multi/validation/data_for_chl_algo_training.csv'
    # file_grid = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MULTI/GRID_FILES/ArcGrid_65_90_4KM_GridBase.nc'
    file_grid = '/store/COP2-OC-TAC/arc/multi/validation/ArcGrid_65_90_4KM_GridBase.nc'
    dir_dataset = '/store/COP2-OC-TAC/arc/multi/'
    # fout = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPSv6/data_for_RRS_validation_match_ups.csv'
    fout = '/store/COP2-OC-TAC/arc/multi/validation/data_for_chl_algo_training_1x1_match_ups.csv'

    dgrid = Dataset(file_grid)
    lat_array = np.array(dgrid.variables['lat'])
    lon_array = np.array(dgrid.variables['lon'])
    dgrid.close()

    lines_out = ['CCIV6_RRS412;CCIV6_RRS443;CCIV6_RRS490;CCIV6_RRS510;CCIV6_RRS560;CCIV6_RRS665;CCIV6_CHLA']
    df = pd.read_csv(file_csv, ';')
    for index, row in df.iterrows():
        line_out = None
        datestr = str(int(row['Date']))
        date_here = dt.strptime(datestr, '%Y%m%d')
        year_str = date_here.strftime('%Y')
        jday_str = date_here.strftime('%j')
        dir_dataset_day = os.path.join(dir_dataset, year_str, jday_str)
        lat_P = float(row['lat'])
        lon_P = float(row['lon'])
        dist_squared = (lat_array - lat_P) ** 2 + (lon_array - lon_P) ** 2
        r, c = np.unravel_index(np.argmin(dist_squared), lon_array.shape)
        name_rrs = f'C{year_str}{jday_str}_rrs-arc-4km.nc'
        file_rrs = os.path.join(dir_dataset_day, name_rrs)
        name_chla = f'C{year_str}{jday_str}_chl-arc-4km.nc'
        file_chla = os.path.join(dir_dataset_day, name_chla)

        print(f'[INFO] Index: {index} ------------------------')
        print(f'[INFO] Date: {date_here} {year_str}-{jday_str}')
        print(f'[INFO] Latitude: {lat_P} Longitude: {lon_P}')
        print(f'[INFO] Row: {r} Column: {c}')

        if os.path.exists(file_rrs):
            print(f'[INFO] File rrs: {file_rrs}')
            drrs = Dataset(file_rrs)
            variables = ['RRS412', 'RRS443', 'RRS490', 'RRS510', 'RRS560', 'RRS665']
            for variable in variables:
                array_here = np.array(drrs.variables[variable])

                ## 1x1
                val_here = array_here[0, r, c]

                ## 3x3
                # val_test = array_here[0,r,c]
                #
                # if val_test!=-999:
                #     array_w = array_here[0,r-1:r+2,c-1:c+2]
                #     array_c = array_w[array_w!=-999]
                #     val_here = np.mean(array_c[:])
                # else:
                #     val_here = -999

                if line_out is None:
                    line_out = f'{val_here}'
                else:
                    line_out = f'{line_out};{val_here}'
            drrs.close()
        else:
            print(f'[WARNING] File rrs {file_rrs} does not exist')
            line_out = 'NaN;NaN;NaN;NaN;NaN;NaN'

        if os.path.exists(file_chla):
            print(f'[INFO] File chl-a: {file_chla}')
            dchl = Dataset(file_chla)
            array_here = np.array(dchl.variables['CHL'])
            val_here = array_here[0, r, c]
            line_out = f'{line_out};{val_here}'
            dchl.close()
        else:
            print(f'[WARNING] File chl-a {file_chla} does not exist')
            line_out = f'{line_out};NaN'

        lines_out.append(line_out)

    print('[INFO] Writting...')
    fr = open(file_csv, 'r')
    fw = open(fout, 'w')
    index = 0
    for line in fr:
        line_out = lines_out[index]
        line_out = f'{line.strip()};{line_out}'
        print(line_out)
        fw.write(line_out)
        fw.write('\n')
        index = index + 1
    fr.close()
    fw.close()

    print(f'[INFO] Completed')


def do_comparison_daily_integrated():
    from netCDF4 import Dataset
    import numpy as np
    print('comparison daily-integrated')
    # dirnew = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/daily'
    # dirold = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/integrated'
    # file_out_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/comparison_old_new_510'

    dirnew = '/store/COP2-OC-TAC/arc/daily'
    dirold = '/store/COP2-OC-TAC/arc/integrated'
    file_out_base = '/store/COP2-OC-TAC/ARC_COMPARISON_MULTI_OLCI/comparison_old_new_510'

    date_ini = dt.strptime(args.input, '%Y-%m-%d')
    date_fin = dt.strptime(args.output, '%Y-%m-%d')
    date_ini_str = date_ini.strftime('%Y%m%d')
    date_fin_str = date_fin.strftime('%Y%m%d')
    file_out = f'{file_out_base}_{date_ini_str}_{date_fin_str}.csv'
    f1 = open(file_out, 'w')
    f1.write('Date;Old-Integrated;New-Daily;Ratio')

    date_ref = date_ini
    while date_ref <= date_fin:
        if date_ref.month <= 2 or date_ref.month >= 11:
            date_ref = date_ref + timedelta(hours=24)
            continue
        date_str = date_ref.strftime('%Y%j')
        print(date_str)
        yyyy = date_ref.strftime('%Y')
        jjj = date_ref.strftime('%j')
        name_file = f'O{date_str}_rrs-arc-fr.nc'
        file_old = os.path.join(dirold, yyyy, jjj, name_file)
        file_new = os.path.join(dirnew, yyyy, jjj, name_file)
        val_old = -999
        val_new = -999
        val_ratio = -999
        if os.path.exists(file_old):
            dold = Dataset(file_old)
            array_old = np.array(dold.variables['RRS510'])
            array_valid_old = array_old[array_old != -999]
            val_old = np.mean(array_valid_old) * 1000
            dold.close()
        if os.path.exists(file_new):
            dnew = Dataset(file_new)
            array_new = np.array(dnew.variables['RRS510'])
            array_valid_new = array_new[array_new != -999]
            val_new = np.mean(array_valid_new) * 1000
            dnew.close()
        if val_old != -999 and val_new != -999:
            val_ratio = val_old / val_new
        line = f'{date_str};{val_old};{val_new};{val_ratio}'
        f1.write('\n')
        f1.write(line)
        date_ref = date_ref + timedelta(hours=24)

    f1.close()
    print('DONE')


def do_comparasion_multi_olci_byday():
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np
    var = 'CHL'
    dir_comparison = f'/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/COMPARISON_{var}'
    fout = f'/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/daily_check_{var}.csv'
    f1 = open(fout, 'w')
    f1.write('date;multival;olcival;ratio')
    date_ini = dt(2016, 5, 1)
    date_fin = dt(2023, 5, 31)
    date_ref = date_ini
    while date_ref <= date_fin:
        if date_ref.month <= 2 or date_ref.month >= 11:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 6 and date_ref.day == 24 and date_ref.year == 2019:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 6 and date_ref.day == 27 and date_ref.year == 2019:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 7 and date_ref.day == 26 and date_ref.year == 2019:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 7 and date_ref.day == 29 and date_ref.year == 2019:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 3 and date_ref.day == 1 and date_ref.year == 2020:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 9 and date_ref.day == 11 and date_ref.year == 2021:
            date_ref = date_ref + timedelta(hours=24)
            continue

        # chl
        if date_ref.month == 3 and date_ref.day == 9 and date_ref.year == 2020:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 7 and date_ref.day == 13 and date_ref.year == 2020:
            date_ref = date_ref + timedelta(hours=24)
            continue
        if date_ref.month == 4 and date_ref.day == 26 and date_ref.year == 2023:
            date_ref = date_ref + timedelta(hours=24)
            continue

        date_str = date_ref.strftime('%Y%j')
        print(date_str)
        name_comparison = f'Comparison_{var}_{date_str}.csv'
        file_comparison = os.path.join(dir_comparison, name_comparison)

        df = pd.read_csv(file_comparison, sep=';')
        val_multi = df['MultiVal'].to_numpy()
        val_olci = df['OlciVal'].to_numpy()
        ratio = np.log10(val_olci) / np.log10(val_multi)
        avg_multi = np.log10(np.mean(val_multi))
        avg_olci = np.log10(np.mean(val_olci))
        avg_ratio = np.mean(ratio)
        line = f'{date_str};{avg_multi};{avg_olci};{avg_ratio}'
        f1.write('\n')
        f1.write(line)

        date_ref = date_ref + timedelta(hours=24)
    f1.close()


def do_comparison_multi_olci():
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np

    if args.input == 'TEST':
        do_test()
        return

    region = 'med'
    if args.region:
        region = args.region
    dir_out_base = args.output

    do_grid = False
    do_global = False
    do_prepare_plot = False
    do_reduce = False
    if args.input == 'GRID':
        do_grid = True

    if args.input.startswith('GLOBAL%'):
        do_global = True

    if args.input == 'PREPAREPLOT':
        do_prepare_plot = True

    if args.input == 'REDUCE':
        do_reduce = True

    if do_grid:
        scale = 40
        # input grid multi
        # file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/MULTI/2022/130/C2022130_chl-arc-4km.nc'
        file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/OLCI/2022/130/O2022130_plankton-arc-fr.nc'
        file_out = os.path.join(dir_out_base, f'GridMulti{region.capitalize()}.csv')
        dataset = Dataset(file_input)
        lat_array = np.array(dataset.variables['lat'])
        lon_array = np.array(dataset.variables['lon'])

        if len(lat_array.shape) == 1:
            nlat = len(lat_array)
            nlon = len(lon_array)
        elif len(lat_array.shape) == 2:
            nlat = lat_array.shape[0]
            nlon = lat_array.shape[1]
        index = 1
        f1 = open(file_out, 'w')
        line = f'Index;YMulti;XMulti;Latitude;Longitude'
        f1.write(line)
        for y in range(0, nlat, scale):
            for x in range(0, nlon, scale):
                if len(lat_array.shape) == 1:
                    lat_here = lat_array[y]
                    lon_here = lon_array[x]
                elif len(lat_array.shape) == 2:
                    lat_here = lat_array[y, x]
                    lon_here = lon_array[y, x]
                line = f'{index};{y};{x};{lat_here};{lon_here}'
                index = index + 1
                print(line)
                f1.write('\n')
                f1.write(line)
        f1.close()

        # input grid olci
        # file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/OLCI/2022/130/O2022130_plankton-arc-fr.nc'
        file_input = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_COMPARISON_OLCI_MULTI/MULTI/2022/130/C2022130_chl-arc-4km.nc'
        file_grid = os.path.join(dir_out_base, f'GridMulti{region.capitalize()}.csv')
        file_out = os.path.join(dir_out_base, f'GridOlci{region.capitalize()}.csv')

        grid = pd.read_csv(file_grid, sep=';')
        lat_grid = grid['Latitude'].to_numpy()
        lon_grid = grid['Longitude'].to_numpy()
        dataset = Dataset(file_input)
        lat_array = np.array(dataset.variables['lat'])
        lon_array = np.array(dataset.variables['lon'])
        f1 = open(file_out, 'w')
        line = f'Index;YOlci;XOlci;Latitude;Longitude'
        f1.write(line)
        for idx in range(len(lat_grid)):
            index = idx + 1
            lat_here = lat_grid[idx]
            lon_here = lon_grid[idx]
            if len(lat_array.shape) == 1:
                y = np.argmin(np.abs(lat_array - lat_here))
                x = np.argmin(np.abs(lon_array - lon_here))
            elif len(lat_array.shape) == 2:
                d = ((lat_array - lat_here) * (lat_array - lat_here)) + (
                        (lon_array - lon_here) * (lon_array - lon_here))
                ixs = np.unravel_index(np.argmin(d), d.shape)
                y = ixs[0]
                x = ixs[1]

            line = f'{index};{y};{x};{lat_here};{lon_here}'
            print(line)
            f1.write('\n')
            f1.write(line)
        f1.close()

        file_end = os.path.join(dir_out_base, f'Grid{region.capitalize()}.csv')
        dfmulti = pd.read_csv(file_grid, sep=';')
        dfolci = pd.read_csv(file_out, sep=';')
        dfmulti['YOlci'] = dfolci['YOlci']
        dfmulti['XOlci'] = dfolci['XOlci']
        dfmulti['MultiVal'] = -999.0
        dfmulti['OlciVal'] = -999.0
        dfmulti['Valid'] = 0
        dfmulti.to_csv(file_end, sep=';')
        return

    if do_global:
        from datetime import datetime as dt
        # getting global points

        param = args.input.split('%')[1]
        param_name = param
        if param.startswith('RRS') and region != 'arc':
            param_name = 'RRS'
        dir_comparison = os.path.join(dir_out_base, f'COMPARISON_{param_name}')

        file_out = os.path.join(dir_out_base, f'{param.lower()}_points.csv')

        # file_ref = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/rrs510_points.csv'
        file_ref = None
        if args.file_ref:
            file_ref = args.file_ref
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')

        colMulti = 'MultiVal'
        colOlci = 'OlciVal'
        if param_name == 'RRS':
            colMulti = f'{colMulti}_{param}'
            colOlci = f'{colOlci}_{param}'

        first_line = f'Date;Index;MultiVal;OlciVal'
        f1 = open(file_out, 'w')
        f1.write(first_line)
        nfiles = 0
        if file_ref is None:
            date_here = start_date
            while date_here <= end_date:
                year = date_here.strftime('%Y')
                jday = date_here.strftime('%j')
                file_c = os.path.join(dir_comparison, f'Comparison_{param_name}_{year}{jday}.csv')
                date_here_str = date_here.strftime('%Y-%m-%d')

                if os.path.exists(file_c):
                    print(file_c)
                    nfiles = nfiles + 1
                    points_here = pd.read_csv(file_c, sep=';')
                    for index, row in points_here.iterrows():
                        multi_val = row[colMulti]
                        olci_val = row[colOlci]
                        index_here = row['Index']
                        # if (param == 'CHL' or param == 'KD490') and (multi_val < 0 or olci_val < 0):
                        #     continue
                        if (param == 'CHL' or param == 'KD490') and (olci_val <= 0):
                            continue
                        line = f'{date_here_str};{index_here};{multi_val};{olci_val}'
                        f1.write('\n')
                        f1.write(line)

                date_here = date_here + timedelta(hours=240)  # 10 days
                # if date_here.year==2019 and date_here.month==10 and date_here.day==23:
                #     date_here = date_here + timedelta(hours=24)
        else:
            df_ref = pd.read_csv(file_ref, sep=';')
            dates_ref = df_ref['Date']
            index_ref = df_ref['Index']
            nref = len(df_ref.index)
            date_check = start_date
            # print(date_check)
            year = date_check.strftime('%Y')
            jday = date_check.strftime('%j')
            date_check_str = date_check.strftime('%Y-%m-%d')
            file_c = os.path.join(dir_comparison, f'Comparison_{param_name}_{year}{jday}.csv')
            points_check_here = pd.read_csv(file_c, sep=';')
            indices_check_here = points_check_here['Index'].to_numpy(dtype=np.int32).tolist()

            nnodata = 0
            ndata = 0
            nfiles = 1
            for idx in range(nref):
                date_here = dt.strptime(str(dates_ref[idx]), '%Y-%m-%d')
                index_here = int(index_ref[idx])
                if date_here != date_check:
                    # print(date_check)
                    date_check = date_here
                    year = date_check.strftime('%Y')
                    jday = date_check.strftime('%j')
                    date_check_str = date_check.strftime('%Y-%m-%d')
                    file_c = os.path.join(dir_comparison, f'Comparison_{param_name}_{year}{jday}.csv')
                    points_check_here = pd.read_csv(file_c, sep=';')
                    indices_check_here = points_check_here['Index'].to_numpy(dtype=np.int32).tolist()
                    nfiles = nfiles + 1
                if index_here in indices_check_here:
                    idx = indices_check_here.index(index_here)
                    multi_val = points_check_here.iloc[idx].at[colMulti]
                    olci_val = points_check_here.iloc[idx].at[colOlci]
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
        return

    if do_prepare_plot:
        from datetime import datetime as dt
        for name in os.listdir(dir_out_base):
            if not name.endswith('.csv'):
                continue
            fname = os.path.join(dir_out_base, name)
            print('------------------------>', fname)
            df = pd.read_csv(fname, sep=';')
            fout = os.path.join(dir_out_base, f'{name[:-4]}_valid.csv')
            f1 = open(fout, 'w')
            f1.write('Date;Index;MultiVal;OlciVal')
            for index, row in df.iterrows():
                date_here = row['Date']
                index_here = str(row['Index'])
                valMulti = row['MultiVal']
                valOlci = row['OlciVal']
                if np.isnan(valMulti):
                    continue
                if np.isnan(valOlci):
                    continue
                f1.write('\n')
                line = f'{date_here};{index_here};{valMulti};{valOlci}'
                f1.write(line)
            f1.close()

        indices = {}
        nfiles = 0
        for name in os.listdir(dir_out_base):
            if not name.endswith('_valid.csv'):
                continue
            nfiles = nfiles + 1
            fname = os.path.join(dir_out_base, name)
            print('------------------------>', fname)
            df = pd.read_csv(fname, sep=';')
            date_ref_ym = ''
            for index, row in df.iterrows():
                date_here = row['Date']
                index_here = str(row['Index'])
                date_here_ym = dt.strptime(date_here, '%Y-%m-%d').strftime('%Y%m')
                di = f'{date_here}_{index_here}'
                if date_here_ym != date_ref_ym:
                    print(date_here_ym)
                    date_ref_ym = date_here_ym

                if di in indices:
                    indices[di] = indices[di] + 1
                else:
                    indices[di] = 1
            print('------------------------')

        for name in os.listdir(dir_out_base):
            if not name.endswith('_valid.csv'):
                continue
            fname = os.path.join(dir_out_base, name)
            fout = os.path.join(dir_out_base, f'{name[:-4]}_common.csv')
            f1 = open(fout, 'w')
            f1.write('Date;Index;MultiVal;OlciVal')
            df = pd.read_csv(fname, sep=';')
            print('------------------------>', fout)
            for index, row in df.iterrows():
                date_here = row['Date']
                index_here = str(row['Index'])
                date_here_ym = dt.strptime(date_here, '%Y-%m-%d').strftime('%Y%m')
                di = f'{date_here}_{index_here}'
                if date_here_ym != date_ref_ym:
                    print(date_here_ym)
                    date_ref_ym = date_here_ym
                if indices[di] == nfiles:
                    f1.write('\n')
                    valMulti = row['MultiVal']
                    valOlci = row['OlciVal']
                    line = f'{date_here};{index_here};{valMulti};{valOlci}'
                    f1.write(line)
            f1.close()

        return

    if do_reduce:
        file_input = args.file_ref
        import pandas as pd
        file_grid = os.path.join(dir_out_base, f'Grid{region.capitalize()}.csv')
        indices = {}
        df = pd.read_csv(file_grid, sep=';')
        for index, row in df.iterrows():
            index_here = str(int(row['Index']))
            ymulti = float(row['YMulti'])
            xmulti = float(row['XMulti'])
            indices[index_here] = {
                'YMulti': ymulti,
                'XMulti': xmulti
            }

        name_in = file_input.split('/')[-1]
        name_out = f'{name_in[:-4]}_reduced.csv'
        file_out = file_input.replace(name_in, name_out)
        f1 = open(file_out, 'w')
        f1.write('Date;Index;MultiVal;OlciVal')
        f2 = open(file_input, 'r')
        for line in f2:
            l = line.split(';')
            if l[1].strip() == 'Index':
                continue
            try:
                index_here = str(int(float(l[1].strip())))
                ymulti_here = indices[index_here]['YMulti']
                xmulti_here = indices[index_here]['XMulti']
                if (ymulti_here % 20) == 0 and (xmulti_here % 20) == 0:
                    f1.write('\n')
                    f1.write(line.strip())
            except:
                pass

        f2.close()
        f1.close()

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
    dir_olci_orig = '/dst04-data1/OC/OLCI/daily_3.01'
    dir_multi_orig = '/store3/OC/MULTI/daily_v202311_x'
    nhours = 240
    if args.interval:
        nhours = int(args.interval) * 24
    if region == 'arc':
        dir_olci_orig = '/store/COP2-OC-TAC/arc/integrated'
        dir_multi_orig = '/store/COP2-OC-TAC/arc/multi'
    if region == 'arcn':
        dir_olci_orig = '/store/COP2-OC-TAC/arc/daily'
        dir_multi_orig = '/store/COP2-OC-TAC/arc/multi'
        region = 'arc'
        # nhours = 24
    # dir_olci_orig = f'/mnt/c/DATA_LUIS/OCTAC_WORK/{region.upper()}_COMPARISON_OLCI_MULTI/OLCI'
    # dir_multi_orig = f'/mnt/c/DATA_LUIS/OCTAC_WORK/{region.upper()}_COMPARISON_OLCI_MULTI/MULTI'
    # FOLDERS: CHLA, RRS443, RRS490, RRS510, RRS560, RRS670
    if args.input == 'ALL':
        if args.region == 'arc' or args.region == 'arcn':
            params = ['RRS443', 'RRS490', 'RRS510', 'RRS560', 'RRS665']
        else:
            params = ['CHL', 'KD490']
    elif args.input == 'RRS':
        params = ['RRS']
        wl_multi = ['443', '490', '510', '560', '665']
        wl_olci = ['442_5', '490', '510', '560', '665']
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
        file_new_grid = os.path.join(dir_out_base, f'Grid{region.capitalize()}_BandShifting.csv')
        dfgrid = pd.read_csv(file_grid, sep=';')

        for wl in wl_multi:
            new_col = f'MultiVal_RRS_{wl}'
            col_names_multi.append(new_col)
            dfgrid[new_col] = -999.0
        for wl in wl_multi:  # BAND SHIFTING FROM OLCI TO MULTI
            new_col = f'OlciVal_RRS_{wl}'
            col_names_olci.append(new_col)
            dfgrid[new_col] = -999.0
        dfgrid = dfgrid.drop('MultiVal', axis=1)
        dfgrid = dfgrid.drop('OlciVal', axis=1)
        dfgrid.to_csv(file_new_grid, sep=';')
        file_grid = file_new_grid

    start_date = dt.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.strptime(args.end_date, '%Y-%m-%d')
    date_here = start_date

    while date_here <= end_date:

        if args.verbose:
            print(f'[INFO] Worknig for date: {date_here}...')

        if region == 'arc':
            if date_here.month >= 11 or date_here.month <= 2:
                date_here = date_here + timedelta(hours=nhours)
                continue
        for param, dir_out in zip(params, dir_outs):
            print(param, dir_out)
            # PARAMS, TO DEFINE FILE NAMES
            param_multi = param
            param_olci = param
            if param_multi == 'RRS443':
                param_olci = 'RRS442_5'
            if param_multi == 'RRS555':
                param_olci = 'RRS560'
            if region == 'arc':
                if param_multi == 'CHL':
                    param_olci = 'PLANKTON'
                if param_multi == 'KD490':
                    param_olci = 'TRANSP'

            var_multi = param_multi
            var_olci = param_olci
            if param_olci == 'PLANKTON':
                var_olci = 'CHL'
            if param_olci == 'TRANSP':
                var_olci = 'KD490'

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
                        if region == 'arc':
                            file_multi = os.path.join(dir_multi, f'C{year}{jday}_{param_multi.lower()}-{region}-4km.nc')
                            file_olci = os.path.join(dir_olci, f'O{year}{jday}_{param_olci.lower()}-{region}-fr.nc')
                        else:
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
                                                           wl_olci, col_names_multi, col_names_olci)
                else:
                    # file_olci = os.path.join(dir_olci, f'O{year}{jday}-{param_olci.lower()}-{region}-fr.nc')
                    if region == 'arc':
                        if param_multi.startswith('RRS'):
                            param_multi = 'RRS'
                            param_olci = 'RRS'
                        file_multi = os.path.join(dir_multi, f'C{year}{jday}_{param_multi.lower()}-{region}-4km.nc')
                        file_olci = os.path.join(dir_olci, f'O{year}{jday}_{param_olci.lower()}-{region}-fr.nc')
                    else:
                        file_multi = os.path.join(dir_multi, f'X{year}{jday}-{param_multi.lower()}-{region}-hr.nc')
                        file_olci = os.path.join(dir_olci, f'O{year}{jday}-{param_olci.lower()}-{region}-fr.nc')

                    if os.path.exists(file_multi) and os.path.exists(file_olci):
                        print(f'[INFO] Making date: {date_here}')
                        file_out = os.path.join(dir_out, f'Comparison_{param}_{year}{jday}.csv')
                        make_comparison_impl(file_grid, file_multi, file_olci, file_out, var_multi, var_olci)

                    if not os.path.exists(file_multi) and os.path.exists(file_olci):
                        print(f'[INFO] Making date: {date_here}')
                        file_out = os.path.join(dir_out, f'Comparison_{param}_{year}{jday}.csv')
                        make_comparison_impl(file_grid, None, file_olci, file_out, None, var_olci)

        date_here = date_here + timedelta(hours=nhours)

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
    # make_comparison_impl(file_grid,file_multi,file_olci,file_out,'CHL','CHL',False)

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
    #             make_comparison_impl(file_grid,file_multi,file_olci,file_out,'RRS665','RRS665',False)
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

    window_size = 3
    if args.region:
        if args.region == 'arc':
            window_size = 13
    wini = int(np.floor(window_size / 2))
    wfin = int(np.ceil(window_size / 2))
    nvalid = window_size * window_size

    grid = pd.read_csv(file_grid, sep=';')
    if file_multi is not None:
        dataset_multi = Dataset(file_multi)
        array_multi = np.array(dataset_multi.variables[variable_multi])
    dataset_olci = Dataset(file_olci)
    array_olci = np.array(dataset_olci.variables[variable_olci])
    for index, row in grid.iterrows():
        ymulti = int(row['YMulti'])
        xmulti = int(row['XMulti'])
        yolci = int(row['YOlci'])
        xolci = int(row['XOlci'])
        valid = 0
        if file_multi is None:
            val_multi = -999
        else:
            val_multi = array_multi[0, ymulti, xmulti]
        array_here = array_olci[0, yolci - wini:yolci + wfin, xolci - wini:xolci + wfin]
        array_here_good = array_here[array_here != -999]
        val_olci = -999
        if len(array_here_good) == nvalid:
            val_olci = np.mean(array_here[array_here != -999])
            # val_olci = val_olci/np.pi

        if val_olci != -999 and val_multi != -999:
            valid = 1
        if val_olci != -999 and file_multi is None:
            valid = 1
        grid.loc[index, 'MultiVal'] = val_multi
        grid.loc[index, 'OlciVal'] = val_olci
        grid.loc[index, 'Valid'] = valid

    dataset_olci.close()
    if file_multi is not None:
        dataset_multi.close()
    grid_valid = grid[grid['Valid'] == 1]
    grid_valid.to_csv(file_out, sep=';')


def make_comparison_band_shifting_impl(file_grid, files_multi, files_olci, file_out, wl_multi, wl_olci, col_names_multi,
                                       col_names_olci):
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np
    from BSC_QAA import bsc_qaa_EUMETSAT as qaa
    # import warnings
    # warnings.filterwarnings("ignore")
    wl_output = [float(x.replace('_', '.')) for x in wl_multi]
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
        if len(array_here_good) == num_olci_good:
            array_here_good_res = np.reshape(array_here_good, (num_o, 9))
            spectra_olci = np.mean(array_here_good_res, axis=1)
            if len(spectra_multi[spectra_multi != -999]) == num_m:
                valid = 1
                spectra_olci = qaa.bsc_qaa(spectra_olci, wl_input, wl_output)
        else:
            spectra_olci = np.array([-999.0] * num_o)

        grid.loc[index, 'Valid'] = valid
        if valid == 1:
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
