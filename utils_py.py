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

import pytz

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(
    description="General utils")

parser.add_argument('-m', "--mode", help="Mode",
                    choices=['concatdf', 'removerep', 'checkextractsdir', 'dhusget', 'printscp', 'removencotmp',
                             'removefiles', 'copyfile', 'copys3folders', 'comparison_bal_multi_olci',
                             'comparison_multi_olci', 'comparison_cmems_certo', 'coverage_cmems_certo', 'extract_csv',
                             'checksensormask','match-ups_from_extracts', 'doors_certo_msi_csv', 'aqua_check', 'monocle',
                             'insitu_match-ups','read_odv','iop_qaa','filter_chla'])
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

def do_test5():
    import pandas as pd
    # file_insitu_withextracts = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_with_extracts/Baltic_CHLA_Valid_AllSources_1997-2023_FINAL_extracts_rrs_chl.csv'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_with_extracts/check.csv'
    # fw = open(file_out,'w')
    # fw.write('Repeated')
    # df = pd.read_csv(file_insitu_withextracts,sep=';')
    # extract_prev = ''
    # for index,row in df.iterrows():
    #     fw.write('\n')
    #     extract_here = row['ExtractRrs']
    #     if extract_here==extract_prev:
    #         fw.write('1')
    #     else:
    #         fw.write('0')
    #     extract_prev = extract_here
    # fw.close()
    file_in = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_brando2021/matchup_bal_cci_chl_surf6_orig__rrsmatch_w12CHL__Filtered_AlreadyIncluded.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_brando2021/n_already_included_by_indices_insitu.csv'
    fw = open(file_out,'w')
    fw.write('Index;NBrando')
    list = [0]*13840
    df = pd.read_csv(file_in,sep=';')
    for index,row in df.iterrows():
        index_is = row['IndexInsitu']
        list[index_is-1] = list[index_is-1]+1

    for i,l in enumerate(list):
        fw.write('\n')
        fw.write(f'{i+1};{l}')
        print(l)
    fw.close()
    return True

def do_test4():
    import pandas as pd
    # file_brando = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_brando2021/matchup_bal_cci_chl_surf6_orig__rrsmatch_w12CHL__All.csv'
    # file_brando_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_brando2021/matchup_bal_cci_chl_surf6_orig__rrsmatch_w12CHL__Filtered.csv'
    # df = pd.read_csv(file_brando,sep=';')
    # first_line = ';'.join(df.columns.tolist())
    # fw = open(file_brando_out,'w')
    # fw.write(first_line)
    # date_prev = -1
    # hour_prev = -1
    # prev = -1
    # nprev = 0
    # ntot = 0
    # for index,row in df.iterrows():
    #     date_here = row['date']
    #     hour_here = row['time']
    #     here = row['lat']
    #     if date_here!=date_prev or hour_here!=hour_prev or index==10137:
    #         if nprev>9:
    #             print(index,nprev)
    #         nprev = 1
    #         ntot = ntot +1
    #         line = ";".join([str(x) for x in row.tolist()])
    #         fw.write('\n')
    #         fw.write(line)
    #     else:
    #         check = abs(here - prev)
    #         if check>0.0001:
    #             print('WARNING->',index,row.tolist())
    #         nprev = nprev +1
    #     date_prev = date_here
    #     hour_prev = hour_here
    #     prev = here
    #
    # fw.close()
    # print(ntot)
    file_insitu_final = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_sources/Baltic_CHLA_Valid_AllSources_1997-2023_FINAL.csv'
    data_insitu = {}
    df = pd.read_csv(file_insitu_final,sep=';')
    for index,row in df.iterrows():
        date_str = row['DATE']
        hour_str = row['HOUR']
        hour_float = float(hour_str.split(':')[0])+(float(hour_str.split(':')[1])/60.0)
        print(date_str,hour_float)
        if date_str not in data_insitu:
            data_insitu[date_str] = {
                'hour': [hour_float],
                'chla': [float(row['CHLA'])],
                'lat': [float(row['LATITUDE'])],
                'lon': [float(row['LONGITUDE'])],
                'index': [int(row['INDEX'])]
            }
        else:
            data_insitu[date_str]['hour'].append(hour_float)
            data_insitu[date_str]['chla'].append(float(row['CHLA']))
            data_insitu[date_str]['lat'].append(float(row['LATITUDE']))
            data_insitu[date_str]['lon'].append(float(row['LONGITUDE']))
            data_insitu[date_str]['index'].append(int(row['INDEX']))


    file_brando = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_brando2021/matchup_bal_cci_chl_surf6_orig__rrsmatch_w12CHL__Filtered.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_brando2021/Index_InSitu.csv'
    fw = open(file_out,'w')
    fw.write('IndexInsitu')
    dfb = pd.read_csv(file_brando,sep=';')
    nnodata = 0
    for index,row in dfb.iterrows():
        fw.write('\n')
        date_str = str(row['date'])
        chl_here = float(row['CHL_in'])
        date_str_c = dt.strptime(date_str,'%Y%m%d').strftime('%Y-%m-%d')
        if date_str_c in data_insitu.keys():
            chl_insitu_array = np.array(data_insitu[date_str_c]['chla'])
            indices = data_insitu[date_str_c]['index']
            index_min = np.argmin(np.abs(chl_here-chl_insitu_array))
            diff_chla = abs(chl_here-chl_insitu_array[index_min])
            if diff_chla<0.01:
                fw.write(str(indices[index_min]))
            else:
                nnodata = nnodata + 1
                fw.write('-1')
        else:
            fw.write('-1')
            nnodata = nnodata + 1

    print(nnodata)
    return True
def do_test3():
    import pandas as pd
    file_new = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_sources/Baltic_CHLA_Valid_AllSources_1997-2023_FINAL_rrs.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_sources/toeliminate.csv'
    fw = open(file_out,'w')
    fw.write('INDEX;ELIMINATE')
    df_new = pd.read_csv(file_new,sep=';')
    extracts = {}
    for index,row in df_new.iterrows():
        index_here = row['IndexExtract']
        fw.write('\n')
        if index_here==-1:
            fw.write(f'{row["INDEX"]};0')
            continue
        extract_here = row['Extract']
        chl_here = float(row['CHLA'])
        if extract_here not in extracts.keys():
            extracts[extract_here] = chl_here
            fw.write(f'{row["INDEX"]};0')
        else:
            diff = abs(chl_here - extracts[extract_here])
            check = diff<=0.105
            print('Index here should be one: ',index_here,'-->',row['INDEX'],chl_here,'<->',extracts[extract_here],check)
            if check:
                fw.write(f'{row["INDEX"]};1')
            else:
                fw.write(f'{row["INDEX"]};0')

    fw.close()


    # file_new = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CSV_MATCH-UPS/MULTI/Baltic_CHLA_Valid_AllSources_1997-2023_extracts_match-ups_3x3_valid.csv'
    # file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CSV_MATCH-UPS/flag_publication.out'
    # file_pub = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/MDB___CCI_INSITU_19970101_20221231_4P5hours.csv'
    # dict_pub ={}
    # df_pub = pd.read_csv(file_pub,sep=';')
    # for index,row in df_pub.iterrows():
    #     ins_time = row['Ins_Time']
    #     if ins_time in dict_pub.keys():
    #         print(ins_time)
    #     dict_pub[ins_time] = {
    #         'CHLA':f'{float(row["insitu_CHLA"]):.2f}',
    #         'FOUND': False
    #     }


    # df_new = pd.read_csv(file_new,sep=';')
    # fw = open(file_out,'w')
    # fw.write('BalArticle2024')
    # n = 0
    # for index,row in df_new.iterrows():
    #     dthere = dt.strptime(row['DATETIME'],'%Y-%m-%dT%H:%M:%S')
    #     dtref  = dthere.strftime('%Y-%m-%d %H:%M')
    #
    #     #print(dthere)
    #     if dtref in dict_pub.keys():
    #         print('AQUI...')
    #         dict_pub[dtref]['FOUND'] = True
    #         n = n +1
    # print(n)
    # fw.close()
    #
    # n = 0
    # for t in dict_pub:
    #     if not dict_pub[t]['FOUND']:
    #         print(t)
    #         n = n +1
    # print(n)
    #
    # print(len(df_pub.index),len(dict_pub))
    return True
def do_test2():
    file_cyano  = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/MDBs/FlagCyanoNew.csv'
    import pandas as pd
    df = pd.read_csv(file_cyano,sep=';')
    data_cyano = {}
    for index,row in df.iterrows():
        key = f'{row["DATE"]}T{row["HOUR"]}'
        # data_cyano[key]={
        #     'RRS555': row['RRS555'],
        #     'RRS670': row['RRS670'],
        #     'FLAG_CYANO': row['FLAG_CYANO']
        # }
        data_cyano[key] = {
            'FLAG_CYANO': row['FLAG_CYANO']
        }

    print('Done 1')

    file_csv = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CSV_MATCH-UPS/MULTI/Baltic_CHLA_Valid_AllSources_1997-2023_FINAL_extracts_rrs_chl_3x3_valid_match-ups.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CSV_MATCH-UPS/MULTI/valid_out.csv'
    fout = open(file_out,'w')
    fout.write('RRS555;RRS670;FLAG_CYANO')
    fout.write('FLAG_CYANO')
    dfcsv = pd.read_csv(file_csv,';')
    for index,row in dfcsv.iterrows():
        key = row['DATETIME']
        if key in data_cyano:
            fout.write('\n')
            #fout.write(f'{data_cyano[key]["RRS555"]};{data_cyano[key]["RRS670"]};{data_cyano[key]["FLAG_CYANO"]}')
            fout.write(f'{data_cyano[key]["FLAG_CYANO"]}')
        else:
            fout.write('\n')
            fout.write('-999')
            # fout.write(f'{-999};{-999};{-999}')
            # if row['RRS665']!=-999:
            #     print('----------------------------------> no',row['DATETIME'],row['RRS665'])

    fout.close()
    print('DONE')
    return True

def create_copy(input_dataset,output_file):
    from netCDF4 import Dataset

    ncout = Dataset(output_file, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(input_dataset.__dict__)

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    for name, variable in input_dataset.variables.items():

        if name=='satellite_CHL_uncertainty':
            continue

        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             shuffle=True, complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)

        #copy data
        ncout[name][:] = input_dataset[name][:]

    # ncout.close()
    # input_dataset.close()
    return ncout

def do_test6():
    from netCDF4 import Dataset
    # dir_base = '/mnt/c/DATA_LUIS/TARA_TEST/chl_match-ups/extracts_chl_olci_regional'
    # dir_out = '/mnt/c/DATA_LUIS/TARA_TEST/chl_match-ups/temp'
    # #os.mkdir(dir_out)
    # for name in os.listdir(dir_base):
    #     file_in = os.path.join(dir_base,name)
    #     dataset = Dataset(file_in)
    #     if 'satellite_CHL_uncertainty' in dataset.variables:
    #
    #         file_out = os.path.join(dir_out,name)
    #         print(file_in, '-->', file_out)
    #         create_copy(dataset,file_out)

    # file_mdb = '/mnt/c/DATA_LUIS/TARA_TEST/chl_match-ups/MDBs/MDBr__CMEMS_MULTI_REGIONAL_1KM_20240101T000000_20240620T235959.nc'
    # file_out = file_mdb.replace('.nc','.csv')
    # dataset = Dataset(file_mdb)
    #
    # insitu_chl = dataset.variables['mu_insitu_Chl'][:]
    # satellite_chl = dataset.variables['mu_satellite_CHL'][:]
    # insitu_id = dataset.variables['mu_insitu_Chl_id'][:]
    # insitu_time = dataset.variables['insitu_time']
    # fw = open(file_out,'w')
    # fw.write('ID;Insitu_Time;Chl_Sat;Chl_Insitu')
    # for idx in range(insitu_chl.shape[0]):
    #     idx_insitu = insitu_id[idx]
    #     insitu_time_here = insitu_time[idx][idx_insitu]
    #     time_str = dt.utcfromtimestamp(float(insitu_time_here)).strftime('%Y-%m-%dT%H:%M:%S')
    #     line = f'{idx};{time_str};{satellite_chl[idx]};{insitu_chl[idx]}'
    #     fw.write('\n')
    #     fw.write(line)
    # fw.close()
    #
    #
    # dataset.close()

    file_mdb = '/mnt/c/DATA_LUIS/TARA_TEST/chl_match-ups/MDBs/MDBr__CCI_CCI-V6_4KM_STANDARD_20240101T000000_20240620T235959.nc'
    file_out = '/mnt/c/DATA_LUIS/TARA_TEST/chl_match-ups/MDBs/MDBr__CCI_CCI-V6_4KM_COMMON_20240101T000000_20240620T235959.nc'
    file_csv = '/mnt/c/DATA_LUIS/TARA_TEST/chl_match-ups/MDBs/MULTI_VALID.csv'
    import pandas as pd
    df = pd.read_csv(file_csv,sep=';')
    array_chl = np.array(df['Chl_Sat_Regional']).astype(np.float32)
    array_valid = np.array(df['Valid']).astype(np.int32)
    print(array_chl.shape,array_valid.shape)
    dataset = Dataset(file_mdb)
    ncout = create_copy(dataset,file_out)
    dataset.close()
    var_chl_reg = ncout.createVariable('mu_satellite_CHL_Regional', 'f4', ('mu_id',), fill_value=-999, zlib=True, complevel=6)
    var_valid = ncout.createVariable('mu_valid', 'f4', ('mu_id',), fill_value=-999, zlib=True, complevel=6)
    var_chl_reg[:] = array_chl[:]
    var_valid[:] = array_valid[:]
    ncout.close()
    return True

def main():
    # if do_test6():
    #     return

    print('[INFO] Started')

    if args.mode == 'insitu_match-ups':
        make_insitu_matchs_ups()
        return
    if args.mode == 'read_odv':
        input_path = args.input
        output_file = args.output
        make_csv_from_odv(input_path,output_file)

    if args.mode == 'monocle':
        input_path = args.input
        output_file = args.output
        make_monocle_from_json_to_csv(input_path, output_file)
        return

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

    if args.mode == 'comparison_cmems_certo':
        do_comparison_cmems_certo()

    if args.mode == 'coverage_cmems_certo':
        do_coverage_cmems_certo()

    if args.mode == 'extract_csv':
        # do_extract_csv()
        do_extract_csv_from_extracts()

    if args.mode == 'checksensormask':
        # do_check_sensor_mask()
        do_test()

    if args.mode == 'match-ups_from_extracts':
        do_match_ups_from_extracts()

    if args.mode == 'doors_certo_msi_csv':
        do_doors_certo_msi_csv()

    if args.mode == 'aqua_check':
        download_doors_http()
        # do_aqua_check()

    if args.mode == 'iop_qaa':
        compute_iop_qaa_from_csv(args.input)

    if args.mode == 'filter_chla':
        make_filter_chla(args.input,args.output)

def make_filter_chla(input_file,output_file):
    import pandas as pd
    df = pd.read_csv(input_file,sep=';')
    valid_lines = [True] * len(df.index)
    dtstr_prev = None
    chl_prev = None
    nremoved = 0
    indices_remove = [7517,4787,5254,5985,6436,6909]
    latitude = np.array(df['LATITUDE'][:])
    longitude = np.array(df['LONGITUDE'][:])
    for index,row in df.iterrows():
        dtstr_here = str(row['DATETIME'])[0:17]
        chl_here = str(row['CHLA'])
        if dtstr_here==dtstr_prev:

            if chl_here==chl_prev or index in indices_remove:
                #print('Remove line')
                valid_lines[index] = False
                nremoved = nremoved +1
            else:
                if chl_here[0:3]==chl_prev[0:3]:
                    #print('Remove line')
                    valid_lines[index] = False
                    nremoved = nremoved + 1
                else:
                    print('SAME DATETIME-->', index, chl_here, chl_prev)
                    print(longitude[index-1],latitude[index-1])
                    print(longitude[index],latitude[index])
                    print('--------------------------------------------------------------------------->to check')
        dtstr_prev = dtstr_here
        chl_prev = chl_here
    print(nremoved)
    df_new = df.loc[valid_lines,:]
    df_new.to_csv(output_file,sep=';')



def compute_iop_qaa_from_csv(input_path):
    import pandas as pd
    import numpy as np
    from BSC_QAA import bsc_qaa_EUMETSAT
    output_path = input_path[:-4]+'_iop.csv'
    df = pd.read_csv(input_path,sep=';')
    bands = [412,443,490,510,560,665]
    col_bands = [f'RRS{x}' for x in bands]
    bands = np.array(bands)
    out_bands = ['bbp443','adg443','aph443','lambda0','eta','s','a555','bbp0','a0']
    out = {x:[] for x in out_bands}
    for index,row in df.iterrows():
        print(index)
        rrs_in = np.array(row[col_bands])
        if rrs_in[0]==-999.0:
            for x in out_bands:
                out[x].append(-999)
            continue
        res = bsc_qaa_EUMETSAT.qaa(rrs_in,bands)
        for x in out_bands:
            out[x].append(res[x])
    for x in out_bands:
        df[x] = out[x]
    df.to_csv(output_path,sep=';')




def download_doors_http():
    import requests

    url = 'https://rsg.pml.ac.uk/shared_files/liat/DOORS_v2_additional_Danube/'
    response = requests.get(url, params={})
    dir_out = args.output
    for line in response.text.split('\n'):
        iini = line.find('a href="')
        ifin = line.find('"', iini + 8)
        if iini > 0:
            name_file = line[iini + 8:ifin]
            if not name_file.startswith('CERTO'):
                continue
            print(name_file)
            url_file = f'{url}{name_file}'
            date_here = dt.strptime(name_file.split('_')[2], '%Y%m%d')
            path_year = os.path.join(dir_out, date_here.strftime('%Y'))
            if not os.path.isdir(path_year):
                os.mkdir(path_year)
            path_jday = os.path.join(path_year, date_here.strftime('%j'))
            if not os.path.isdir(path_jday):
                os.mkdir(path_jday)
            file_out = os.path.join(path_jday, name_file)
            if not os.path.exists(file_out):
                r = requests.get(url_file, allow_redirects=True)
                open(file_out, 'wb').write(r.content)

def make_insitu_matchs_ups():
    import pandas as pd
    from datetime import datetime as dt
    file_chla = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_sources/Baltic_CHLA_Valid_Algaline_2022-2024.csv'
    file_rrs = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_sources/Monocle_PML005_WarrenNIRCorrection.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_sources/out.csv'
    time_diff_max = 60 * 3
    space_diff_max = (1 / 60) * 3
    method = 'nearest' #nearest, all
    df_rrs = pd.read_csv(file_rrs,sep=';')

    indices_by_date = {}
    rrs_timestamps = []
    for index,row in df_rrs.iterrows():
        date_time = dt.strptime(row['DateTime'],'%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
        rrs_timestamps.append(date_time.timestamp())
        key_date = date_time.strftime('%Y%m%d')
        if key_date not in indices_by_date.keys():
            indices_by_date[key_date] = [index]
        else:
            indices_by_date[key_date].append(index)
    rrs_timestamps = np.array(rrs_timestamps)
    df_chla = pd.read_csv(file_chla,sep=';')
    columns = df_chla.columns.tolist()+['INDEX_MATCH-UP','INDEX_SPECTRA','TIME_DIFF']+df_rrs.columns.tolist()
    df_new = pd.DataFrame(columns=columns)
    index_mu = 0
    for index,row in df_chla.iterrows():
        date_time = dt.strptime(row['datetime'],'%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.utc)
        utc_chl = date_time.timestamp()
        lat_chl = float(row['lat'])
        lon_chl = float(row['lon'])
        key_date = date_time.strftime('%Y%m%d')
        if key_date in indices_by_date:
            indices = indices_by_date[key_date]
            utc_rrs = rrs_timestamps[indices]
            time_diff = abs(utc_rrs-utc_chl)
            nfiltered = np.sum(time_diff<time_diff_max)
            if nfiltered>0:
                ##tutti
                indices_sort = np.argsort(time_diff)[0:nfiltered]
                indices_row = np.array(indices)[indices_sort]
                #df_day = df_rrs[indices_row.tolist()]
                lat_valid_day = np.array(df_rrs.loc[indices_row.tolist(),'GpsLatitude'])
                lon_valid_day = np.array(df_rrs.loc[indices_row.tolist(),'GpsLongitude'])
                diff_lat = np.abs(lat_valid_day-lat_chl)
                diff_lon = np.abs(lon_valid_day-lon_chl)

                indices_sort_final = indices_sort[np.logical_and(diff_lat<space_diff_max,diff_lon<=space_diff_max)]
                indices_row_final = np.array(indices)[indices_sort_final]
                if len(indices_row_final)>0:
                    print(f'[INFO] Identified {len(indices_row_final)} with a time difference lower that {time_diff_max} seconds')
                    if method=='nearest':
                        row_rrs = df_rrs.loc[indices_row_final[0]]
                        row_new = pd.concat([row,row_rrs])
                        row_new['INDEX_MATCH-UP'] = index_mu
                        row_new['INDEX_SPECTRA'] = 0
                        row_new['TIME_DIFF'] = time_diff[indices_sort_final[0]]
                        df_new = df_new.append(row_new, ignore_index=True)
                    if method=='all':
                        for idx in range(len(indices_row_final)):
                            row_rrs = df_rrs.loc[indices_row_final[idx]]
                            row_new = pd.concat([row, row_rrs])
                            row_new['INDEX_MATCH-UP'] = index_mu
                            row_new['INDEX_SPECTRA'] = idx
                            row_new['TIME_DIFF'] = time_diff[indices_sort_final[idx]]
                            df_new = df_new.append(row_new, ignore_index=True)
                    index_mu = index_mu + 1




    # # print(date_time.timestamp(),date_time,dt.utcfromtimestamp(date_time.timestamp()))
    # for date in indices_by_date:
    #     print(date,len(indices_by_date[date]))
    #
    # indices = indices_by_date['20230622']
    # df_day = df_rrs.loc[indices]
    # rrs_timestamps = np.array(rrs_timestamps)
    # utc_day = rrs_timestamps[indices]
    # for u in utc_day:
    #     print(dt.utcfromtimestamp(u))
    #
    # row_rrs = df_rrs.loc[indices[2]]
    # print(type(row_rrs))
    #
    # df_chla = pd.read_csv(file_chla,sep=';')
    # row_chla = df_chla.loc[6]
    # columns = df_chla.columns.tolist()+['INDEX_SPECTRA','TIME_DIFF']+df_rrs.columns.tolist()
    # df_new = pd.DataFrame(columns=columns)
    # row_new = pd.concat([row_chla,row_rrs]).to_dict()
    # row_new['INDEX_SPECTRA'] = 0
    # row_new['TIME_DIFF'] = 4.56
    # print(row_new)
    # print('---------------------------------')
    # df_new = df_new.append(row_new,ignore_index=True)
    # print(df_new)
    df_new.to_csv(file_out,sep=';')


def make_csv_from_odv(input_path,output_file):
    from odvlois.base import ODV_Struct
    import os
    import pandas as pd
    if not os.path.isdir(input_path):
        print(f'[ERROR] Input path {input_path} is not a valid directory')
        return
    output_path = os.path.dirname(output_file)
    if not os.path.isdir(output_path):
        try:
            os.mkdir(output_path)
        except:
            print(f'[ERROR] Output path {output_path} is not a valid writting directory')
            return
    if not output_file.endswith('.csv'):
        print(f'[ERROR] Output file (-o) should be a .csv file')
        return

    dataset_out = pd.DataFrame()
    for name in os.listdir(input_path):
        if not name.lower().endswith('.txt'):continue
        if name.lower().endswith('readme.txt'): continue
        print(f'[INFO] File name: {name}')
        odv = ODV_Struct(os.path.join(input_path, name))
        if odv.valid_odv:
            row = odv.extract_chl_as_row_dict()
            if row is not None:
                dataset_out = dataset_out.append(row, ignore_index=True)
    dataset_out.to_csv(output_file, sep=';')
    print(f'[INFO] Completed. Data saved to file: {output_file}')
def make_monocle_from_json_to_csv(input_path, output_file):
    print(f'[INFO] Reading file: {input_path}')
    import json
    fout = open(output_file,'w')
    first_line = None
    first_line_list = []
    f = open(input_path)
    jdata = json.load(f)
    for irow in jdata:
        if first_line is None:
            for key in irow:
                if key!='Rrs_corrected': first_line_list.append(key)
            first_line_list.append('DateTime')
            min_rrs = irow['wl0']
            max_rrs = irow['wln']
            n_rrs = irow['Rrs_len']
            step = ((max_rrs+1)-min_rrs)/n_rrs
            values = np.arange(min_rrs,(max_rrs+1),step)
            for v in values:
                first_line_list.append(f'Rrs_{v:.0f}')
            first_line = ";".join(first_line_list)
            fout.write(first_line)

        line = []
        for key in first_line_list:
            if key in irow: line.append(str(irow[key]))

        line.append(dt.utcfromtimestamp(float(irow['StartTimeUTC'])/1000).strftime('%Y-%m-%d %H:%M:%S'))
        rrs_values = irow['Rrs_corrected']
        for v in rrs_values:
            line.append(str(v))
        line = ";".join(line)
        fout.write('\n')
        fout.write(line)

    f.close()
    fout.close()


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

    if end_date < start_date:
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
    array_sum = None
    input_file_check = None
    nfiles = 0
    while date_here <= end_date:
        path_date = os.path.join(input_path, date_here.strftime('%Y'), date_here.strftime('%j'))
        name_file = f'X{date_here.strftime("%Y%j")}-chl-{region.lower()}-hr.nc'
        input_file = os.path.join(path_date, name_file)
        res = None
        if os.path.exists(input_file):
            print(f'[INFO] Working with file: {input_file}')
            input_file_check = input_file
            res, array_sum = get_info_aqua(input_file, array_sum)
            nfiles = nfiles + 1
        else:
            print(f'[WARNING] Input file: {input_file} does not exist. Skipping date...')

        if res is not None:
            if fout is None:
                fout = open(output_file, 'w')
                first_line = ';'.join(list(res.keys()))
                first_line = f'date;{first_line}'
                fout.write(first_line)
            line = ';'.join([str(x) for x in list(res.values())])
            line = f'{date_here.strftime("%Y-%m-%d")};{line}'
            fout.write('\n')
            fout.write(line)
        date_here = date_here + timedelta(hours=24)
    if fout is not None:
        fout.close()

    if nfiles > 0 and input_file_check is not None:
        array_porc = (array_sum / nfiles) * 100
        array_porc[array_sum == -999.0] = -999.0
        output_file_nc = output_file.replace('.csv', '.nc')
        from netCDF4 import Dataset
        dataset_in = Dataset(input_file_check)
        dataset_out = Dataset(output_file_nc, 'w')
        # copy global attributes all at once via dictionary
        dataset_out.setncatts(dataset_in.__dict__)
        for name, dimension in dataset_in.dimensions.items():
            dataset_out.createDimension(
                name, (len(dimension) if not dimension.isunlimited() else None))
        for name, variable in dataset_in.variables.items():
            if name == 'lat' or name == 'lon':
                fill_value = None
                if '_FillValue' in list(dataset_in.ncattrs()):
                    fill_value = variable._FillValue
                var = dataset_out.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value,
                                                 zlib=True,
                                                 shuffle=True, complevel=6)
                var.setncatts(dataset_in[name].__dict__)
                var[:] = dataset_in[name][:]

        var = dataset_out.createVariable('NAqua', 'i4', ('lat', 'lon'), fill_value=-999, zlib=True,
                                         shuffle=True, complevel=6)
        var[:] = array_sum[:]
        var = dataset_out.createVariable('PAqua', 'f4', ('lat', 'lon'), fill_value=-999, zlib=True,
                                         shuffle=True, complevel=6)
        var[:] = array_porc[:]
        dataset_out.start_date = start_date.strftime('%Y-%m-%d')
        dataset_out.end_date = end_date.strftime('%Y-%m-%d')
        dataset_in.close()
        dataset_out.close()

    print('[INFO]COMPLETED')


def get_info_aqua(file, array_sum):
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
    aqua_map = smask.copy()
    aqua_map[smask > 0] = 0
    aqua_map[smask == 2] = 1

    smask = smask[smask != -999]  ##only valid pixles
    res['ntotal'] = smask.shape[0]
    res['nvalid'] = np.count_nonzero(smask)
    nobs = np.zeros(smask.shape)
    nsensors = 0
    for val in flag_values:
        bitval = np.bitwise_and(smask, val)
        bitval[bitval > 0] = 1
        if np.sum(bitval) > 0:
            nsensors = nsensors + 1
        if val == 2:  ##aqua:
            aqua_array = bitval
        nobs = nobs + bitval
    res['nsensors'] = nsensors
    res['naqua'] = np.sum(aqua_array)
    aqua_combined = aqua_array + nobs
    aqua_combined[aqua_array == 0] = 0

    if array_sum is None:
        array_sum = aqua_map
    else:
        array_sum[np.logical_and(aqua_map == 1, array_sum >= 0)] = array_sum[np.logical_and(aqua_map == 1,
                                                                                            array_sum >= 0)] + 1
        array_sum[np.logical_and(aqua_map == 1, array_sum < 0)] = 1

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
    res['percent_degraded_pixels'] = (res['ndegraded'] / (res['nvalid'] - res['naqua_only'])) * 100
    res['percent_aqua_obs_degraded_pixels'] = (res['sum_obs_aqua_degraded_pixels'] / res[
        'sum_obs_degraded_pixels']) * 100
    dataset.close()

    return res, array_sum


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
    from netCDF4 import Dataset

    #file_csv = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/Baltic_CHLA_Valid_AllSources_2016-2023_rrs_chl.csv'
    #file_csv = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPS-PFT/results_chla/Chl_per_group_sup_up_11m_chla_extracts.csv'
    #file_csv = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/Baltic_CHLA_Valid_AllSources_1997-2023_FINAL_extracts_rrs_chl.csv'
    file_csv =  '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/Baltic_CHLA_Valid_AllSources_2016-2023_FINAL_OLCI_rrs_chl.csv'

    dir_extracts_rrs = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/extracts_rrs'
    dir_extracts_chl = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/extracts_chl'
    var_chl = 'satellite_CHL'
    col_index_extract_chl = 'IndexExtractChl'
    col_extract_chl = 'ExtractChl'
    col_index_extract_rrs = 'IndexExtractRrs'
    col_extract_rrs = 'ExtractRrs'

    protocol = '3x3'
    rrs_list = []
    if dir_extracts_rrs is not None and os.path.isdir(dir_extracts_rrs):
        for name in os.listdir(dir_extracts_rrs):
            if name.endswith('.nc'):
                file_nc = os.path.join(dir_extracts_rrs,name)
                dataset_check_rrs = Dataset(file_nc)
                if 'satellite_Rrs' in dataset_check_rrs.variables and 'satellite_bands' in dataset_check_rrs.variables:
                    array = dataset_check_rrs.variables['satellite_bands'][:]
                    rrs_list = [f'{x:.2f}' for x in array]
                    print(rrs_list)
                    rrs_list = [x[:-3] if x.endswith('.00') else x.replace('.','_') for x in rrs_list]
                    rrs_list = [f'RRS{x[:-1]}' if (x.endswith('0') and x.find('_')>0) else f'RRS{x}' for x in rrs_list]
                dataset_check_rrs.close()

                break
    if dir_extracts_chl is not None and os.path.isdir(dir_extracts_chl) and var_chl is not None:
        for name in os.listdir(dir_extracts_chl):
            if name.endswith('.nc'):
                file_nc = os.path.join(dir_extracts_chl,name)
                dataset_check_chl = Dataset(file_nc)
                if not var_chl in dataset_check_chl.variables:
                    var_chl = None
                dataset_check_chl.close()
                break
    print(rrs_list)
    print(var_chl)


    file_out = f'{file_csv[:-4]}_{protocol}.csv'
    if protocol == '1x1':
        rc_ini = 12
        rc_fin = 12
    elif protocol == '3x3':
        rc_ini = 11
        rc_fin = 13

    df = pd.read_csv(file_csv, ';')
    col_names = df.columns.tolist()
    n_bands = len(rrs_list) if len(rrs_list)>0 else 0
    if n_bands>0:
        rrs_list_valid = [f'{x}_NVALID' for x in rrs_list]
        col_names = col_names + rrs_list +rrs_list_valid
    if var_chl is not None:
        col_names = col_names + [var_chl,f'{var_chl}_NVALID']

    print(col_names)
    first_line = ';'.join(col_names)
    fw = open(file_out, 'w')
    fw.write(first_line)
    for index, row in df.iterrows():
        if index%100==0: print('INDEX: ', index)
        lrow = [str(x).strip() for x in list(row)]
        line = ';'.join(lrow)

        ##rrs
        if n_bands>0:
            index_extract = row[col_index_extract_rrs]
            values = [-999] * n_bands
            nvalid = [-999] * n_bands
            if index_extract>=0:
                name_extract = row[col_extract_rrs]
                extract_file = os.path.join(dir_extracts_rrs,f'extract_{name_extract}')
                dataset = Dataset(extract_file)
                for ivar in range(n_bands):
                    rrs_data = np.array(dataset.variables['satellite_Rrs'][0, ivar, rc_ini:rc_fin+1,rc_ini:rc_fin+1])
                    rrs_data_c = rrs_data[rrs_data!=-999.0]
                    nvalid[ivar] = len(rrs_data_c)
                    if len(rrs_data_c)>0:
                        values[ivar] = np.median(rrs_data_c)
                    else:
                        values[ivar] = -999.0
                dataset.close()
            for x in values:
                line = f'{line};{x}'
            for x in nvalid:
                line = f'{line};{x}'

        # chla
        if var_chl is not None:
            index_extract = row[col_index_extract_chl]
            values = [-999.0]*2
            if index_extract >= 0:
                name_extract = row[col_extract_chl]
                extract_file = os.path.join(dir_extracts_chl, f'extract_{name_extract}')
                dataset = Dataset(extract_file)
                chl_data = np.array(dataset.variables[var_chl][0, rc_ini:rc_fin + 1, rc_ini:rc_fin + 1])
                chl_data_c = chl_data[chl_data != -999.0]
                values[1] = len(chl_data_c)
                if len(chl_data_c) > 0:
                    values[0] = np.median(chl_data_c)
                else:
                    values[0] = -999.0
                dataset.close()
            for ivar in range(len(values)):
                line = f'{line};{values[ivar]}'
        fw.write('\n')
        fw.write(line)
        #print(line)
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
    # from BSC_QAA import bsc_qaa_EUMETSAT as qaa
    # res = qaa.qaa([0.000169934,0.000196819,0.000117161,0.000317291,0.000656212,0.000466968],[412,443,490,510,560,665])
    # print(res)

    import pandas as pd
    from datetime import datetime as dt
    file_ref = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CSV_MATCH-UPS/Baltic_CHLA_Valid_AllSources_1997-2023_extracts_match-ups_3x3_all.csv'


    dref = pd.read_csv(file_ref,sep=';')
    datetime_ref = dref['DATETIME']
    chla_ref = dref['INSITU-CHLA']
    lat_ref = dref['LATITUDE']
    nvalues = len(dref.index)
    out = [''] * nvalues

    file_before = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/insitu_sources/original_sources/Baltic_CHLA_Valid_WithBrando2021_1997-2021_NOREPEATED.csv'
    dbefore = pd.read_csv(file_before,sep=';')
    datetime_before = dbefore['DATETIME']
    chla_before = dbefore['CHLA']
    lat_before = dbefore['LATITUDE']
    nbefore = len(dbefore.index)
    data_before = {}
    nbefore_count  = 0
    for idx in range(nbefore):
        dt_here = str(datetime_before[idx])
        chl_here = str(chla_before[idx])
        lat_here = str(lat_before[idx])
        if not dt_here in data_before.keys():
            data_before[dt_here]= {lat_here:{chl_here:False}}
            nbefore_count = nbefore_count + 1
        else:
            data_before[dt_here][lat_here] = {chl_here:False}

            nbefore_count = nbefore_count + 1

    print(nbefore_count,' shoud be 8531')

    nold = 0
    for idx in range(nvalues):
        out[idx] = 'NEW'
        dt_here = str(datetime_ref[idx])
        chl_here = str(chla_ref[idx])
        lat_here = str(lat_ref[idx])
        if dt_here in data_before.keys() and lat_here in data_before[dt_here].keys() and chl_here in data_before[dt_here][lat_here].keys():
            data_before[dt_here][lat_here][chl_here] = True
            out[idx]='OLD'
            nold = nold + 1

        if dt_here=='2017-01-18T10:12:41' and chl_here=='1.21':
            data_before[dt_here]['55.27'][chl_here] = True
            out[idx] = 'OLD'
            nold = nold + 1
        if dt_here=='2017-01-18T12:00:51' and chl_here=='0.84':
            data_before[dt_here]['55.66'][chl_here] = True
            out[idx] = 'OLD'
            nold = nold + 1


    print('NOLD: ',nold)


    nfalse = 0
    ntrue = 0
    for dt_here in data_before.keys():
        for lat_here in data_before[dt_here].keys():
            for chl_here in data_before[dt_here][lat_here].keys():
                if not data_before[dt_here][lat_here][chl_here]:
                    print(dt_here, lat_here, chl_here)
                    nfalse = nfalse + 1
                else:
                    ntrue = ntrue + 1
    ntot = nfalse + ntrue
    print(ntrue,nfalse,ntot,'shoud be 8531')

    file_ref_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CSV_MATCH-UPS/SourceN.csv'
    fout = open(file_ref_out, 'w')
    fout.write('SOURCE_N')
    for o in out:
        fout.write('\n')
        fout.write(o)
    fout.close()



    # file_pml = '/mnt/c/Users/Luis/Downloads/ESACCI-OC-L3S-RRS-MERGED-1D_DAILY_4km_GEO_PML_RRS-20240327-fv6.0.nc'
    # file_cmems = '/mnt/c/Users/Luis/Downloads/20240327_c3s_obs-oc_glo_bgc-reflectance_my_l3-multi-4km_P1D.nc'
    # bands_pml = ['Rrs_412','Rrs_443','Rrs_490','Rrs_510','Rrs_560','Rrs_665']
    # bands_cmems = ['RRS412','RRS443','RRS490','RRS510','RRS560','RRS665']
    # from netCDF4 import Dataset
    # import numpy as np
    # dpml = Dataset(file_pml)
    # dcmems = Dataset(file_cmems)
    # for iband,band_pml in enumerate(bands_pml):
    #     band_cmems = bands_cmems[iband]
    #     array_pml = dpml.variables[band_pml][:]
    #     array_cmems = dcmems.variables[band_cmems][:]
    #     diff = array_pml-array_cmems
    #     print(np.mean(diff),np.mean(array_pml),np.mean(array_cmems))
    # dpml.close()
    # dcmems.close()



    # from odvlois.base import ODV_Struct
    # import os
    # import pandas as pd
    # input_path = '/mnt/c/Users/Luis/ownCloud/COP2-OC-TAC/BAL_Evolutions/Baltic_insitu_2024/order_67894_unrestricted'
    # output_path = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/BalticSeaDataNet.csv'
    # dataset_out = pd.DataFrame()
    # for name in os.listdir(input_path):
    #     if not name.endswith('.txt'):continue
    #     if name.endswith('README.txt'):continue
    #     print(f'[INFO] File name: {name}')
    #     odv = ODV_Struct(os.path.join(input_path,name))
    #     if odv.valid_odv:
    #         row = odv.extract_chl_as_row_dict()
    #         if row is not None:
    #             dataset_out = dataset_out.append(row,ignore_index=True)
    # dataset_out.to_csv(output_path,sep=';')

    # os.save_to_csv_file(
    #     '/mnt/c/Users/Luis/ownCloud/COP2-OC-TAC/BAL_Evolutions/Baltic_insitu_2024/order_67894_unrestricted/test_data.csv')
    # dir_extracts = '/mnt/c/DATA_LUIS/DOORS_WORK/extracts/certo_msi_danube'
    # from netCDF4 import Dataset
    # from datetime import datetime as dt
    # for name in os.listdir(dir_extracts):
    #     file_extract = os.path.join(dir_extracts,name)
    #     dataset = Dataset(file_extract)
    #     sat_time =float(dataset.variables['satellite_time'][0])
    #     print(dt.utcfromtimestamp(sat_time))
    #     dataset.close()

    # dir_mdb = '/mnt/c/DATA_LUIS/DOORS_WORK/MDBs'
    # for name in os.listdir(dir_mdb):
    #     file_nc = os.path.join(dir_mdb, name)
    #     file_out = os.path.join(dir_mdb, f'{name[:-3]}.csv')
    #     print(file_out)
    #     fout = open(file_out, 'w')
    #     fout.write('SatelliteId;InsituId;SatelliteTimeStamp;InsituTimeStamp;SatelliteTime;InsituTime;TimeDifference')
    #
    #     dataset = Dataset(file_nc)
    #     sat_time = np.array(dataset.variables['satellite_time'])
    #     ins_time = np.array(dataset.variables['insitu_time'])
    #     # fill_value = dataset.variables['insitu_time']._FillValue
    #     fill_value = 9.969209968386869e+36
    #     time_diff = np.array(dataset.variables['time_difference'])
    #     for satellite_id in range(sat_time.shape[0]):
    #         sat_stamp = sat_time[satellite_id]
    #         for insitu_id in range(50):
    #             ins_stamp = ins_time[satellite_id][insitu_id]
    #             if ins_stamp != fill_value:
    #                 sat_time_stamp = dt.utcfromtimestamp(sat_stamp)
    #                 ins_time_stamp = dt.utcfromtimestamp(ins_stamp)
    #                 time_diff_here = time_diff[satellite_id][insitu_id]
    #                 time_diff_computed = abs((sat_time_stamp - ins_time_stamp).total_seconds())
    #                 time_diff_computed_2 = abs(sat_stamp - ins_stamp)
    #                 # print(sat_time_stamp,ins_time_stamp,time_diff_here,time_diff_computed,time_diff_computed_2)
    #                 line = f'{satellite_id};{insitu_id};{sat_stamp};{ins_stamp};{sat_time_stamp.strftime("%Y-%m-%d %H:%M:%S")};{ins_time_stamp.strftime("%Y-%m-%d %H:%M:%S")};{time_diff_here}'
    #                 tal = abs(time_diff_here - time_diff_computed)
    #                 cual = abs(time_diff_here - time_diff_computed_2)
    #                 print(tal, cual)
    #                 fout.write('\n')
    #                 fout.write(line)
    #     dataset.close()
    #     fout.close()

    # dir_sources = '/mnt/c/DATA_LUIS/OCTACWORK'
    # dir_bad = '/mnt/c/DATA_LUIS/OCTACWORK/BAD'
    # dir_check = '/mnt/c/DATA_LUIS/OCTACWORK/CHECK'

    # dir_sources = '/store/COP2-OC-TAC/arc/sources/20240421'
    # dir_bad = '/store/COP2-OC-TAC/arc/bad'
    # dir_check = '/store/COP2-OC-TAC/arc/CHECK'
    #
    # import zipfile
    # from netCDF4 import Dataset
    # for name in os.listdir(dir_sources):
    #     if not name.endswith('.zip'):
    #         continue
    #     # if not name=='S3B_OL_2_WFR____20240421T000327_20240421T000627_20240421T015954_0180_092_116_1800_MAR_O_NR_003.SEN3':
    #     #     continue
    #
    #     #name = 'S3A_OL_2_WFR____20240421T141019_20240421T141319_20240421T160835_0179_111_267_1800_MAR_O_NR_003.SEN3.zip'
    #     file = os.path.join(dir_sources,name)
    #     file_bad = os.path.join(dir_bad,name)
    #
    #     zip_ref = zipfile.ZipFile(file, 'r')
    #     name_ref = f'{name[:-4]}/'
    #     nfiles = 0
    #     for name_here in zip_ref.namelist():
    #         if name_here.startswith(name_ref):
    #             nfiles = nfiles + 1
    #     if nfiles<33:
    #         print(name,'->',nfiles)
    #         os.rename(file,file_bad)
    #     folder_out = os.path.join(dir_check,name[:-4])
    #     if not os.path.exists(folder_out):
    #         zip_ref.extractall(dir_check)
    #     zip_ref.close()
    #     valid = True
    #     for name_out in os.listdir(folder_out):
    #         if not name_out.endswith('nc'):
    #             continue
    #         file_nc = os.path.join(folder_out,name_out)
    #         #print(file_nc)
    #         try:
    #             dataset = Dataset(file_nc,'r')
    #             dataset.close()
    #         except:
    #             valid = False
    #             break
    #     if not valid:
    #         print(f'Folder out: {folder_out} is not valid')
    #         os.rename(file, file_bad)

    # from netCDF4 import Dataset
    # file = '/mnt/c/DATA_LUIS/OCTAC_WORK/CHECK_SENSOR_MASK/X2022130-chl-med-hr.nc'
    # dataset = Dataset(file, 'r')
    # smask = np.array(dataset.variables['SENSORMASK'][:])
    # print(smask.min(), smask.max())


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

    return True


def do_extract_csv_from_extracts():
    import pandas as pd
    from netCDF4 import Dataset
    type = 'rrs_3x3'
    param = type.split('_')[0]
    window = type.split('_')[1]

    # file_csv = f'/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPSv6_3/insitu_{param}/Chl_in_situ_dataset_cleaned_extra_{param}.csv'
    # file_out = f'/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPSv6_3/insitu_{param}/Chl_in_situ_dataset_cleaned_extra_{type}.csv'
    # dir_extracts = f'/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MATCH-UPSv6_3/extracts_orig_cciV6_{param}'

    file_csv = f'/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/Baltic_CHLA_Valid_AllSources_1997-2023_extracts_{param}.csv'
    file_out = f'/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/Baltic_CHLA_Valid_AllSources_1997-2023_extracts_{type}.csv'
    dir_extracts = f'/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/extracts_{param}'



    if param=='chl':
        #variable_list = ['CHL','MICRO','MICRO_BIAS','MICRO_RMSE','NANO','NANO_BIAS','NANO_RMSE','PICO','PICO_BIAS','PICO_RMSE']
        variable_list = ['CHL']
    elif param=='sst':
        variable_list = ['analysed_st','analysis_error','mask','observation_max','observation_min','observation_num','observation_std','sea_ice_fraction']


    df = pd.read_csv(file_csv, sep=';')
    if type=='rrs_1x1':
        lines_out = ['RRS412;RRS443;RRS490;RRS510;RRS560;RRS665']
        line_no_valid = ';'.join(['-999.0'] * 6)
    elif type=='rrs_3x3':
        lines_out = ['RRS412;NV412;RRS443;NV443;RRS490;NV490;RRS510;NV510;RRS560;NV560;RRS665;NV665']
        line_no_valid = ';'.join(['-999.0'] * 12)
    else:
        if window=='1x1':
            lines_out = [';'.join(variable_list)]
            line_no_valid = ';'.join(['-999.0'] * len(variable_list))
        elif window=='3x3':
            lines_h = []
            for var in variable_list:
                lines_h.append(var)
                lines_h.append(f'NV_{var}')
            lines_out = [';'.join(lines_h)]
            line_no_valid = ';'.join(['-999.0'] * len(lines_h))

    for index, row in df.iterrows():
        line_out = None
        if index%100 ==0:print(f'Row: {index}/{len(df.index)}')
        # print(row)
        extract = row['Extract']

        if not isinstance(extract, str):
            lines_out.append(line_no_valid)
            continue
        file_extract = os.path.join(dir_extracts, f'extract_{extract}')
        if not os.path.exists(file_extract):
            print(f'File extract: extract_{extract} is not available. Please review')
            break

        dataset = Dataset(file_extract)

        if param=='rrs':
            rrs = dataset.variables['satellite_Rrs']
            for iband in range(rrs.shape[1]):
                # 1x1
                if window=='1x1':
                    rrs_val_c = rrs[0, iband, 12, 12]
                    if np.ma.is_masked(rrs_val_c):
                        rrs_val = -999.0
                    else:
                        rrs_val = rrs_val_c
                    if line_out is None:
                        line_out = f'{rrs_val}'
                    else:
                        line_out = f'{line_out};{rrs_val}'
                # 3x3
                elif window=='3x3':
                    rrs_here = rrs[0, iband, 11:14, 11:14]

                    shape_before = rrs_here.shape
                    rrs_here = rrs_here[~rrs_here.mask]

                    if len(rrs_here) >= 1:
                        #print(shape_before, '-->', len(rrs_here))
                        rrs_val = np.mean(rrs_here)
                    else:
                        rrs_val = -999.0
                    if line_out is None:
                        line_out = f'{rrs_val};{len(rrs_here)}'
                    else:
                        line_out = f'{line_out};{rrs_val};{len(rrs_here)}'
        else:
            for var in variable_list:
                array = dataset.variables[f'satellite_{var}']
                if window=='1x1':
                    param_val_c = array[0, 12, 12]
                    if np.ma.is_masked(param_val_c):
                        param_val = -999.0
                    else:
                        #print(index, ':',param_val_c)
                        param_val = param_val_c
                    if line_out is None:
                        line_out = f'{param_val}'
                    else:
                        line_out = f'{line_out};{param_val}'
                elif window=='3x3':
                    param_here = array[0, 11:14, 11:14]
                    shape_before = param_here.shape
                    param_here = param_here[~param_here.mask]
                    if len(param_here) >= 1:
                        #print(index,':',shape_before, '-->', len(param_here))
                        param_val = np.mean(param_here)
                    else:
                        param_val = -999.0
                    if line_out is None:
                        line_out = f'{param_val};{len(param_here)}'
                    else:
                        line_out = f'{line_out};{param_val};{len(param_here)}'

        lines_out.append(line_out)
        dataset.close()



    print('[INFO] Writting...')
    fr = open(file_csv, 'r')
    fw = open(file_out, 'w')
    index = 0
    for line in fr:
        line_out = lines_out[index]
        line_out = f'{line.strip()};{line_out}'
        # print(line_out)
        fw.write(line_out)
        fw.write('\n')
        index = index + 1
    fr.close()
    fw.close()

    print(f'[INFO] Completed')


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


def do_coverage_cmems_certo():
    from datetime import datetime as dt
    from netCDF4 import Dataset
    start_date = dt.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.strptime(args.end_date, '%Y-%m-%d')
    date_here = start_date
    nhours = 24
    if args.interval:
        nhours = int(args.interval) * 24
    file_out = args.output
    dir_out_base = os.path.dirname(file_out)
    if not os.path.exists(dir_out_base):
        print(f'[ERROR] Output dir: {dir_out_base} does not exist')
        return
    if not os.path.basename(file_out).endswith('.nc'):
        print(f'[ERROR] Output file shoud be a NetCDF file')
        return
    # SERVER
    if args.input == 'SERVER' or args.input == 'SCMEMS' or args.input == 'SCERTO':
        dir_cmems_orig = '/dst04-data1/OC/OLCI/daily_v202311_bc'
        dir_certo_orig = '/store3/DOORS/CERTO_SOURCES'
    # LOCAL
    elif args.input == 'LOCAL' or args.input == 'LCMEMS' or args.input == 'LCERTO':
        dir_cmems_orig = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/SOURCES_CMEMS'
        dir_certo_orig = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/SOURCES_CERTO'
    else:
        return

    wl_cmems = ['400', '412_5', '442_5', '490', '510', '560', '620', '665', '673_75', '681_25', '708_75', '753_75',
                '778_75', '865', '885', '1020']
    wl_certo = ['400', '412', '443', '490', '510', '560', '620', '665', '674', '681', '709', '754', '779', '865',
                '885', '1020']
    all_cmems = ['chl', 'kd490', 'tsmnn']
    all_certo = ['blended_chla', 'blended_chla_from_predominant_owt', 'blended_chla_top_2_weighted',
                 'blended_chla_top_3_weighted']
    stats = ['N', 'avg', 'std', 'min', 'max', 'median', 'p25', 'p75']
    stats_nan = {}
    for stat in stats:
        stats_nan[stat] = -999.0
    stats_nan['N'] = 0

    ndays = (end_date - start_date).days + 1
    print('[INFO] NDays is: ', ndays)

    if args.input.endswith('CMEMS'):
        do_spatial_coverage_cmems(dir_cmems_orig, start_date, end_date, wl_cmems, all_cmems, file_out)
        return
    if args.input.endswith('CERTO'):
        do_spatial_coverage_certo(dir_certo_orig, start_date, end_date, wl_certo, all_certo, file_out)
        return

    ncout = Dataset(file_out, 'w', format='NETCDF4')
    ncout.createDimension('time', ndays)
    ncout.createVariable('time', 'f8', ('time',), fill_value=-999.0, zlib=True, complevel=6)
    # time_array = np.zeros((ndays,),dtype=np.float64)
    for wl in wl_cmems:
        for stat in stats:
            name_var = f'CMEMS_{wl}_{stat}'
            ncout.createVariable(name_var, 'f4', ('time',), fill_value=-999.0, zlib=True, complevel=6)
    for wl in wl_certo:
        for stat in stats:
            name_var = f'CERTO_{wl}_{stat}'
            ncout.createVariable(name_var, 'f4', ('time',), fill_value=-999.0, zlib=True, complevel=6)
    for param in all_cmems:
        for stat in stats:
            name_var = f'CMEMS_{param}_{stat}'
            ncout.createVariable(name_var, 'f4', ('time',), fill_value=-999.0, zlib=True, complevel=6)
    for param in all_certo:
        for stat in stats:
            name_var = f'CERTO_{param}_{stat}'
            ncout.createVariable(name_var, 'f4', ('time',), fill_value=-999.0, zlib=True, complevel=6)
    name_var = 'CERTO_owt_dominant_OWT'
    ncout.createVariable(name_var, 'i4', ('time',), fill_value=-999.0, zlib=True, complevel=6)

    iday = 0
    while date_here <= end_date:
        if args.verbose:
            print(f'[INFO] Worknig for date: {date_here}...')
        ncout.variables['time'][iday] = float(date_here.replace(hour=12, tzinfo=pytz.utc).timestamp())
        year = date_here.strftime('%Y')
        jday = date_here.strftime('%j')
        dir_cmems = os.path.join(dir_cmems_orig, year, jday)
        dir_certo = os.path.join(dir_certo_orig, year, jday)
        for wlc in wl_cmems:
            file_cmems = os.path.join(dir_cmems, f'O{year}{jday}-rrs{wlc}-bs-fr.nc')
            if os.path.exists(file_cmems):
                dataset_cmems = Dataset(file_cmems)
                stats = get_stats_variable(dataset_cmems, f'RRS{wlc}', False, stats_nan)
                ncout = assign_data_variable(ncout, iday, f'CMEMS_{wlc}_', stats)
                dataset_cmems.close()
            else:
                ncout = assign_data_variable(ncout, iday, f'CMEMS_{wlc}_', stats_nan)
        for param in all_cmems:
            file_cmems = os.path.join(dir_cmems, f'O{year}{jday}-{param}-bs-fr.nc')
            if os.path.exists(file_cmems):
                dataset_cmems = Dataset(file_cmems)
                stats = get_stats_variable(dataset_cmems, f'{param.upper()}', False, stats_nan)
                ncout = assign_data_variable(ncout, iday, f'CMEMS_{param}_', stats)
                dataset_cmems.close()
            else:
                ncout = assign_data_variable(ncout, iday, f'CMEMS_{param}_', stats_nan)
        name_certo = f'CERTO_blk_{date_here.strftime("%Y%m%d")}_OLCI_RES300__final_l3_product.nc'
        file_certo = os.path.join(dir_certo, name_certo)
        if not os.path.exists(file_certo):
            try:
                dir_certo_year = os.path.join(dir_certo_orig, year)
                if not os.path.isdir(dir_certo_year):
                    os.mkdir(dir_certo_year)
                dir_certo_jday = os.path.join(dir_certo_year, jday)
                if not os.path.isdir(dir_certo_jday):
                    os.mkdir(dir_certo_jday)
                download_olci_source(name_certo, file_certo)
            except:
                pass
        if os.path.exists(file_certo) and os.stat(file_certo).st_size == 0:
            os.remove(file_certo)
        if os.path.exists(file_certo):
            dataset_certo = Dataset(file_certo)
            for wlc in wl_certo:
                stats = get_stats_variable(dataset_certo, f'Rw{wlc}_rep', False, stats_nan)
                ncout = assign_data_variable(ncout, iday, f'CERTO_{wlc}_', stats)
            for param in all_certo:
                stats = get_stats_variable(dataset_certo, param, False, stats_nan)
                ncout = assign_data_variable(ncout, iday, f'CERTO_{param}_', stats)
            stats = get_stats_variable(dataset_certo, 'owt_dominant_OWT', True, stats_nan)
            ncout.variables['CERTO_owt_dominant_OWT'][iday] = stats['mode']
            dataset_certo.close()
        else:
            for wlc in wl_certo:
                ncout = assign_data_variable(ncout, iday, f'CERTO_{wlc}_', stats_nan)
            for param in all_certo:
                ncout = assign_data_variable(ncout, iday, f'CERTO_{param}_', stats_nan)
            ncout.variables['CERTO_owt_dominant_OWT'][iday] = -999.0
        iday = iday + 1
        date_here = date_here + timedelta(hours=nhours)
    ncout.close()


def do_spatial_coverage_certo(dir_certo_orig, start_date, end_date, wl_certo, all_certo, file_out):
    from netCDF4 import Dataset

    date_here = start_date
    nhours = 24
    if args.interval:
        nhours = int(args.interval) * 24

    ##gettting file ref
    file_ref = None
    while date_here <= end_date:
        if args.verbose:
            print(f'[INFO] Getting file ref with date: {date_here}...')
        year = date_here.strftime('%Y')
        jday = date_here.strftime('%j')
        dir_certo = os.path.join(dir_certo_orig, year, jday)
        name_certo = f'CERTO_blk_{date_here.strftime("%Y%m%d")}_OLCI_RES300__final_l3_product.nc'
        file_certo = os.path.join(dir_certo, name_certo)
        if os.path.exists(file_certo):
            file_ref = file_certo
            break
        if file_ref is not None:
            break
        date_here = date_here + timedelta(hours=nhours)

    if file_ref is None:
        return

    ##start dataset out from Dataset Ref
    datasetRef = Dataset(file_ref)
    n_lat = len(datasetRef.dimensions['lat'])
    n_lon = len(datasetRef.dimensions['lon'])
    lat_array = np.array(datasetRef.variables['lat'])
    lon_array = np.array(datasetRef.variables['lon'])
    ncout = Dataset(file_out, 'w', format='NETCDF4')
    ncout.createDimension('lat', n_lat)
    ncout.createDimension('lon', n_lon)
    var_lat = ncout.createVariable('lat', 'f4', ('lat',), fill_value=-999.0, zlib=True, complevel=6)
    var_lon = ncout.createVariable('lon', 'f4', ('lon',), fill_value=-999.0, zlib=True, complevel=6)
    var_lat.setncatts(datasetRef['lat'].__dict__)
    var_lon.setncatts(datasetRef['lon'].__dict__)
    var_lat[:] = lat_array[:]
    var_lon[:] = lon_array[:]
    fill_value = datasetRef[all_certo[0]]._FillValue
    datasetRef.close()

    ##Adding variables
    ndays_by_var = {}
    for wl in wl_certo:
        name_var = f'Rw{wl}_rep'
        ndays_by_var[name_var] = 0
        ncout.createVariable(name_var, 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        ncout[name_var][:, :] = np.zeros((n_lat, n_lon))
    for param in all_certo:
        name_var = f'{param}'
        ndays_by_var[name_var] = 0
        ncout.createVariable(name_var, 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        ncout[name_var][:, :] = np.zeros((n_lat, n_lon))

    ##checking coverage
    date_here = start_date
    while date_here <= end_date:
        if args.verbose:
            print(f'[INFO] Checking coverage for date: {date_here}')
        year = date_here.strftime('%Y')
        jday = date_here.strftime('%j')
        dir_certo = os.path.join(dir_certo_orig, year, jday)
        name_certo = f'CERTO_blk_{date_here.strftime("%Y%m%d")}_OLCI_RES300__final_l3_product.nc'
        file_certo = os.path.join(dir_certo, name_certo)

        if os.path.exists(file_certo):
            datasetCERTO = Dataset(file_certo)
            for wlc in wl_certo:
                name_var = f'Rw{wlc}_rep'
                ndays_by_var[name_var] = ndays_by_var[name_var] + 1
                array = np.squeeze(np.array(datasetCERTO.variables[name_var]))
                check = np.array(ncout.variables[name_var])
                check[array != fill_value] = check[array != fill_value] + 1
                ncout.variables[name_var][:, :] = check[:, :]

            for param in all_certo:
                name_var = f'{param}'
                ndays_by_var[name_var] = ndays_by_var[name_var] + 1
                array = np.squeeze(np.array(datasetCERTO.variables[name_var]))
                check = np.array(ncout.variables[name_var])
                check[array != fill_value] = check[array != fill_value] + 1
                ncout.variables[name_var][:, :] = check[:, :]
            datasetCERTO.close()
        date_here = date_here + timedelta(hours=nhours)
    ncout.setncatts(ndays_by_var)
    ncout.close()
    print('COMPLETED')


def do_spatial_coverage_cmems(dir_cmems_orig, start_date, end_date, wl_cmems, all_cmems, file_out):
    from netCDF4 import Dataset

    date_here = start_date
    nhours = 24
    if args.interval:
        nhours = int(args.interval) * 24

    ##gettting file ref
    file_ref = None
    while date_here <= end_date:
        if args.verbose:
            print(f'[INFO] Getting file ref with date: {date_here}...')
        year = date_here.strftime('%Y')
        jday = date_here.strftime('%j')
        dir_cmems = os.path.join(dir_cmems_orig, year, jday)
        for wlc in wl_cmems:
            file_cmems = os.path.join(dir_cmems, f'O{year}{jday}-rrs{wlc}-bs-fr.nc')
            if os.path.exists(file_cmems):
                file_ref = file_cmems
                break
        if file_ref is not None:
            break
        date_here = date_here + timedelta(hours=nhours)

    if file_ref is None:
        return

    ##start dataset out from Dataset Ref
    datasetRef = Dataset(file_ref)
    n_lat = len(datasetRef.dimensions['lat'])
    n_lon = len(datasetRef.dimensions['lon'])
    lat_array = np.array(datasetRef.variables['lat'])
    lon_array = np.array(datasetRef.variables['lon'])
    ncout = Dataset(file_out, 'w', format='NETCDF4')
    ncout.createDimension('lat', n_lat)
    ncout.createDimension('lon', n_lon)
    var_lat = ncout.createVariable('lat', 'f4', ('lat',), fill_value=-999.0, zlib=True, complevel=6)
    var_lon = ncout.createVariable('lon', 'f4', ('lon',), fill_value=-999.0, zlib=True, complevel=6)
    var_lat.setncatts(datasetRef['lat'].__dict__)
    var_lon.setncatts(datasetRef['lon'].__dict__)
    var_lat[:] = lat_array[:]
    var_lon[:] = lon_array[:]
    datasetRef.close()

    ##Adding variables
    ndays_by_var = {}
    for wl in wl_cmems:
        name_var = f'RRS{wl}'
        ndays_by_var[name_var] = 0
        ncout.createVariable(name_var, 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        ncout[name_var][:, :] = np.zeros((n_lat, n_lon))
    for param in all_cmems:
        name_var = f'{param.upper()}'
        ndays_by_var[name_var] = 0
        ncout.createVariable(name_var, 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        ncout[name_var][:, :] = np.zeros((n_lat, n_lon))

    ##checking coverage
    date_here = start_date
    while date_here <= end_date:
        if args.verbose:
            print(f'[INFO] Checking coverage for date: {date_here}')
        year = date_here.strftime('%Y')
        jday = date_here.strftime('%j')
        dir_cmems = os.path.join(dir_cmems_orig, year, jday)
        for wlc in wl_cmems:
            name_var = f'RRS{wlc}'
            file_cmems = os.path.join(dir_cmems, f'O{year}{jday}-rrs{wlc}-bs-fr.nc')
            if os.path.exists(file_cmems):
                ndays_by_var[name_var] = ndays_by_var[name_var] + 1
                datasetCMEMS = Dataset(file_cmems)
                array = np.squeeze(np.array(datasetCMEMS.variables[name_var]))
                check = np.array(ncout.variables[name_var])
                check[array != -999.0] = check[array != -999.0] + 1
                ncout.variables[name_var][:, :] = check[:, :]
                datasetCMEMS.close()
        for param in all_cmems:
            name_var = f'{param.upper()}'
            file_cmems = os.path.join(dir_cmems, f'O{year}{jday}-{param}-bs-fr.nc')
            if os.path.exists(file_cmems):
                ndays_by_var[name_var] = ndays_by_var[name_var] + 1
                datasetCMEMS = Dataset(file_cmems)
                array = np.squeeze(np.array(datasetCMEMS.variables[name_var]))
                check = np.array(ncout.variables[name_var])
                check[array != -999.0] = check[array != -999.0] + 1
                ncout.variables[name_var][:, :] = check[:, :]
                datasetCMEMS.close()
        date_here = date_here + timedelta(hours=nhours)
    ncout.setncatts(ndays_by_var)
    ncout.close()
    print('COMPLETED')


def assign_data_variable(ncout, iday, prename, stats):
    for stat in stats:
        name_var = f'{prename}{stat}'
        ncout.variables[name_var][iday] = stats[stat]
    return ncout


def get_stats_variable(dataset, variable, compute_mode, stats_nan):
    array_c = np.array(dataset.variables[variable])
    array_v = array_c[array_c != dataset.variables[variable]._FillValue]
    if len(array_v) == 0:
        if not compute_mode:
            return stats_nan
        if compute_mode:
            stats = {
                'mode': -999.0
            }
            return stats
    if not compute_mode:
        stats = {
            'N': array_v.shape[0],
            'avg': np.mean(array_v),
            'std': np.std(array_v),
            'min': np.min(array_v),
            'max': np.max(array_v),
            'median': np.median(array_v),
            'p25': np.percentile(array_v, 25),
            'p75': np.percentile(array_v, 75)
        }
    if compute_mode:
        array_v = array_v.astype(np.int16)
        stats = {
            'mode': np.argmax(np.bincount(array_v))
        }
    return stats


def do_comparison_cmems_certo():
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np
    from datetime import datetime as dt
    do_grid = False
    do_global = False
    do_add_param = False
    do_prepare_plot = False
    do_prepare_plot_all = False
    if args.input == 'GRID':
        do_grid = True
    if args.input.startswith('GLOBAL%'):
        do_global = True
    if args.input == 'PREPAREPLOT':
        do_prepare_plot = True
    if args.input == 'PREPAREPLOTALL':
        do_prepare_plot_all = True
    if args.input == 'ADDPARAM':
        do_add_param = True

    if do_grid:
        file_orig = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/Grid_CMEMS_OLCI_BS_OLD.csv'
        file_grid_cmems = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/Grid_CMEMS_OLCI_BS.csv'
        file_grid_certo = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/Grid_CERTO_OLCI_BS.csv'
        file_input_certo = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/CERTO_blk_20170629_OLCI_RES300__final_l3_product.nc'
        dataset = Dataset(file_input_certo)
        lat_array = np.array(dataset.variables['lat'])
        lon_array = np.array(dataset.variables['lon'])
        dataset.close()
        # nlat = len(lat_array)
        # nlon = len(lon_array)

        grid = pd.read_csv(file_orig, sep=';')
        lat_grid = grid['Latitude'].to_numpy()
        lon_grid = grid['Longitude'].to_numpy()
        y_grid = grid['YCMEMS'].to_numpy()
        x_grid = grid['XCMEMS'].to_numpy()

        fCerto = open(file_grid_certo, 'w')
        line = f'Index;YCERTO;XCERTO;Latitude;Longitude'
        fCerto.write(line)

        fCmems = open(file_grid_cmems, 'w')
        line = f'Index;YCMEMS;XCMEMS;Latitude;Longitude'
        fCmems.write(line)

        all_dist = []
        # import geopy.distance

        for idx in range(len(lat_grid)):
            index = idx + 1
            lat_here = lat_grid[idx]
            lon_here = lon_grid[idx]
            y_here = y_grid[idx]
            x_here = x_grid[idx]
            y = np.argmin(np.abs(lat_array - lat_here))
            x = np.argmin(np.abs(lon_array - lon_here))
            lat_altro = lat_array[y]
            lon_altro = lon_array[x]

            coord_1 = (lat_here, lon_here)
            coord_2 = (lat_altro, lon_altro)
            # dist = geopy.distance.geodesic(coord_1, coord_2).m
            dist = abs(lat_here - lon_here)

            if dist < 160.0:
                line_certo = f'{index};{y};{x};{lat_here};{lon_here}'
                fCerto.write('\n')
                fCerto.write(line_certo)
                line_cmems = f'{index};{y_here};{x_here};{lat_altro};{lon_altro}'
                fCmems.write('\n')
                fCmems.write(line_cmems)

                all_dist.append(dist)
                print(index, '->', dist)

        fCmems.close()
        fCerto.close()
        dist_avg = np.mean(np.array(all_dist))
        dist_min = np.min(np.array(all_dist))
        dist_max = np.max(np.array(all_dist))
        print('Arerage distance:', dist_avg, dist_min, dist_max)
        file_end = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/Grid_OLCI_BS.csv'
        dfcmems = pd.read_csv(file_grid_cmems, sep=';')
        dfcerto = pd.read_csv(file_grid_certo, sep=';')
        dfcmems['YCERTO'] = dfcerto['YCERTO']
        dfcmems['XCERTO'] = dfcerto['XCERTO']
        dfcmems['CMEMSVal'] = -999.0
        dfcmems['CERTOVal'] = -999.0
        dfcmems['Valid'] = 0
        dfcmems.to_csv(file_end, sep=';')
        return

    if do_global:
        print('[INFO] STARTED  GETTING GLOBAL POINTS...')
        from datetime import datetime as dt
        # getting global points
        dir_out_base = args.output
        param = args.input.split('%')[1]
        param_name = param
        if param.startswith('RRS'):
            # params_rrs_cmems=param: RRS_400,RRS_412_5,RRS_442_5,RRS_490,RRS_510,RRS_560,RRS_620,RRS_665,RRS_673_75,RRS_681_25,RRS_708_75,RRS_753_75,RRS_778_75,RRS_865,RRS_885,RRS_1020
            rrs_conversion = {
                'RRS_400': 'RRS_400',
                'RRS_412_5': 'RRS_412',
                'RRS_442_5': 'RRS_443',
                'RRS_490': 'RRS_490',
                'RRS_510': 'RRS_510',
                'RRS_560': 'RRS_560',
                'RRS_620': 'RRS_620',
                'RRS_665': 'RRS_665',
                'RRS_673_75': 'RRS_674',
                'RRS_681_25': 'RRS_681',
                'RRS_708_75': 'RRS_709',
                'RRS_753_75': 'RRS_754',
                'RRS_778_75': 'RRS_779',
                'RRS_865': 'RRS_865',
                'RRS_885': 'RRS_885',
                'RRS_1020': 'RRS_1020',
            }
            param_rrs_cmems = param
            param_rrs_certo = rrs_conversion[param]
            param_name = 'RRS'

        dir_comparison = os.path.join(dir_out_base, f'COMPARISON_{param_name}')

        file_out = os.path.join(dir_out_base, f'{param.lower()}_points.csv')

        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')

        colCMEMS = 'CMEMSVal'
        colCERTO = 'CERTOVal'
        if param_name == 'RRS':
            colCMEMS = f'{colCMEMS}_{param_rrs_cmems}'
            colCERTO = f'{colCERTO}_{param_rrs_certo}'
            first_line = f'Date;Index;CMEMSVal;CERTOVal'
        if param_name == 'ALL':
            first_line = None

        f1 = open(file_out, 'w')
        if first_line is not None:
            f1.write(first_line)
        nfiles = 0

        date_here = start_date
        while date_here <= end_date:
            year = date_here.strftime('%Y')
            jday = date_here.strftime('%j')
            file_c = os.path.join(dir_comparison, f'Comparison_{param_name}_{year}{jday}.csv')
            date_here_str = date_here.strftime('%Y-%m-%d')

            ##first line if is None
            if os.path.exists(file_c) and param_name == 'ALL':
                if first_line is None:
                    fr = open(file_c, 'r')
                    first_line = fr.readline()
                    first_line_s = first_line.strip().split(';')[3:]
                    first_line = f'Date;{";".join(first_line_s)}'
                    fr.close()
                    f1.write(first_line)
                fr = open(file_c, 'r')
                started = False
                for line in fr:
                    if not started:
                        started = True
                    else:
                        line_s = line.strip().split(';')[3:]
                        line = f'{date_here_str};{";".join(line_s)}'
                        f1.write('\n')
                        f1.write(line)

                fr.close()

            if os.path.exists(file_c) and param_name != 'ALL':
                print(f'[INFO]Date: {date_here}->{file_c}')
                nfiles = nfiles + 1
                points_here = pd.read_csv(file_c, sep=';')
                for index, row in points_here.iterrows():
                    cmems_val = row[colCMEMS]
                    certo_val = row[colCERTO]
                    index_here = row['Index']
                    line = f'{date_here_str};{index_here};{cmems_val};{certo_val}'
                    f1.write('\n')
                    f1.write(line)
            date_here = date_here + timedelta(hours=24)  # 10 days

        print(f'[INFO] Completed')
        return

    if do_prepare_plot:
        from datetime import datetime as dt
        dir_out_base = args.output
        for name in os.listdir(dir_out_base):
            if not name.endswith('_points.csv'):
                continue
            fname = os.path.join(dir_out_base, name)
            print('------------------------>', fname)
            df = pd.read_csv(fname, sep=';')
            fout = os.path.join(dir_out_base, f'{name[:-4]}_valid.csv')
            f1 = open(fout, 'w')
            f1.write('Date;Index;CMEMSVal;CERTOVal')
            for index, row in df.iterrows():
                date_here = row['Date']
                index_here = str(row['Index'])
                valCMEMS = row['CMEMSVal']
                valCERTO = row['CERTOVal']
                if np.isnan(valCMEMS):
                    continue
                if np.isnan(valCERTO):
                    continue
                if valCMEMS < (-1):
                    continue
                if valCERTO < (-1):
                    continue
                f1.write('\n')
                line = f'{date_here};{index_here};{valCMEMS};{valCERTO}'
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
            f1.write('Date;Index;CMEMSVal;CERTOVal')
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
                    valMulti = row['CMEMSVal']
                    valOlci = row['CERTOVal']
                    line = f'{date_here};{index_here};{valMulti};{valOlci}'
                    f1.write(line)
            f1.close()

        return

    if do_prepare_plot_all:
        file_csv = args.output
        if not os.path.exists(file_csv):
            print(f'[ERROR] Input file csv {file_csv} does not exist')
            return
        name = os.path.basename(file_csv)
        file_out = os.path.join(os.path.dirname(file_csv), f'{name[:-4]}_common.csv')
        dir_ref = os.path.join(os.path.dirname(file_csv), 'RRS_POINTS')
        for name in os.listdir(dir_ref):
            if name.find('valid_common.csv') > 0:
                file_ref = os.path.join(dir_ref, name)
                break
        df = pd.read_csv(file_ref, sep=';')
        indices = []
        for index, row in df.iterrows():
            date_here = row['Date']
            index_here = row['Index']
            index_here = f'{index_here:.0f}'

            di = f'{date_here}_{index_here}'
            print(di)
            indices.append(di)
        print('Common points: ', len(indices))
        dinput = pd.read_csv(file_csv, sep=';')
        fout = open(file_out, 'w')
        first_line = ";".join(dinput.columns.tolist())
        fout.write(first_line)
        ntotal = 0
        nadded = 0
        for index, row in dinput.iterrows():

            date_here = row['Date']
            index_here = str(row['Index'])
            di = f'{date_here}_{index_here}'
            if ntotal % 1000 == 0:
                print(nadded, ' de ', ntotal, '->', di)
            ntotal = ntotal + 1
            if di in indices:
                lr = [str(x) for x in row.tolist()]
                line = ";".join(lr)
                fout.write('\n')
                fout.write(line)
                nadded = nadded + 1

        fout.close()
        print(f'Total lines: {ntotal} Added: {nadded}')
        return

    if do_add_param:
        dir_comparison = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/COMPARISON_RRS'
        dir_base = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/RRS_POINTS/'
        file_ref = os.path.join(dir_base, 'rrs_400_points_valid_common.csv')
        dir_out = os.path.join(dir_base, 'tmp')
        if not os.path.isdir(dir_out):
            os.mkdir(dir_out)
        ref_wce = 'points_valid_common.csv'
        df = pd.read_csv(file_ref, ';')
        nvalues = len(df.index)
        output = np.zeros((nvalues,))
        ioutput = 0
        dates = pd.unique(df['Date'][:])
        for date in dates:
            date_here = dt.strptime(date, '%Y-%m-%d')
            indices = np.array(df.loc[df['Date'] == date, 'Index'])
            file_comparison = os.path.join(dir_comparison, f'Comparison_RRS_{date_here.strftime("%Y%j")}.csv')
            dfcomparison = pd.read_csv(file_comparison, sep=';')
            if len(indices) < len(dfcomparison.index):
                values = []
                for index in indices:
                    values.append(dfcomparison.loc[dfcomparison['Index'] == index, 'blended_dominant_owt'])
                values = np.array(values)
            else:
                values = np.array(dfcomparison.loc[dfcomparison['Index'] == indices, 'blended_dominant_owt'])
            output[ioutput:ioutput + len(values)] = values.flatten()[:]
            ioutput = ioutput + len(values)
            print(date, ioutput)

        for name in os.listdir(dir_base):
            if name.find(ref_wce) > 0:
                file_in = os.path.join(dir_base, name)
                file_out = os.path.join(dir_out, name)
                print('->', name)
                df_in = pd.read_csv(file_in, ';')
                df_out = df_in.assign(blended_dominat_owt=output)
                df_out.to_csv(file_out, sep=';')

        return

    ##comparison
    print('[INFO] STARTED  COMPARISON...')
    from datetime import datetime as dt
    dir_out_base = args.output
    if not os.path.exists(dir_out_base):
        print(f'[ERROR] Output dir: {dir_out_base} does not exist')
        return
    # SERVER
    if dir_out_base.startswith('/store'):
        dir_cmems_orig = '/dst04-data1/OC/OLCI/daily_v202311_bc'
        dir_certo_orig = '/store3/DOORS/CERTO_SOURCES'
    # LOCAL
    elif dir_out_base.startswith('/mnt'):
        dir_cmems_orig = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/SOURCES_CMEMS'
        dir_certo_orig = '/mnt/c/DATA_LUIS/DOORS_WORK/COMPARISON_CMEMS_CERTO/SOURCES_CERTO'
    else:
        return
    nhours = 240
    if args.interval:
        nhours = int(args.interval) * 24
    if args.input == 'RRS':
        params = ['RRS']
        wl_cmems = ['400', '412_5', '442_5', '490', '510', '560', '620', '665', '673_75', '681_25', '708_75', '753_75',
                    '778_75', '865', '885', '1020']
        wl_certo = ['400', '412', '443', '490', '510', '560', '620', '665', '674', '681', '709', '754', '779', '865',
                    '885', '1020']

    if args.input == 'ALL':
        params = ['ALL']
        all_cmems = ['chl', 'chlnn', 'chloc4me', 'kd490', 't865', 'tsmnn']
        all_certo = ['blended_chla', 'blended_chla_from_predominant_owt', 'blended_chla_top_2_reweighted',
                     'blended_chla_top_2_weighted', 'blended_chla_top_3_reweighted', 'blended_chla_top_3_weighted',
                     'blended_tsm', 'blended_tsm_top_2_reweighted', 'blended_tsm_top_2_weighted',
                     'blended_tsm_top_3_reweighted', 'blended_tsm_top_3_weighted',
                     'chlor_ChlGdal', 'chlor_ChlGdal_Capped', 'chlor_ChlGilSA2_NaN', 'chlor_ChlGit',
                     'chlor_ChlGit_Capped', 'chlor_oc3', 'chlor_oc4Med', 'chlor_oc5', 'chlor_oc5ci', 'chlor_oci2']

    dir_outs = []
    for param in params:
        dir_out = os.path.join(dir_out_base, f'COMPARISON_{param}')
        if not os.path.exists(dir_out):
            os.mkdir(dir_out)
        dir_outs.append(dir_out)
    file_grid = os.path.join(dir_out_base, f'Grid_OLCI_BS.csv')

    optical_water_types = ['blended_dominant_owt', 'owt_dominant_OWT']

    if args.input == 'RRS':
        col_names_cmems = []
        col_names_certo = []
        file_new_grid = os.path.join(dir_out_base, f'Grid_OLCI_BS_Bands.csv')
        dfgrid = pd.read_csv(file_grid, sep=';')

        for wl in wl_cmems:
            new_col = f'CMEMSVal_RRS_{wl}'
            col_names_cmems.append(new_col)
            dfgrid[new_col] = -999.0
        for wl in wl_certo:
            new_col = f'CERTOVal_RRS_{wl}'
            col_names_certo.append(new_col)
            dfgrid[new_col] = -999.0
        for owt in optical_water_types:
            dfgrid[owt] = -999.0
        dfgrid = dfgrid.drop('CMEMSVal', axis=1)
        dfgrid = dfgrid.drop('CERTOVal', axis=1)
        dfgrid.to_csv(file_new_grid, sep=';')
        file_grid = file_new_grid

    if args.input == 'ALL':
        col_names_cmems = []
        col_names_certo = []
        file_new_grid = os.path.join(dir_out_base, f'Grid_OLCI_BS_All.csv')
        dfgrid = pd.read_csv(file_grid, sep=';')
        for p in all_cmems:
            new_col = f'CMEMS_{p}'
            col_names_cmems.append(new_col)
            dfgrid[new_col] = -999.0
        for p in all_certo:
            new_col = f'CERTO_{p}'
            col_names_certo.append(new_col)
            dfgrid[new_col] = -999.0
        for owt in optical_water_types:
            dfgrid[owt] = -999.0
        dfgrid = dfgrid.drop('CMEMSVal', axis=1)
        dfgrid = dfgrid.drop('CERTOVal', axis=1)
        dfgrid.to_csv(file_new_grid, sep=';')
        file_grid = file_new_grid

    start_date = dt.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.strptime(args.end_date, '%Y-%m-%d')
    date_here = start_date

    while date_here <= end_date:

        if args.verbose:
            print(f'[INFO] Worknig for date: {date_here}...')

        for param, dir_out in zip(params, dir_outs):
            # print(param, dir_out)
            # PARAMS, TO DEFINE FILE NAMES
            param_cmems = param
            param_certo = param
            # var_cmems = param_cmems
            # var_certo = param_certo
            # if param_olci == 'PLANKTON':
            #     var_olci = 'CHL'
            # if param_olci == 'TRANSP':
            #     var_olci = 'KD490'

            year = date_here.strftime('%Y')
            jday = date_here.strftime('%j')
            dir_cmems = os.path.join(dir_cmems_orig, year, jday)
            dir_certo = os.path.join(dir_certo_orig, year, jday)

            if os.path.exists(dir_cmems):
                if param_cmems == 'RRS':
                    files_cmems = []
                    name_certo = f'CERTO_blk_{date_here.strftime("%Y%m%d")}_OLCI_RES300__final_l3_product.nc'
                    file_certo = os.path.join(dir_certo, name_certo)
                    do_proc = True
                    for wlc in wl_cmems:
                        file_cmems = os.path.join(dir_cmems, f'O{year}{jday}-rrs{wlc}-bs-fr.nc')
                        if os.path.exists(file_cmems):
                            files_cmems.append(file_cmems)
                        else:
                            print(f'[WARNING] File CMEMS {file_cmems} does not exist...')
                            do_proc = False
                            break
                    if do_proc:
                        try_download = False
                        if not os.path.exists(file_certo):
                            try_download = True
                        if os.path.exists(file_certo) and os.stat(file_certo).st_size == 0:
                            os.remove(file_certo)
                            try_download = True
                        if try_download:
                            try:
                                dir_certo_year = os.path.join(dir_certo_orig, year)
                                if not os.path.isdir(dir_certo_year):
                                    os.mkdir(dir_certo_year)
                                dir_certo_jday = os.path.join(dir_certo_year, jday)
                                if not os.path.isdir(dir_certo_jday):
                                    os.mkdir(dir_certo_jday)
                                download_olci_source(name_certo, file_certo)
                            except:
                                pass
                        do_proc = os.path.exists(file_certo) and os.stat(file_certo).st_size > 0
                        if do_proc:
                            file_out = os.path.join(dir_out, f'Comparison_{param}_{year}{jday}.csv')
                            if os.path.exists(file_out):
                                print(f'[WARNING] File: {file_out} already exists. Skipping...')
                            else:
                                make_comparison_band_impl(file_grid, files_cmems, file_certo, file_out, wl_cmems,
                                                          wl_certo, col_names_cmems, col_names_certo,
                                                          optical_water_types)
                        else:
                            print(f'[WARNING] File CERTO {file_certo} is not available. Skipping....')

                if param_cmems == 'ALL':
                    files_cmems = []
                    name_certo = f'CERTO_blk_{date_here.strftime("%Y%m%d")}_OLCI_RES300__final_l3_product.nc'
                    file_certo = os.path.join(dir_certo, name_certo)
                    do_proc = True
                    for p in all_cmems:
                        file_cmems = os.path.join(dir_cmems, f'O{year}{jday}-{p}-bs-fr.nc')
                        if os.path.exists(file_cmems):
                            files_cmems.append(file_cmems)
                        else:
                            print(f'[WARNING] File CMEMS {file_cmems} does not exist...')
                            do_proc = False
                            break
                    if do_proc:
                        try_download = False
                        if not os.path.exists(file_certo):
                            try_download = True
                        if os.path.exists(file_certo) and os.stat(file_certo).st_size == 0:
                            os.remove(file_certo)
                            try_download = True
                        if try_download:
                            try:
                                dir_certo_year = os.path.join(dir_certo_orig, year)
                                if not os.path.isdir(dir_certo_year):
                                    os.mkdir(dir_certo_year)
                                dir_certo_jday = os.path.join(dir_certo_year, jday)
                                if not os.path.isdir(dir_certo_jday):
                                    os.mkdir(dir_certo_jday)
                                download_olci_source(name_certo, file_certo)
                            except:
                                pass
                        do_proc = os.path.exists(file_certo) and os.stat(file_certo).st_size > 0
                        if do_proc:
                            file_out = os.path.join(dir_out, f'Comparison_{param}_{year}{jday}.csv')
                            if os.path.exists(file_out):
                                print(f'[WARNING] File: {file_out} already exists. Skipping...')
                            else:
                                make_comparison_all_impl(file_grid, files_cmems, file_certo, file_out, all_cmems,
                                                         all_certo, col_names_cmems, col_names_certo,
                                                         optical_water_types)
                        else:
                            print(f'[WARNING] File CERTO {file_certo} is not available. Skipping....')

            else:
                if not os.path.exists(dir_cmems):
                    print(f'[WARNING] CMEMS path {dir_cmems} does not exits. Skipping...')
                if not os.path.exists(dir_certo):
                    print(f'[WARNING] CERTO path {dir_certo} does not exits. Skipping...')
        date_here = date_here + timedelta(hours=nhours)


def download_olci_source(namefile, file_out):
    ##DONWLOAD
    cmd = f'wget --user=rsg_dump --password=yohlooHohw2Pa9ohv1Chi ftp://ftp.rsg.pml.ac.uk/DOORS_matchups/OLCI/{namefile} -O {file_out}'
    if args.verbose:
        print(f'[INFO] Trying download with cmd:')
        print(f'[INFO] {cmd}')

    proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        outs, errs = proc.communicate(timeout=1800)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()


def do_comparison_multi_olci():
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np

    # if args.input == 'TEST':
    #     do_test()
    #     return

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
    print('[INFO] STARTED HARDCORED COMPARISON...')
    from datetime import datetime as dt
    dir_olci_orig = '/store3/OC/OLCI_POLYMER/BAL/POLYMER_BAL202411'
    dir_multi_orig = '/store3/OC/CCI_v2017/daily_v202411'
    #FOLDERS: CHLA, RRS443, RRS490, RRS510, RRS560, RRS670
    dir_out = '/store/COP2-OC-TAC/BAL_Evolutions/COMPARISON_MULTI_OLCI/CHLA_202411'
    file_grid = '/store/COP2-OC-TAC/BAL_Evolutions/COMPARISON_MULTI_OLCI/Grid.csv'
    start_date = dt(2019,1,1)
    end_date = dt(2019,12,31)
    #end_date = dt(2022,12,31)
    date_here = start_date
    while date_here<=end_date:
        #date_here_str = date_here.strftime('%Y%m%d')
        year = date_here.strftime('%Y')
        jday = date_here.strftime('%j')
        dir_olci = os.path.join(dir_olci_orig,year,jday)
        dir_multi = os.path.join(dir_multi_orig,year,jday)
        #print(date_here_str)
        if os.path.exists(dir_olci) and os.path.exists(dir_multi):

            #chla,rrs442_5,rrs490,rrs510,rrs560,rrs665
            file_olci =os.path.join(dir_olci,f'O{year}{jday}-chl-bal-fr.nc')
            # chla,rrs443,rrs490,rrs510,rrs560,rrs665
            file_multi = os.path.join(dir_multi,f'C{year}{jday}-chl-bal-hr.nc')

            if os.path.exists(file_multi) and os.path.exists(file_olci):
                print(f'[INFO] Making date: {date_here}')
                file_out = os.path.join(dir_out,f'Comparison_CHL_{year}{jday}.csv')
                make_comparison_impl(file_grid,file_multi,file_olci,file_out,'CHL','CHL')
        date_here = date_here + timedelta(hours=240)

    # getting global points
    # val = 'CHLA'
    # dir_comparison = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/{val}'
    # val = val.lower()
    # file_out = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/{val}_points.csv'
    #
    # file_ref = f'/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/COMPARISON_OLCI_MULTI/rrs510_points.csv'
    # # file_ref = None
    # first_line = f'Date;Index;MultiVal;OlciVal'
    # f1 = open(file_out, 'w')
    # f1.write(first_line)
    # nfiles = 0
    # if file_ref is None:
    #     start_date = dt(2016, 5, 1)
    #     end_date = dt(2022, 12, 31)
    #     date_here = start_date
    #
    #     while date_here <= end_date:
    #         year = date_here.strftime('%Y')
    #         jday = date_here.strftime('%j')
    #         file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
    #         date_here_str = date_here.strftime('%Y-%m-%d')
    #         print(date_here_str)
    #         if os.path.exists(file_c):
    #             nfiles = nfiles + 1
    #             points_here = pd.read_csv(file_c, sep=';')
    #             for index, row in points_here.iterrows():
    #                 multi_val = row['MultiVal']
    #                 olci_val = row['OlciVal']
    #                 index_here = row['Index']
    #                 line = f'{date_here_str};{index_here};{multi_val};{olci_val}'
    #                 f1.write('\n')
    #                 f1.write(line)
    #         date_here = date_here + timedelta(hours=240)  # 10 days
    # else:
    #     df_ref = pd.read_csv(file_ref, sep=';')
    #     dates_ref = df_ref['Date']
    #     index_ref = df_ref['Index']
    #     nref = len(df_ref.index)
    #     date_check = dt(2016, 5, 1)
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
    #         date_here = dt.strptime(str(dates_ref[idx]), '%Y-%m-%d')
    #         index_here = int(index_ref[idx])
    #         if date_here != date_check:
    #             print(date_check)
    #             date_check = date_here
    #             year = date_check.strftime('%Y')
    #             jday = date_check.strftime('%j')
    #             date_check_str = date_check.strftime('%Y-%m-%d')
    #             file_c = os.path.join(dir_comparison, f'Comparison_{val}_{year}{jday}.csv')
    #             points_check_here = pd.read_csv(file_c, sep=';')
    #             indices_check_here = points_check_here['Index'].to_numpy(dtype=np.int32).tolist()
    #             nfiles = nfiles + 1
    #         if index_here in indices_check_here:
    #             idx = indices_check_here.index(index_here)
    #             multi_val = points_check_here.iloc[idx].at['MultiVal']
    #             olci_val = points_check_here.iloc[idx].at['OlciVal']
    #             line = f'{date_check_str};{index_here};{multi_val};{olci_val}'
    #             f1.write('\n')
    #             f1.write(line)
    #             ndata = ndata + 1
    #         else:
    #             nnodata = nnodata + 1
    #
    #     print('NFILES: ', nfiles)
    #     print('NDATA: ', ndata)
    #     print('NNODATA: ', nnodata)
    #
    # f1.close()
    # print('NFILES: ', nfiles)


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


def make_comparison_band_impl(file_grid, files_cmems, file_certo, file_out, wl_cmems, wl_certo, col_names_cmems,
                              col_names_certo, optical_water_types):
    from netCDF4 import Dataset
    grid = pd.read_csv(file_grid, sep=';')

    if args.verbose:
        print(f'[INFO] Creating CMEMS array...')
    num_m = len(files_cmems)
    for idx in range(num_m):
        file_cmems = files_cmems[idx]
        wlc = wl_cmems[idx]
        dataset_cmems = Dataset(file_cmems)
        variable_cmems = f'RRS{wlc}'
        array_c = np.array(dataset_cmems.variables[variable_cmems])
        if idx == 0:
            fill_value_cmems = dataset_cmems.variables[variable_cmems]._FillValue
            s = array_c.shape
            array_cmems = np.zeros((num_m, s[1], s[2]))
        array_cmems[idx, :, :] = array_c[0, :, :]
        dataset_cmems.close()

    if args.verbose:
        print(f'[INFO] Creating CERTO array...')
    dataset_certo = Dataset(file_certo)
    for idx, wl in enumerate(wl_certo):
        variable_certo = f'Rw{wl}_rep'
        array_c = np.array(dataset_certo.variables[variable_certo])
        if idx == 0:
            fill_value_certo = dataset_certo.variables[variable_certo]._FillValue
            s = array_c.shape
            array_certo = np.zeros((num_m, s[1], s[2]))
            if len(optical_water_types) > 0:
                array_owt = np.zeros((len(optical_water_types), s[1], s[2]))
        array_certo[idx, :, :] = array_c[0, :, :]

    if len(optical_water_types) > 0:
        for idx, owt in enumerate(optical_water_types):
            array_c = np.array(dataset_certo.variables[owt])
            array_owt[idx, :, :] = array_c[0, :, :]

    dataset_certo.close()

    nvalid = 0
    required_valid_pixels = num_m * 9
    if args.verbose:
        print(
            f'[INFO] Number of bands: {num_m}. Valid pixels per band: 9. Required valid pixels: {required_valid_pixels}')
        print(f'[INFO] Checking grid values...')
    for index, row in grid.iterrows():
        ycmems = int(row['YCMEMS'])
        xcmems = int(row['XCMEMS'])
        ycerto = int(row['YCERTO'])
        xcerto = int(row['XCERTO'])
        array_cmems_here = array_cmems[:, ycmems - 1:ycmems + 2, xcmems - 1:xcmems + 2]
        array_cmems_good = array_cmems_here[array_cmems_here != fill_value_cmems]
        array_certo_here = array_certo[:, ycerto - 1:ycerto + 2, xcerto - 1:xcerto + 2]
        array_certo_good = array_certo_here[array_certo_here != fill_value_certo]
        valid = 0

        if len(array_certo_good) >= required_valid_pixels and len(array_cmems_good) >= required_valid_pixels:
            nvalid = nvalid + 1
            valid = 1

        grid.loc[index, 'Valid'] = valid
        if valid == 1:
            spectra_cmems = np.mean(array_cmems_here, axis=(1, 2))
            spectra_certo = np.mean(array_certo_here, axis=(1, 2))
            spectra_certo = spectra_certo / np.pi
            grid.loc[index, col_names_cmems] = spectra_cmems
            grid.loc[index, col_names_certo] = spectra_certo
            if len(optical_water_types) > 0:
                res_owt = np.zeros(len(optical_water_types))

                for iowt in range(len(optical_water_types)):
                    array_owt_here = array_owt[iowt, ycerto - 1:ycerto + 2, xcerto - 1:xcerto + 2]
                    array_owt_here = array_owt_here[array_owt_here < 18]
                    if len(array_owt_here) == 0:
                        res_owt[iowt] = -1
                    else:
                        res_owt[iowt] = np.argmax(np.bincount(array_owt_here.flatten().astype(np.int32)))
                grid.loc[index, optical_water_types] = res_owt
    if nvalid > 0:
        grid_valid = grid[grid['Valid'] == 1]
        grid_valid.to_csv(file_out, sep=';')
        if args.verbose:
            print(f'[INFO] Completed. Number of valid values: {nvalid}')
    else:
        print(f'[WARNING] Valid pixels were not found')


def make_comparison_all_impl(file_grid, files_cmems, file_certo, file_out, all_cmems, all_certo, col_names_cmems,
                             col_names_certo, optical_water_types):
    from netCDF4 import Dataset
    grid = pd.read_csv(file_grid, sep=';')

    if args.verbose:
        print(f'[INFO] Creating CMEMS array...')
    num_m = len(files_cmems)
    for idx in range(num_m):
        file_cmems = files_cmems[idx]
        p = all_cmems[idx]
        dataset_cmems = Dataset(file_cmems)
        variable_cmems = f'{p.upper()}'
        array_c = np.array(dataset_cmems.variables[variable_cmems])
        if idx == 0:
            fill_value_cmems = dataset_cmems.variables[variable_cmems]._FillValue
            s = array_c.shape
            array_cmems = np.zeros((num_m, s[1], s[2]))
        array_cmems[idx, :, :] = array_c[0, :, :]
        dataset_cmems.close()

    num_c = len(all_certo)
    if args.verbose:
        print(f'[INFO] Creating CERTO array...')
    dataset_certo = Dataset(file_certo)
    for idx, p in enumerate(all_certo):
        variable_certo = f'{p}'
        array_c = np.array(dataset_certo.variables[variable_certo])
        if idx == 0:
            fill_value_certo = dataset_certo.variables[variable_certo]._FillValue
            s = array_c.shape
            array_certo = np.zeros((num_c, s[1], s[2]))
            if len(optical_water_types) > 0:
                array_owt = np.zeros((len(optical_water_types), s[1], s[2]))
        array_certo[idx, :, :] = array_c[0, :, :]

    if len(optical_water_types) > 0:
        for idx, owt in enumerate(optical_water_types):
            array_c = np.array(dataset_certo.variables[owt])
            array_owt[idx, :, :] = array_c[0, :, :]

    dataset_certo.close()

    nvalid = 0
    required_valid_pixels = 9
    if args.verbose:
        print(f'[INFO] Required valid pixels per parameter: 9')
        print(f'[INFO] Checking grid values...')
    for index, row in grid.iterrows():
        ycmems = int(row['YCMEMS'])
        xcmems = int(row['XCMEMS'])
        ycerto = int(row['YCERTO'])
        xcerto = int(row['XCERTO'])
        array_cmems_here = array_cmems[:, ycmems - 1:ycmems + 2, xcmems - 1:xcmems + 2]
        n_cmems_good = np.count_nonzero(array_cmems_here != fill_value_cmems, axis=(1, 2))
        array_certo_here = array_certo[:, ycerto - 1:ycerto + 2, xcerto - 1:xcerto + 2]
        n_certo_good = np.count_nonzero(array_certo_here != fill_value_certo, axis=(1, 2))

        n_valid_here = np.count_nonzero(n_cmems_good >= required_valid_pixels) + np.count_nonzero(
            n_certo_good >= required_valid_pixels)
        valid = 0
        if n_valid_here > 0:
            nvalid = nvalid + 1
            valid = 1

        # array_certo_good = array_certo_here[array_certo_here != fill_value_certo]
        # valid = 0

        # if len(array_certo_good) >= required_valid_pixels and len(array_cmems_good) >= required_valid_pixels:
        #    nvalid = nvalid + 1
        #    valid = 1

        grid.loc[index, 'Valid'] = valid
        if valid == 1:
            all_cmems = np.mean(array_cmems_here, axis=(1, 2))
            all_certo = np.mean(array_certo_here, axis=(1, 2))
            all_cmems[n_cmems_good < required_valid_pixels] = -999.0
            all_certo[n_certo_good < required_valid_pixels] = -999.0
            # print(all_cmems.shape,len(col_names_cmems),col_names_cmems)
            grid.loc[index, col_names_cmems] = all_cmems
            # print(all_certo.shape, len(col_names_certo), col_names_certo)
            grid.loc[index, col_names_certo] = all_certo
            if len(optical_water_types) > 0:
                res_owt = np.zeros(len(optical_water_types))

                for iowt in range(len(optical_water_types)):
                    array_owt_here = array_owt[iowt, ycerto - 1:ycerto + 2, xcerto - 1:xcerto + 2]
                    array_owt_here = array_owt_here[array_owt_here < 18]
                    if len(array_owt_here) == 0:
                        res_owt[iowt] = -1
                    else:
                        res_owt[iowt] = np.argmax(np.bincount(array_owt_here.flatten().astype(np.int32)))
                grid.loc[index, optical_water_types] = res_owt
    if nvalid > 0:
        grid_valid = grid[grid['Valid'] == 1]
        grid_valid.to_csv(file_out, sep=';')
        if args.verbose:
            print(f'[INFO] Completed. Number of valid values: {nvalid}')
    else:
        print(f'[WARNING] Valid pixels were not found')


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
