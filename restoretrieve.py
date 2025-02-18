import argparse
import os.path
from datetime import datetime as dt
from datetime import timedelta
from restoweb.resto import RESTO_WEB
from netCDF4 import Dataset

parser = argparse.ArgumentParser(description="Retrieve RRS data from Wisp Stations in Trasimeno Lake")
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD Default: 2022-03-16")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD Dafult: Today")
parser.add_argument('-o', "--outputfile", help="Output file (.nc) or output directory", required=True)
parser.add_argument('-ld', "--date_list_file", help="Input .nc file for obtaining date list. Output file must be a directory")
parser.add_argument('-concat',"--concat_files",help="-o option indicates the input/output directory for contatenation of .nc files with name format: RESTO_TRIT_*.nc", action="store_true")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


def main():
    print('[INFO] Started')

    start_date = dt(2022, 3, 16)
    end_date = dt.now()
    if args.startdate:
        try:
            start_date = dt.strptime(args.startdate, '%Y-%m-%d')
        except:
            print(f'[ERROR] Start date: {args.startdate} is not in a valid format (YYYY-mm-dd)')
            exit()
    if args.startdate:
        try:
            end_date = dt.strptime(args.enddate, '%Y-%m-%d')
            end_date = end_date.replace(hour=23, minute=59)
        except:
            print(f'[ERROR] End date: {args.enddate} is not in a valid format (YYYY-mm-dd)')
            exit()
    if start_date >= end_date:
        print(f'[ERROR] Start date {start_date} must be before than end date: {end_date}')
        exit()

    if os.path.exists(args.outputfile) and os.path.isdir(args.outputfile):
        output_file = os.path.join(args.outputfile,'RESTO_TAIT_'+start_date.strftime('%Y%m%d')+'_'+end_date.strftime('%Y%m%d')+'.nc')
    else:
        if args.outputfile.find('.') == -1:
            output_file = args.outputfile + '.nc'
        else:
            output_file = args.outputfile

    if not output_file.endswith('.nc'):
        print(f'[ERROR] Output file: {output_file} must be a *.nc file')
        exit()
    if os.path.exists(output_file):
        print(f'[WARNING] Output file: {output_file} already exist.')



    rweb = RESTO_WEB(args.verbose)
    rweb.retrive_data(start_date,end_date, None)
    rweb.save_data_as_ncfile(output_file)


def retrieve_date_list():
    input_file = args.date_list_file
    if not input_file.endswith('.nc'):
        print(f'[ERROR] Date list file {input_file} must be a .nc file')
        exit()
    if not os.path.exists(args.outputfile):
        print(f'[ERROR] Output directory {args.outputfile} does not exist')
        exit()
    if not os.path.isdir(args.outputfile):
        print(f'[ERROR] Output  {args.outputfile} is not a directory')
        exit()
    print('[INFO] Started')
    name_file = input_file.split('/')[-1]
    name_file = name_file[:name_file.find('.')]+'_date_list.txt'
    output_file = os.path.join(args.outputfile,name_file)

    dset = Dataset(input_file)
    time_array = dset.variables['Time'][:]
    date_list = []
    idate = 0
    dtref = dt(1970, 1, 1, 0, 0, 0).replace(microsecond=0)
    for t in time_array:
        dthere = dtref + timedelta(seconds=t)
        dthere_day = dthere.strftime('%Y-%m-%d')
        print(dthere_day)
        if idate==0:
            date_list.append(dthere_day)
            idate = idate +1
        else:
            if dthere_day!=date_list[idate-1]:
                date_list.append(dthere_day)
                idate = idate +1

    print(f'[INFO] Number of dates: {idate}')
    print(f'[INFO] Writting file list...')
    with open(output_file, 'w') as file:
        for date in date_list:
            file.writelines(date)
            file.writelines('\n')
    print(f'[INFO] Completed')

def make_concatenation():
    input_dir = args.outputfile
    if not os.path.isdir(input_dir):
        print(f'[ERROR] Ouput option {input_dir} must be a valid directory')
        return
    file_list = {}
    for name in os.listdir(input_dir):
        if not name.startswith('RESTO_TRIT_') or name.endswith('COMPLETE.nc'):
            continue
        input_file = os.path.join(input_dir,name)
        dataset = Dataset(input_file)

        start_time = dt.utcfromtimestamp(int(dataset.variables['Time'][0]))
        end_time = dt.utcfromtimestamp(int(dataset.variables['Time'][-1]))
        ref = int(start_time.strftime('%Y%m%d'))
        file_list[ref] = {
            'file': input_file,
            'start_time': start_time,
            'end_time': end_time
        }
        dataset.close()
    myKeys = list(file_list.keys())
    myKeys.sort()
    # Sorted Dictionary
    file_list= {i: file_list[i] for i in myKeys}
    start_date_abs = None
    end_date_abs = None
    nominal_wl = None
    file_list_concat = {}
    ntimes_all = 0

    for ref in file_list:
        file = file_list[ref]['file']
        dataset = Dataset(file,'r')
        time_array = dataset.variables['Time'][:]
        ntimes = len(time_array)
        if start_date_abs is None and end_date_abs is None:##first file
            start_date_abs = file_list[ref]['start_time']
            end_date_abs = file_list[ref]['end_time']
            nominal_wl = dataset.variables['Nominal_Wavelenghts'][:]
            ntimes_all = ntimes
            file_list_concat[file] = {
                'index_ini':0,
                'index_end':ntimes
            }
        else:
            if file_list[ref]['end_time']<=end_date_abs:
                ##dates are overlapped with the previous file
                continue
            index_ini = 0
            if file_list[ref]['start_time']<=end_date_abs: ##we must find the first index
                for idx in range(ntimes):
                    there = dt.utcfromtimestamp(int(time_array[idx]))
                    if there>end_date_abs:
                        index_ini = idx
                        break

            end_date_abs = file_list[ref]['end_time']

            ntimes_all = ntimes_all + (ntimes-index_ini)
            file_list_concat[file] = {
                'index_ini': index_ini,
                'index_end': ntimes
            }

        dataset.close()

    name_concat = f'RESTO_TRIT_{start_date_abs.strftime("%Y%m%d")}_{end_date_abs.strftime("%Y%m%d")}_COMPLETE.nc'
    file_out = os.path.join(input_dir,name_concat)
    nwl = len(nominal_wl)
    print(f'[INFO] Creating file out: {file_out}')
    print(f'[INFO] --> Number of spectra: {ntimes_all}')
    print(f'[INFO] --> Number of bands: {nwl}')
    dout = Dataset(file_out,'w')
    dout.createDimension('TimeIndex',ntimes_all)
    dout.createDimension('Central_wavelenghts',nwl)
    attrs = {
        'site_name': 'Trasimeno_Lake',
        'site': 'TRIT',
        'longitude': 12.13306,
        'latitude': 43.12278,
        'instrument': 'Wispstation012',
        'start_date': start_date_abs.replace(hour=0,minute=0).strftime('%Y-%m-%d %H:%M'),
        'end_date': end_date_abs.replace(hour=23,minute=59).strftime('%Y-%m-%d %H:%M')
    }
    dout.setncatts(attrs)

    var_time = dout.createVariable('Time','f8',('TimeIndex',),complevel=6,zlib=True,fill_value=-999.0)
    var_time.units = 'Seconds since 1970-1-1'

    var_wl = dout.createVariable('Nominal_Wavelenghts','f4',('Central_wavelenghts',),complevel=6,zlib=True)
    var_wl.units = 'nm'

    var_rrs = dout.createVariable('RRS', 'f4', ('TimeIndex','Central_wavelenghts'), complevel=6, zlib=True,fill_value=-999.0)

    index_ref = 0
    for file in file_list_concat:
        print(f'[INFO] Working with file: {file}')
        dataset = Dataset(file,'r')
        index_ini = file_list_concat[file]['index_ini']
        index_end = file_list_concat[file]['index_end']
        index_ref_end = index_ref + (index_end-index_ini)
        var_time[index_ref:index_ref_end] = dataset.variables['Time'][index_ini:index_end]
        var_rrs[index_ref:index_ref_end,:] = dataset.variables['RRS'][index_ini:index_end,:]
        index_ref = index_ref_end
        dataset.close()
        #print(file,file_list_concat[file]['index_ini'],file_list_concat[file]['index_end'])
    dout.close()

    print(f'[INFO] Completed')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if args.date_list_file:
        retrieve_date_list()
    if args.concat_files:
        make_concatenation()
    else:
        main()
