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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if args.date_list_file:
        retrieve_date_list()
    else:
        main()
