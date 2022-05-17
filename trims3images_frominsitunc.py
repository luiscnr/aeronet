import argparse

import pandas as pd
import numpy as np
import zipfile as zp
import tarfile as tp
from netCDF4 import Dataset
from datetime import datetime as dt
from datetime import timedelta
from base.flag_class import FLAG_LOIS
from check_geo import CHECK_GEO
import os
import subprocess
import s3olcitrim_bal_frompy as trimtool

parser = argparse.ArgumentParser(
    description="Search and trim S3 Level 1B products around a in-situ locations with valid data")

parser.add_argument('-m', "--mode", help="Mode", choices=['trim', 'dfcsv'])
parser.add_argument('-i', "--inputfile", help="Input nc or csv file", required=True)
parser.add_argument('-s', "--source", help="Source directory", required=True)
parser.add_argument('-o', "--output", help="Output directory (trim mode) or output file (dfcsv mode)", required=True)
parser.add_argument('-z', "--unzip_path", help="Temporal unzip directory")
parser.add_argument('-res', "--res_tag", help="Resolution tag (EFR, WFR)")
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD ")
parser.add_argument("-l", "--list_files",
                    help="Optional name for text file with a list of trimmed files (Default: None")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


class CSVInsituFile():
    def __init__(self, csv_path):
        self.dforig = pd.read_csv(csv_path, sep=';')
        self.valid_dates = {}

        # paramaters to be implemented using a configuration file
        # self.date_variable = None #if date_variable and date_format are None, date/time are in the same variable
        # self.date_format = None
        # self.time_variable = 'date'
        # self.time_format = '%Y-%m-%dT%H:%M'
        # self.lat_variable = 'latitude'
        # self.lon_variable = 'longitude'
        # self.invalid_value = None
        # self.variables = ['chl']

        self.date_variable = 'Date'  # if date_variable and date_format are None, date/time are in the same variable
        self.date_format = '%d/%m/%Y'
        self.time_variable = 'Time'
        self.time_format = '%H:%M:%S'
        self.lat_variable = 'Lat'
        self.lon_variable = 'Lon'
        self.invalid_value = -9999
        self.variables = ['CHLA']

    def get_valid_dates(self, valid_hours, start_date, end_date):
        if valid_hours is None:
            valid_hours = [8, 14]
        datearray = None
        if self.date_variable is not None:
            datearray = np.array(self.dforig.loc[:, self.date_variable])
        timearray = np.array(self.dforig.loc[:, self.time_variable])
        latarray = np.array(self.dforig.loc[:, self.lat_variable])
        lonarray = np.array(self.dforig.loc[:, self.lon_variable])

        # if self.flag_variable is not None:
        #     flag = self.nc.variables[self.flag_variable]
        #     flagclass = FLAG_LOIS(flag.flag_values, flag.flag_meanings)

        for idx in range(len(timearray)):

            time_here = None
            if datearray is None:
                time_here = dt.strptime(timearray[idx].strip(), self.time_format)
            else:
                format = f'{self.date_format}T{self.time_format}'
                time_here_str = f'{datearray[idx].strip()}T{timearray[idx].strip()}'
                time_here = dt.strptime(time_here_str, format)

            if time_here is None:
                print(f'[WARNING] Time is not valid. Skipping...')
                continue

            if time_here < start_date or time_here > end_date:
                if args.verbose:
                    # print(f'[WARNING] Time out of range. Skipping...')
                    continue

            # print(time_here,latarray[idx],lonarray[idx])

            flag_valid = True
            if self.invalid_value is not None:
                flag_valid = False
                for var in self.variables:
                    vararray = self.dforig.loc[:, var]
                    if vararray[idx] != self.invalid_value:
                        flag_valid = True
                        break
            # if self.flag_variable is not None:
            #     flag_value = flag[idx]
            #     flag_valid = flagclass.is_any_flag_valid(self.flag_valid_list, flag_value)

            if (valid_hours[0] <= time_here.hour <= valid_hours[1]) and flag_valid:
                date_here = time_here.strftime('%Y-%m-%d')
                hour_here = time_here.strftime('%H:%M:%S')
                if not date_here in self.valid_dates.keys():
                    self.valid_dates[date_here] = {
                        hour_here: {
                            'lat': float(latarray[idx]),
                            'lon': float(lonarray[idx])
                        }
                    }
                else:
                    self.valid_dates[date_here][hour_here] = {
                        'lat': float(latarray[idx]),
                        'lon': float(lonarray[idx])
                    }
                for var in self.variables:
                    vararray = self.dforig.loc[:, var]
                    self.valid_dates[date_here][hour_here][var] = float(vararray[idx])
        return self.valid_dates

    def compute_geo_limits(self):
        for d in self.valid_dates:
            lat_min = 90
            lat_max = -90
            lon_min = 180
            lon_max = -180
            for h in self.valid_dates[d]:
                if self.valid_dates[d][h]['lat'] < lat_min:
                    lat_min = self.valid_dates[d][h]['lat']
                if self.valid_dates[d][h]['lat'] > lat_max:
                    lat_max = self.valid_dates[d][h]['lat']
                if self.valid_dates[d][h]['lon'] < lon_min:
                    lon_min = self.valid_dates[d][h]['lon']
                if self.valid_dates[d][h]['lon'] > lon_max:
                    lon_max = self.valid_dates[d][h]['lon']
            self.valid_dates[d]['lat_min'] = lat_min
            self.valid_dates[d]['lat_max'] = lat_max
            self.valid_dates[d]['lon_min'] = lon_min
            self.valid_dates[d]['lon_max'] = lon_max

        return self.valid_dates


class NCInsituFile():

    def __init__(self, ncpath):
        self.nc = Dataset(ncpath)
        self.valid_dates = {}

        ##parameters to be implemented using a configuration file
        self.time_variable = 'TIME'
        self.time_ref = dt(1950, 1, 1)
        self.time_ref_units = 'days'

        self.lat_variable = 'LATITUDE'
        self.lon_variable = 'LONGITUDE'

        self.variables = ['CPHL']
        self.flag_variable = 'CPHL_QC'
        self.flag_valid_list = ['good_data']

    def get_valid_dates(self, valid_hours, start_date, end_date):
        if valid_hours is None:
            valid_hours = [8, 14]
        time = self.nc.variables[self.time_variable]
        lat = self.nc.variables[self.lat_variable]
        lon = self.nc.variables[self.lon_variable]
        if self.flag_variable is not None:
            flag = self.nc.variables[self.flag_variable]
            flagclass = FLAG_LOIS(flag.flag_values, flag.flag_meanings)

        for idx in range(len(time)):
            time_here = None
            if self.time_ref_units == 'days':
                time_here = self.time_ref + timedelta(days=float(time[idx]))
            if time_here is None:
                print(f'[WARNING] Time is not valid. Skipping...')
                continue
            if time_here < start_date or time_here > end_date:
                if args.verbose:
                    # print(f'[WARNING] Time out of range. Skipping...')
                    continue

            flag_valid = True
            if self.flag_variable is not None:
                flag_value = flag[idx]
                flag_valid = flagclass.is_any_flag_valid(self.flag_valid_list, flag_value)

            if (valid_hours[0] <= time_here.hour <= valid_hours[1]) and flag_valid:
                date_here = time_here.strftime('%Y-%m-%d')
                hour_here = time_here.strftime('%H:%M:%S')
                if not date_here in self.valid_dates.keys():
                    self.valid_dates[date_here] = {
                        hour_here: {
                            'lat': float(lat[idx]),
                            'lon': float(lon[idx])
                        }
                    }
                else:
                    self.valid_dates[date_here][hour_here] = {
                        'lat': float(lat[idx]),
                        'lon': float(lon[idx])
                    }
                for var in self.variables:
                    self.valid_dates[date_here][hour_here][var] = float(self.nc.variables[var][idx])
        return self.valid_dates

    def compute_geo_limits(self):
        for d in self.valid_dates:
            lat_min = 90
            lat_max = -90
            lon_min = 180
            lon_max = -180
            for h in self.valid_dates[d]:
                if self.valid_dates[d][h]['lat'] < lat_min:
                    lat_min = self.valid_dates[d][h]['lat']
                if self.valid_dates[d][h]['lat'] > lat_max:
                    lat_max = self.valid_dates[d][h]['lat']
                if self.valid_dates[d][h]['lon'] < lon_min:
                    lon_min = self.valid_dates[d][h]['lon']
                if self.valid_dates[d][h]['lon'] > lon_max:
                    lon_max = self.valid_dates[d][h]['lon']
            self.valid_dates[d]['lat_min'] = lat_min
            self.valid_dates[d]['lat_max'] = lat_max
            self.valid_dates[d]['lon_min'] = lon_min
            self.valid_dates[d]['lon_max'] = lon_max

        return self.valid_dates


def main():
    print('[INFO] Started')

    # check all the arguments are givem
    mode = 'trim'
    if args.mode:
        mode = args.mode
    if mode == 'trim' and not args.source:
        print(f'[ERROR] Source dir is required for trim mode')
        exit(-1)

    # check arguments
    inputfile = args.inputfile
    if not os.path.exists(inputfile):
        print(f'[ERROR] Input source {inputfile} does not exist')

    if mode == 'trim':
        out_dir = args.output
        if not os.path.exists(out_dir):
            try:
                os.mkdir(out_dir)
            except OSError:
                print(f'[ERROR] Output dir {out_dir} does not exist and nor can be created')
                exit(-1)
        source_dir = args.source
        if not os.path.exists(source_dir):
            print(f'[ERROR] Source dir {source_dir} does not exist')
            exit(-1)
        unzip_path = None
        if args.unzip_path:
            unzip_path = args.unzip_path
            if not os.path.exists(unzip_path):
                unzip_path = None
                print(f'[WARNING] Unzip path: {unzip_path} does not exist')
        if args.verbose:
            print(f'[INFO] Source directory: {source_dir}')
            print(f'[INFO] Output directory: {out_dir}')
            print(f'[INFO] Input file: {inputfile}')

    if mode == 'dfcsv':
        output_file = args.output
        source_data = 'unknown'
        if args.source:
            source_data = args.source
        if args.verbose:
            print(f'[INFO] Output file: {output_file}')
            print(f'[INFO] Input file: {inputfile}')

    res_tag = 'EFR'
    if args.res_tag:
        res_tag = args.res_tag

    start_date = dt.strptime('2016-04-01', '%Y-%m-%d')
    end_date = dt.now()
    if args.startdate:
        try:
            start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] Argument start_date sd: {args.start_date} is not valid')
            exit(-1)
    if args.enddate:
        try:
            end_date = args.enddate
        except ValueError:
            print(f'[ERROR] Argument end_date ed: {args.start_date} is not valid')
            exit(-1)

    if args.verbose:
        sdate = start_date.strftime('%Y-%m-%d')
        edate = end_date.strftime('%Y-%m-%d')
        print(f'[INFO] Start date: {sdate}')
        print(f'[INFO] End date: {edate}')

    if inputfile.endswith('nc'):
        ifobj = NCInsituFile(inputfile)
    if inputfile.endswith('csv'):
        ifobj = CSVInsituFile(inputfile)

    ifobj.get_valid_dates(None, start_date, end_date)
    if mode == 'trim':
        valid_dates = ifobj.compute_geo_limits()
    valid_dates = ifobj.valid_dates

    if mode == 'trim':
        res_list = []
        global_use_unzip_folder = False
        for d in valid_dates:
            dhere = dt.strptime(d, '%Y-%m-%d')
            south = valid_dates[d]['lat_min'] - 0.15
            north = valid_dates[d]['lat_max'] + 0.15
            west = valid_dates[d]['lon_min'] - 0.15
            east = valid_dates[d]['lon_max'] + 0.15
            if args.verbose:
                print('===============================================================================================')
                print(f'[INFO] Date: {dhere}')
            year = dhere.strftime('%Y')
            jday = dhere.strftime('%j')
            source_dir_date = os.path.join(source_dir, year, jday)
            use_unzip_folder = False
            if not os.path.exists(source_dir_date):
                if args.verbose:
                    print(f'[INFO] Source folder {source_dir_date} does not exist. Skipping...')
                continue
            if os.path.exists(source_dir_date):
                for prod in os.listdir(source_dir_date):
                    path_prod = os.path.join(source_dir_date, prod)
                    iszipped = False
                    istar = False
                    if args.verbose:
                        print(f'PRODUCT: {path_prod}')
                    cgeo = CHECK_GEO()
                    prod_started = False
                    if prod.endswith('SEN3') and prod.find(res_tag) > 0 and os.path.isdir(path_prod):
                        cgeo.start_polygon_image_from_folder_manifest_file(path_prod)
                        prod_started = True
                    if prod.endswith('.zip') and prod.find(res_tag) > 0 and zp.is_zipfile(path_prod):
                        iszipped = True
                        if unzip_path is None:
                            print(f'[WARNING] Unzip path is not defined. Skipping...')
                            continue
                        cgeo.start_polygon_image_from_zip_manifest_file(path_prod)
                    if prod.endswith('.tar') and prod.find(res_tag) > 0 and tp.is_tarfile(path_prod):
                        istar = True
                        if unzip_path is None:
                            print(f'[WARNING] Unzip path is not defined. Skipping...')
                            continue
                        cgeo.start_polygon_image_from_tar_manifest_file(path_prod, unzip_path)
                    if iszipped or istar:
                        prod_started = True
                        use_unzip_folder = True
                        global_use_unzip_folder = True
                    if not prod_started:
                        print(f'[WARNING] Product is not valid. Skipping...')
                        continue
                    flag_location = cgeo.check_geo_area(south, north, west, east)
                    if flag_location == -1:
                        print(f'[WARNING] Polygon was not defined in the manifest file. Skipping...')
                        continue
                    if flag_location == 0:
                        print(f'[WARNING] Product does not contain in situ locations. Skipping...')
                        continue

                    if iszipped:
                        path_prod = cgeo.unzip_product(path_prod, unzip_path, args.verbose)
                    if istar:
                        path_prod = cgeo.untar_product(path_prod, unzip_path, args.verbose)
                    pcheck = cgeo.check_uncompressed_product(path_prod, year, jday, args.verbose)
                    if pcheck:
                        prod_output = trimtool.make_trim(south, north, west, east, path_prod, None, False, out_dir,
                                                         args.verbose)
                        sval = path_prod + ';' + os.path.join(out_dir, prod_output)
                        res_list.append(sval)
                    if args.verbose:
                        print('-------------------------------------------------------------------')
            if use_unzip_folder:
                if args.verbose:
                    print(f'Deleting temporary files in unzip folder {unzip_path} for date: {d}')
                cgeo = CHECK_GEO()
                for folder in os.listdir(unzip_path):
                    cgeo.delete_folder_content(os.path.join(unzip_path, folder))
                if args.verbose:
                    print('-------------------------------------------------------------------')

        if args.list_files:
            file_list = os.path.join(out_dir, args.list_files)
            with open(file_list, 'w') as f:
                for row in res_list:
                    f.write(row)
                    f.write('\n')

        if global_use_unzip_folder:
            for folder in os.listdir(unzip_path):
                if folder == 'NOTRIMMED':
                    continue
                path_delete = os.path.join(unzip_path, folder)
                cmd = f'rm -d -f {path_delete}'
                proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
                proc.communicate()

    if mode == 'dfcsv':
        col_names = ['DATE', 'HOUR', 'SOURCE', 'LATITUDE', 'LONGITUDE']
        for var in ifobj.variables:
            col_names.append(var)
        df = pd.DataFrame(columns=col_names)
        index_df = 0
        for d in valid_dates:
            for h in valid_dates[d]:
                if h.startswith('lat') or h.startswith('lon'):
                    continue
                row = {
                    'DATE': d,
                    'HOUR': h,
                    'SOURCE': source_data,
                    'LATITUDE': valid_dates[d][h]['lat'],
                    'LONGITUDE': valid_dates[d][h]['lon']
                }
                for var in ifobj.variables:
                    row[var] = valid_dates[d][h][var]

                dfhere = pd.DataFrame.from_dict(row, orient='index').transpose()
                dfhere.index = [index_df]
                df = pd.concat([df, dfhere])
                index_df = index_df + 1
        df.to_csv(output_file, sep=';')
        if args.verbose:
            print(f'[INFO] Data saved to file: {output_file}')

    # for d in valid_dates:
    #     print(d, '-------------------------')
    #     print(valid_dates[d]['lat_min'])
    #     print(valid_dates[d]['lat_max'])
    #     print(valid_dates[d]['lon_min'])
    #     print(valid_dates[d]['lon_max'])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
