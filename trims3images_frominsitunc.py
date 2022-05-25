import argparse
import configparser
import pandas as pd
import zipfile as zp
import tarfile as tp
from datetime import datetime as dt
from check_geo import CHECK_GEO
import os
import subprocess
import s3olcitrim_bal_frompy as trimtool
from insitu.nc_insitu_file import NCInsituFile
from insitu.csv_insitu_file import CSVInsituFile

parser = argparse.ArgumentParser(
    description="Search and trim S3 Level 1B products around a in-situ locations with valid data")

parser.add_argument('-m', "--mode", help="Mode", choices=['trim', 'dfcsv'])
parser.add_argument('-i', "--inputfile", help="Input nc or csv file", required=True)
parser.add_argument('-s', "--source", help="Source directory", required=True)
parser.add_argument('-o', "--output", help="Output directory (trim mode) or output file (dfcsv mode)", required=True)
parser.add_argument('-z', "--unzip_path", help="Temporal unzip directory")
parser.add_argument('-c', "--configfile", help="Configuration file")
parser.add_argument('-res', "--res_tag", help="Resolution tag (EFR, WFR)")
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD ")
parser.add_argument('-vh', "--validhours", help="Valid min and max hours min-max")
parser.add_argument("-l", "--list_files",
                    help="Optional name for text file with a list of trimmed files (Default: None")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


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
            start_date = dt.strptime(args.startdate, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] Argument start_date sd: {args.startdate} is not valid')
            exit(-1)
    if args.enddate:
        try:
            end_date = args.enddate
        except ValueError:
            print(f'[ERROR] Argument end_date ed: {args.enddate} is not valid')
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
        if args.configfile:
            ifobj.set_params_fromconfig_file(args.configfile)

    valid_hours = None
    if args.validhours:
        try:
            vdsplit = args.validhours.split('_')
            valid_hours = [int(vdsplit[0]), int(vdsplit[1])]
        except:
            print(f'[WARNING] Argument valid_hours (vd) is not correct. Using defaults values (complete day: 0-24)')
            pass

    ifobj.get_valid_dates(valid_hours, start_date, end_date)
    if mode == 'trim':
        ifobj.compute_geo_limits()
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
                        print('-------------------------------------------------------------------')
                        print(f'[INFO]PRODUCT: {path_prod}')
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
