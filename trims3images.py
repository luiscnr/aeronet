import os.path
import s3olcitrim_bal_frompy as trimtool
import configparser
import csv
from netCDF4 import Dataset
import numpy as np
import argparse

parser = argparse.ArgumentParser(
    description="Search and trim S3 Level 1B products around a in-situ location")
parser.add_argument('-site', "--site",
                    help="Aeronet site (Baltic: Gustav_Dalen_Tower, Helsinki_Lighthouse, Irbe_Lighthouse")
parser.add_argument('-ilat', "--insitu_lat", help="In situ lat")
parser.add_argument('-ilon', "--insitu_lon", help="In situ lon")
parser.add_argument('-anc', "--anet_nc_file", help="Aeronet NC File")
parser.add_argument('-fsit', "--sites_file", help="Sites File")
parser.add_argument('-s', "--sourcedir", help="Output directory")
parser.add_argument('-o', "--outputdir", help="Output directory", required=False)
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD ")
parser.add_argument("-l", "--createlist", help="Create list", action="store_true")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")

args = parser.parse_args()

# ANET_SOURCE_DIR = '/mnt/d/LUIS/OCTAC_WORK/BALTIC'
# file_sites = '/mnt/d/LUIS/OCTAC_WORK/BALTIC/site_list.ini'
# source_dir = '/mnt/d/LUIS/OCTAC_WORK/BALTIC'

ANET_SOURCE_DIR = '/store3/HYPERNETS/INSITU_AOC/NC/'

source_dir = '/dst04-data1/OC/OLCI/trimmed_sources'
file_sites = '/store3/HYPERNETS/INSITU_AOC/site_list.ini'

from base.anet_nc_reader import AERONETReader


def main():
    print('STARTED')
    if args.sites_file:
        file_sites = args.sites_file
    site = None
    if args.insitu_lat and args.insitu_lon:
        insitu_lat = args.insitu_lat
        insitu_lon = args.insitu_lon
        if args.verbose:
            print(f'LOCATION: latitude:{insitu_lat}, longitude:{insitu_lon}')
    else:
        if args.site:
            site = args.site
        else:
            site = 'Gustav_Dalen_Tower'

        if not os.path.exists(file_sites):
            print(f'ERROR: Sites file: {file_sites} does not exist')
            return
        options = configparser.ConfigParser()
        options.read(file_sites)
        insitu_lat = float(options[site]['Latitude'])
        insitu_lon = float(options[site]['Longitude'])
        if args.verbose:
            print(f'SITE: {site} latitude:{insitu_lat}, longitude:{insitu_lon}')
    w = insitu_lon - 0.15
    e = insitu_lon + 0.15
    s = insitu_lat - 0.15
    n = insitu_lat + 0.15

    if args.outputdir:
        out_dir = args.outputdir
    else:
        print('ERROR: Output dir is required')

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    out_dir_site = os.path.join(out_dir, site)
    if not os.path.exists(out_dir_site):
        os.mkdir(out_dir_site)
    if args.verbose:
        print(f'Output directory: {out_dir_site}')

    file_nc = None
    if args.anet_nc_file:
        file_nc = args.anet_nc_file
    else:
        if not site is None and os.path.exists(ANET_SOURCE_DIR):
            # file_nc = '/store3/HYPERNETS/INSITU_AOC/NC/20020101_20220129_Gustav_Dalen_Tower.LWN_lev20_15.nc'
            # file_nc = '/mnt/d/LUIS/OCTAC_WORK/BALTIC/20020101_20220129_Gustav_Dalen_Tower.LWN_lev20_15.nc'
            for f in os.listdir(ANET_SOURCE_DIR):
                if f.find(site) > 0:
                    file_nc = os.path.join(ANET_SOURCE_DIR, f)
                    break
    if file_nc is None:
        print(f'ERROR: Aeronet NC file is not available')
        return
    if not os.path.exists(file_nc):
        print(f'ERROR: Aernote NC file: {file_nc} does not exist')
        return
    if args.verbose:
        print(f'Aeronet NC file: {file_nc}')

    if args.sourcedir:
        source_dir = args.sourcedir

    if args.verbose:
        print(f'Source directory: {source_dir}')

    areader = AERONETReader(file_nc)
    start_date = '2016-04-01'
    end_date = None
    if args.startdate:
        start_date = args.startdate
    if args.enddate:
        end_date = args.enddate
    date_list = areader.get_available_dates(start_date, end_date)
    res_list = []
    for d in date_list:
        year = d.strftime('%Y')
        jday = d.strftime('%j')
        source_dir_date = os.path.join(source_dir, year, jday)
        if os.path.exists(source_dir_date):
            for prod in os.listdir(source_dir_date):
                path_prod = os.path.join(source_dir_date, prod)
                flag_location = -1
                if prod.endswith('SEN3') and prod.find('EFR') > 0 and prod.find('BAL') > 0 and os.path.isdir(path_prod):
                    path_geo = os.path.join(path_prod, 'geo_coordinates.nc')
                    if os.path.exists(path_geo):
                        dset = Dataset(path_geo)
                        latArray = dset.variables['latitude'][:, :]
                        lonArray = dset.variables['longitude'][:, :]
                        flag_location = check_location(latArray, lonArray, insitu_lat, insitu_lon)

                if flag_location == -1:
                    if args.verbose:
                        print(f'Product: {prod} is not valid (SEN3, Level 1B or Baltic)')
                    continue
                if flag_location == 0:
                    if args.verbose:
                        print(f'Product {prod} does not contain site location: {site}')
                    continue
                if flag_location == 1:
                    if args.verbose:
                        print(f'Trimming product: {prod} for site: {site}')
                    prod_output = trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir_site, args.verbose)
                    sval = path_prod + ';' + os.path.join(out_dir_site, prod_output)
                    res_list.append(sval)

    if args.createlist:
        file_list = os.path.join(out_dir_site, 'TrimList.txt')
        with open(file_list, 'w') as f:
            for row in res_list:
                f.write(row)
                f.write('\n')

    print(f'COMPLETED. Trimmed files: {len(res_list)}')


def check_location(latArray, lonArray, in_situ_lat, in_situ_lon):
    r, c = find_row_column_from_lat_lon(latArray, lonArray, in_situ_lat, in_situ_lon)
    check_flag = 0
    if 12 <= r < (latArray.shape[0] - 12) and 12 <= c < (latArray.shape[1] - 12):
        check_flag = 1
    return check_flag


def contain_location(latArray, lonArray, in_situ_lat, in_situ_lon):
    if latArray.min() <= in_situ_lat <= latArray.max() and lonArray.min() <= in_situ_lon <= lonArray.max():
        contain_flag = 1
    else:
        contain_flag = 0

    return contain_flag


def find_row_column_from_lat_lon(latArray, lonArray, in_situ_lat, in_situ_lon):
    if contain_location(latArray, lonArray, in_situ_lat, in_situ_lon):
        dist_squared = (latArray - in_situ_lat) ** 2 + (lonArray - in_situ_lon) ** 2
        r, c = np.unravel_index(np.argmin(dist_squared),
                                lonArray.shape)  # index to the closest in the latitude and longitude arrays
    else:
        r = -1
        c = -1
    return r, c


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
