import argparse
import configparser
import os
import subprocess
import shutil

import numpy as np
from netCDF4 import Dataset

import s3olcitrim_bal_frompy as trimtool
import zipfile as zp
import tarfile as tp

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

parser = argparse.ArgumentParser(description="Search and trim S3 Level 1B products around a in-situ location")
parser.add_argument('-site', "--site",
                    help="Aeronet site (Baltic: Gustav_Dalen_Tower, Helsinki_Lighthouse, Irbe_Lighthouse")
parser.add_argument('-ilat', "--insitu_lat", help="In situ lat")
parser.add_argument('-ilon', "--insitu_lon", help="In situ lon")
parser.add_argument('-anc', "--anet_nc_file", help="Aeronet NC File")
parser.add_argument('-fsit', "--sites_file", help="Sites File")
parser.add_argument('-s', "--sourcedir", help="Output directory")
parser.add_argument('-o', "--outputdir", help="Output directory", required=True)
parser.add_argument('-z', "--unzip_path", help="Temporal unzip directory")
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD ")
parser.add_argument('-res', "--res_tag", help="Resolution tag (EFR, WFR)", )
parser.add_argument("-l", "--list_files",
                    help="Optional name for text file with a list of trimmed files (Default: None")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")

args = parser.parse_args()

# ANET_SOURCE_DIR = '/mnt/d/LUIS/OCTAC_WORK/BALTIC'
# file_sites = '/mnt/d/LUIS/OCTAC_WORK/BALTIC/site_list.ini'
# source_dir = '/mnt/d/LUIS/OCTAC_WORK/BALTIC'


from base.anet_nc_reader import AERONETReader


def main():
    print('STARTED')
    ANET_SOURCE_DIR = '/store3/HYPERNETS/INSITU_AOC/NC/'
    source_dir = '/dst04-data1/OC/OLCI/trimmed_sources'
    file_sites = '/store3/HYPERNETS/INSITU_AOC/site_list.ini'
    unzip_path = '/home/Luis.Gonzalezvilas/TEMPDATA/unzip_folder'
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

    res_tag = 'EFR'
    if args.res_tag:
        res_tag = args.res_tag

    point_site = Point(insitu_lon, insitu_lat)
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
    out_dir_site_t = os.path.join(out_dir, site)
    if not os.path.exists(out_dir_site_t):
        os.mkdir(out_dir_site_t)
    if res_tag == 'EFR':
        out_dir_site = os.path.join(out_dir_site_t, 'trim')
    elif res_tag == 'WFR':
        out_dir_site_w = os.path.join(out_dir_site_t, 'WFR')
        if not os.path.exists(out_dir_site_w):
            os.mkdir(out_dir_site_w)
        out_dir_site = os.path.join(out_dir_site_w, 'results')
    else:
        out_dir_site = os.path.join(out_dir_site_t, res_tag)
    if not os.path.exists(out_dir_site):
        os.mkdir(out_dir_site)

    if args.verbose:
        print(f'Output directory: {out_dir_site}')

    if args.unzip_path:
        unzip_path = args.unzip_path
        print(f'Unzip temporal path: {unzip_path}')

    file_nc = None
    if args.anet_nc_file:
        file_nc = args.anet_nc_file
    else:
        if not site is None and os.path.exists(ANET_SOURCE_DIR):
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
        if args.verbose:
            print()
            print(f'DATE: {d}')
            print()
        year = d.strftime('%Y')
        jday = d.strftime('%j')
        source_dir_date = os.path.join(source_dir, year, jday)
        if os.path.exists(source_dir_date):
            for prod in os.listdir(source_dir_date):
                path_prod = os.path.join(source_dir_date, prod)
                iszipped = False
                istar = False
                if args.verbose:
                    print(f'PRODUCT: {path_prod}')
                flag_location = -1
                if prod.endswith('SEN3') and prod.find(res_tag) > 0 and os.path.isdir(path_prod):
                    geoname = os.path.join(path_prod, 'xfdumanifest.xml')
                    if os.path.exists(geoname):
                        fgeo = open(geoname, 'r')
                        for line in fgeo:
                            line_str = line.strip()
                            if line_str.startswith('<gml:posList>'):
                                flag_location = get_flag_location_from_line_geo(line_str, point_site)
                        fgeo.close()

                if prod.endswith('.zip') and prod.find(res_tag) > 0 and zp.is_zipfile(path_prod):
                    iszipped = True
                    with zp.ZipFile(path_prod, 'r') as zprod:
                        fname = path_prod.split('/')[-1][0:-4]
                        if not fname.endswith('SEN3'):
                            fname = fname + '.SEN3'
                        geoname = os.path.join(fname, 'xfdumanifest.xml')
                        if geoname in zprod.namelist():
                            gc = zprod.open(geoname)
                            for line in gc:
                                line_str = line.decode().strip()
                                if line_str.startswith('<gml:posList>'):
                                    flag_location = get_flag_location_from_line_geo(line_str, point_site)
                            gc.close()

                if prod.endswith('.tar') and prod.find(res_tag) > 0 and tp.is_tarfile(path_prod):
                    istar = True
                    with tp.TarFile(path_prod, 'r') as tprod:
                        fname = path_prod.split('/')[-1][0:-4]
                        if not fname.endswith('SEN3'):
                            fname = fname + '.SEN3'
                        geoname = os.path.join(fname, 'xfdumanifest.xml')
                        for member in tprod.getmembers():
                            if member.name == geoname:
                                tprod.extract(member, path=unzip_path)
                                if os.path.exists(os.path.join(unzip_path, geoname)):
                                    fgeo = open(os.path.join(unzip_path, geoname))
                                    for line in fgeo:
                                        if line.strip().startswith('<gml:posList>'):
                                            flag_location = get_flag_location_from_line_geo(line.strip(), point_site)

                if flag_location == -1:
                    if args.verbose:
                        print(f'Product is not valid (SEN3, Level 1B or Baltic)')
                if flag_location == 0:
                    if args.verbose:
                        print(f'Product does not contain site location: {site}')
                if flag_location == 1:
                    if iszipped:
                        # path_temporal = '/mnt/c/data_luis/octac_work/bal_evolution/examples/unzip_folder'
                        with zp.ZipFile(path_prod, 'r') as zprod:
                            if args.verbose:
                                print(f'Unziping to: {unzip_path}')
                            zprod.extractall(path=unzip_path)
                        path_prod_u = path_prod.split('/')[-1][0:-4]
                        if not path_prod_u.endswith('.SEN3'):
                            path_prod_u = path_prod_u + '.SEN3'
                        path_prod_u = os.path.join(unzip_path, path_prod_u)
                        if args.verbose:
                            print(f'Trimming product for site: {site}...')
                        pcheck = check_uncompressed_product(path_prod_u, year, jday)
                        if not pcheck:
                            if args.verbose:
                                print(f'[ERROR] Product can not be trimmed. Saved to NOTRIMMED folder')
                        else:
                            prod_output = trimtool.make_trim(s, n, w, e, path_prod_u, None, False, out_dir_site,
                                                             args.verbose)
                            sval = path_prod + ';' + os.path.join(out_dir_site, prod_output)
                            res_list.append(sval)
                    elif istar:
                        with tp.TarFile(path_prod, 'r') as tprod:
                            if args.verbose:
                                print(f'Untar to: {unzip_path}')
                            tprod.extractall(path=unzip_path)
                        path_prod_u = path_prod.split('/')[-1][0:-4]
                        if not path_prod_u.endswith('.SEN3'):
                            path_prod_u = path_prod_u + '.SEN3'
                        path_prod_u = os.path.join(unzip_path, path_prod_u)
                        if args.verbose:
                            print(f'Trimming product for site: {site}...')
                        pcheck = check_uncompressed_product(path_prod_u, year, jday)
                        if not pcheck:
                            if args.verbose:
                                print(f'[ERROR] Product can not be trimmed. Saved to NOTRIMMED folder')
                        else:
                            prod_output = trimtool.make_trim(s, n, w, e, path_prod_u, None, False, out_dir_site,
                                                             args.verbose)
                            sval = path_prod + ';' + os.path.join(out_dir_site, prod_output)
                            res_list.append(sval)
                    else:
                        pcheck = check_uncompressed_product(path_prod, year, jday)
                        if not pcheck:
                            if args.verbose:
                                print(f'[ERROR] Product can not be trimmed. Saved to NOTRIMMED folder')
                        else:
                            prod_output = trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir_site,
                                                             args.verbose)
                            sval = path_prod + ';' + os.path.join(out_dir_site, prod_output)
                            res_list.append(sval)

                if args.verbose:
                    print('-------------------------------------------------------------------')
        if os.path.exists(unzip_path) and os.path.isdir(unzip_path):
            if args.verbose:
                print(f'Deleting temporary files in unzip folder {unzip_path} for date: {d}')
            for folder in os.listdir(unzip_path):
                delete_folder_content(os.path.join(unzip_path, folder))

            if args.verbose:
                print('-------------------------------------------------------------------')
    if args.list_files:
        file_list = os.path.join(out_dir_site, args.list_files)
        with open(file_list, 'w') as f:
            for row in res_list:
                f.write(row)
                f.write('\n')

    if os.path.exists(unzip_path) and os.path.isdir(unzip_path):
        for folder in os.listdir(unzip_path):
            if folder == 'NOTRIMMED':
                continue
            path_delete = os.path.join(unzip_path, folder)
            cmd = f'rm -d -f {path_delete}'
            proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
            proc.communicate()

    print(f'COMPLETED. Trimmed files: {len(res_list)}')


# Check if netCDF4 is able to read nc file
def check_uncompressed_product(path_product, year, jday):
    try:
        for f in os.listdir(path_product):
            if f.endswith('nc'):
                Dataset(os.path.join(path_product, f))
        return True
    except OSError:
        if args.verbose:
            print(f'[INFO] Checking path: {path_product}')
        name_dir = path_product.split(('/'))[-1]
        path_base = os.path.dirname(path_product)
        path_no_trimmed = os.path.join(path_base, 'NOTRIMMED')
        path_year = os.path.join(path_no_trimmed, year)
        path_jday = os.path.join(path_year, jday)
        path_end = os.path.join(path_jday, name_dir)
        if not os.path.exists(path_end):
            os.makedirs(path_end)

        if args.verbose:
            print(f'[INFO] Path created: {path_end}')
        for f in os.listdir(path_product):
            shutil.copyfile(os.path.join(path_product, f), os.path.join(path_end, f))
        return False


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


def delete_folder_content(path_folder):
    res = True
    for f in os.listdir(path_folder):
        try:
            os.remove(os.path.join(path_folder, f))
        except OSError:
            res = False
    return res


def get_flag_location_from_line_geo(line_str, point_site):
    clist = line_str[len('<gml:posList>'):line_str.index('</gml:posList>')].split()
    coords = []
    for i in range(0, len(clist), 2):
        coord_here = (float(clist[i + 1]), float(clist[i]))
        coords.append(coord_here)
    polygon_image = Polygon(coords)  # create polygon
    if point_site.within(polygon_image):
        flag_location = 1
    else:
        flag_location = 0
    return flag_location


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# path_geo = os.path.join(path_prod, 'geo_coordinates.nc')
# if os.path.exists(path_geo):
#     dset = Dataset(path_geo)
#     latArray = dset.variables['latitude'][:, :]
#     lonArray = dset.variables['longitude'][:, :]
#     flag_location = check_location(latArray, lonArray, insitu_lat, insitu_lon)
