import argparse
from datetime import datetime as dt
import os
import zipfile as zp
import tarfile as tp
from skie.skie_csv import SKIE_CSV
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from netCDF4 import Dataset
import shutil
import subprocess
import s3olcitrim_bal_frompy as trimtool

parser = argparse.ArgumentParser(description="Search and trim S3 Level 1B products around a Skie trajectory")
parser.add_argument('-i', "--inputskie", help="Input Skie csv file", required=True)
parser.add_argument('-s', "--sourcedir", help="Source directory", required=True)
parser.add_argument('-o', "--outputdir", help="Output directory", required=True)
parser.add_argument('-z', "--unzip_path", help="Temporal unzip directory")
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD ")
parser.add_argument("-l", "--list_files",
                    help="Optional name for text file with a list of trimmed files (Default: None")
parser.add_argument('-res', "--res_tag", help="Resolution tag (EFR, WFR)")
parser.add_argument('-mtime', "--max_diff_time", help="Maximum time difference between satellite and spectra (minutes)")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")

args = parser.parse_args()


def main():
    print('[INFO]Started')
    if not os.path.exists(args.inputskie):
        print(f'[ERROR] Input SKIE csv file: {args.inputskie} does not exist')
        return
    start_date = dt(2016, 4, 1)
    end_date = dt.now()
    if args.startdate:
        try:
            start_date = dt.strptime(args.startdate, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] Start date: {args.startdate} is not in the correct format (%Y-%m-%d')
            return
    if args.enddate:
        try:
            end_date = dt.strptime(args.enddate, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] End date: {args.enddate} is not in the correct format (%Y-%m-%d')
            return
    if end_date<start_date:
        print(f'[ERROR] End date: {end_date} must be after start date: {start_date}')
        return
    if args.sourcedir:
        source_dir = args.sourcedir
    if not os.path.exists(source_dir):
        print(f'[ERROR] Source dir: {source_dir} does not exist')
        return
    out_dir_site = get_output_path()
    if out_dir_site is None:
        return

    unzip_path = get_unzip_path()
    if not os.path.exists(unzip_path):
        print(f'[ERROR] Unzip path: {unzip_path} does not exist')
        return

    nminutes = 120
    if args.max_diff_time:
        try:
            nminutes = int(args.max_diff_time)
        except ValueError:
            print(f'[ERROR] Max diff time: {args.max_diff_time} is not correct, it must be a integer value')
            return

    max_diff_time = nminutes * 60

    skie_file = SKIE_CSV(args.inputskie)
    ndates = skie_file.start_list_dates()
    if args.verbose:
        print(f'[INFO] Dates in Skie csv file: {ndates}')
    if ndates == 0:
        print(f'[ERROR] Dates were not retrieved from SKIE csv file: {args.inputskie}. Check name of columnt time')
        return

    # sat_time = dt(2019,9,16)
    # dftal = skie_file.get_subdf(sat_time,sat_time,120)


    res_list = []

    for d in skie_file.dates:
        dateskie = dt.strptime(d, '%Y%m%d')
        if dateskie < start_date or dateskie > end_date:
            continue
        if args.verbose:
            print('--------------------------------------------------------------')
            print(f'[INFO]DATE: {dateskie}')
        year = dateskie.strftime('%Y')
        jday = dateskie.strftime('%j')
        source_dir_date = os.path.join(source_dir, year, jday)
        if os.path.exists(source_dir_date):
            for prod in os.listdir(source_dir_date):
                path_prod = os.path.join(source_dir_date, prod)
                sat_time = get_sat_time_from_fname(path_prod)
                if sat_time is None:
                    print(f'[WARNING] Sat time for product: {prod} is not valid')
                    continue

                has_data_intime = skie_file.get_subdf(sat_time, sat_time, max_diff_time)

                if args.verbose:
                    print('----------------------------')
                    print(f'[INFO]PRODUCT: {path_prod}')
                if not has_data_intime:
                    continue
                lat_points, lon_points = skie_file.get_lat_lon_from_subdb()
                flag_location, geolimits, iszipped, istar = get_flag_location(path_prod, lat_points, lon_points)

                if flag_location == -1:
                    if args.verbose:
                        print(f'[WARNING]Product is not valid (SEN3)')
                    continue
                if flag_location == 0:
                    if args.verbose:
                        print(f'[WARNING]Product does not contain any point of the trajectory')
                    continue
                if flag_location >= 1:
                    if iszipped:
                        path_prod_u = uncompress_zip_file(path_prod)
                    elif istar:
                        path_prod_u = uncompress_tar_file(path_prod)
                    else:
                        path_prod_u = path_prod
                    pcheck = check_uncompressed_product(path_prod_u, year, jday)
                    if not pcheck:
                        if args.verbose:
                            print(f'[ERROR] Product can not be trimmed. Saved to NOTRIMMED folder')
                    else:
                        if args.verbose:
                            print(f'[INFO] Points inside the scene: {flag_location}')
                            print(f'[INFO] Trimming to coordinates: {geolimits}')
                        prod_output = trimtool.make_trim(geolimits[0], geolimits[1], geolimits[2], geolimits[3], path_prod_u, None, False, out_dir_site,
                                                         args.verbose)
                        sval = path_prod + ';' + os.path.join(out_dir_site, prod_output)
                        res_list.append(sval)

        if os.path.exists(unzip_path) and os.path.isdir(unzip_path):
            if args.verbose:
                print(f'[INFO]Deleting temporary files in unzip folder {unzip_path} for date: {d}')
            for folder in os.listdir(unzip_path):
                delete_folder_content(os.path.join(unzip_path, folder))

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

    print(f'[INFO]COMPLETED. Trimmed files: {len(res_list)}')


def uncompress_zip_file(path_prod):
    unzip_path = get_unzip_path()
    with zp.ZipFile(path_prod, 'r') as zprod:
        if args.verbose:
            print(f'[INFO] Unzipping to: {unzip_path}')
        zprod.extractall(path=unzip_path)
    path_prod_u = path_prod.split('/')[-1][0:-4]
    if not path_prod_u.endswith('.SEN3'):
        path_prod_u = path_prod_u + '.SEN3'
    path_prod_u = os.path.join(unzip_path, path_prod_u)
    return path_prod_u


def uncompress_tar_file(path_prod):
    unzip_path = get_unzip_path()
    with tp.TarFile(path_prod, 'r') as tprod:
        if args.verbose:
            print(f'Untar to: {unzip_path}')
        tprod.extractall(path=unzip_path)
    path_prod_u = path_prod.split('/')[-1][0:-4]
    if not path_prod_u.endswith('.SEN3'):
        path_prod_u = path_prod_u + '.SEN3'
    path_prod_u = os.path.join(unzip_path, path_prod_u)
    return path_prod_u

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


def get_flag_location(path_prod, lat_points, lon_points):
    res_tag = 'EFR'
    if args.res_tag:
        res_tag = args.res_tag

    unzip_path = get_unzip_path()

    prod = path_prod.split('/')[-1]
    iszipped = False
    istar = False
    flag_location = -1
    geolimits = None

    if prod.endswith('SEN3') and prod.find(res_tag) > 0 and os.path.isdir(path_prod):
        geoname = os.path.join(path_prod, 'xfdumanifest.xml')
        if os.path.exists(geoname):
            fgeo = open(geoname, 'r')
            for line in fgeo:
                line_str = line.strip()
                if line_str.startswith('<gml:posList>'):
                    flag_location, geolimits = get_flag_location_from_line_geo(line_str, lat_points, lon_points)
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
                        flag_location, geolimits = get_flag_location_from_line_geo(line_str, lat_points, lon_points)
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
                                flag_location, geolimits = get_flag_location_from_line_geo(line.strip(), lat_points,
                                                                                           lon_points)

    return flag_location, geolimits, iszipped, istar


def get_flag_location_from_line_geo(line_str, lat_points, lon_points):
    clist = line_str[len('<gml:posList>'):line_str.index('</gml:posList>')].split()
    coords = []
    for i in range(0, len(clist), 2):
        coord_here = (float(clist[i + 1]), float(clist[i]))
        coords.append(coord_here)
    polygon_image = Polygon(coords)  # create polygon
    flag_location = 0
    west = 180
    east = -180
    south = 90
    north = -90
    for index in range(len(lat_points)):
        point_site = Point(lon_points[index], lat_points[index])
        if point_site.within(polygon_image):
            flag_location = flag_location + 1
            if lat_points[index] < south:
                south = lat_points[index]
            if lat_points[index] > north:
                north = lat_points[index]
            if lon_points[index] < west:
                west = lon_points[index]
            if lon_points[index] > east:
                east = lon_points[index]
    geolimits = None
    if flag_location >= 1:
        south = south - 0.15
        north = north + 0.15
        west = west - 0.15
        east = east + 0.15
        geolimits = [south, north, west, east]

    return flag_location, geolimits


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


def get_unzip_path():
    if args.unzip_path:
        unzip_path = args.unzip_path
    else:
        out_dir_site = get_output_path()
        unzip_path = os.path.join(out_dir_site,'UNZIPPED_TMP')
        if not os.path.exists(unzip_path):
            os.mkdir(unzip_path)
    return unzip_path


def get_output_path():
    if args.outputdir:
        out_dir = args.outputdir
    else:
        print('ERROR: Output dir is required')
        return None

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    res_tag = 'EFR'
    if args.res_tag:
        res_tag = args.res_tag

    if res_tag == 'EFR':
        out_dir_site = os.path.join(out_dir, 'trim')
    elif res_tag == 'WFR':
        out_dir_site_w = os.path.join(out_dir, 'WFR')
        if not os.path.exists(out_dir_site_w):
            os.mkdir(out_dir_site_w)
        out_dir_site = os.path.join(out_dir_site_w, 'results')
    else:
        out_dir_site = os.path.join(out_dir, res_tag)
    if not os.path.exists(out_dir_site):
        os.mkdir(out_dir_site)

    if args.verbose:
        print(f'Output directory: {out_dir_site}')

    return out_dir_site

def delete_folder_content(path_folder):
    res = True
    for f in os.listdir(path_folder):
        try:
            os.remove(os.path.join(path_folder, f))
        except OSError:
            res = False
    return res

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
