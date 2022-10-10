from datetime import datetime as dt
from datetime import timedelta
import subprocess
import os
import zipfile as zp
from shapely.geometry.polygon import Polygon
import s3olcitrim_bal_frompy as trimtool
import argparse
from shapely.geometry import Point
from check_geo import CHECK_GEO

parser = argparse.ArgumentParser(description="Search and trim S3 Level 1B products around a in-situ location")
parser.add_argument('-s', "--sourcedir", help="Source directory")
parser.add_argument('-p', "--sourceproduct", help="Source directory")
parser.add_argument('-o', "--outputdir", help="Output directory", required=True)
parser.add_argument('-geo', "--geo_limits", help="Geo limits", required=True)
parser.add_argument('-fdates', "--list_dates", help="Date list")
parser.add_argument('-wce', "--wce", help="Wild card expression")
parser.add_argument('-sd', "--start_date", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--end_date", help="The End Date - format YYYY-MM-DD ")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


def main():
    ##TO CHECK REMAINING IDEPIX FILE
    # input_path = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Helsinki_Lighthouse/trim'
    # output_path = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Helsinki_Lighthouse/IDEPIX/results'
    # b = check(input_path, output_path)
    # if b:
    #     return
    ####################
    geo_coords = args.geo_limits
    if geo_coords == 'BAL':
        s = 53.25
        n = 65.85
        w = 9.25
        e = 30.25
    elif geo_coords == 'BAL_GDT':
        s = 58
        n = 59
        w = 17
        e = 18
        insitu_lat = 58.59417
        insitu_lon = 17.46683
    elif geo_coords == 'BAL_HLH':
        s = 59.5
        n = 60.5
        w = 24.5
        e = 25.5
        insitu_lat = 59.94897
        insitu_lon = 24.92636
    elif geo_coords == 'BAL_ILH':
        s = 57.25
        n = 58.25
        w = 21.25
        e = 22.25
        insitu_lat = 57.75092
        insitu_lon = 21.72297
    elif geo_coords == 'TRASIMENO':
        w = 11.6
        e = 12.6
        s = 42.6
        n = 43.6
        insitu_lat = 43.12278
        insitu_lon = 12.13306
    else:
        geo_list = geo_coords.split('_')
        s = float(geo_list[0].strip())
        n = float(geo_list[1].strip())
        w = float(geo_list[2].strip())
        e = float(geo_list[3].strip())
        insitu_lon = (w + e) / 2
        insitu_lat = (s + n) / 2

    ##trim for a list of files saved in a directory yyyy/jjj
    if args.start_date and args.end_date and args.sourcedir:
        start_date, end_date = get_dates()
        if start_date == 'ERROR_DATE':
            return
        input_dir = args.sourcedir
        output_dir = args.outputdir
        do_trim_dates(input_dir, output_dir, start_date, end_date, [s, n, w, e])
        exit(0)

    point_site = Point(insitu_lon, insitu_lat)
    out_dir = args.outputdir
    if args.sourceproduct:
        path_prod = args.sourceproduct
        trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir, args.verbose)
    elif args.sourcedir:
        if not args.list_dates:
            for path in os.listdir(args.sourcedir):
                path_prod = os.path.join(args.sourcedir, path)
                print(f'[INFO] Trimming product: {path_prod}')
                trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir, args.verbose)
        else:
            wce = 'EFR'
            if args.wce:
                wce = args.wce
            file = open(args.list_dates)
            for line in file:
                date_here = dt.strptime(line.strip(), '%Y-%m-%d')
                path_year = os.path.join(args.sourcedir, date_here.strftime('%Y'))
                path_jday = os.path.join(path_year, date_here.strftime('%j'))
                if os.path.exists(path_jday):
                    for name in os.listdir(path_jday):
                        path_prod = os.path.join(path_jday, name)
                        if path_prod.find(wce) > 0:
                            flag_location = check_prod_site(path_prod, point_site)
                            if flag_location == 1:
                                print(f'[INFO] Trimming product: {path_prod}')
                                trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir, args.verbose)
                            else:
                                print(f'[INFO] Product {path_prod} does not contain flag location')
                else:
                    print(f'[WARNING] Path for date: {line} -> {path_jday} was not found in source directory')


def do_trim_dates(input_dir, output_dir, start_date, end_date, geo_limits):
    wce = None
    if args.wce:
        wce = args.wce

    # TEMPORAL DIR FOR ZIP
    temporal_dir = os.path.join(output_dir, 'TMP_UNZIPPED')
    if not os.path.exists(temporal_dir):
        os.mkdir(temporal_dir)

    s = geo_limits[0]
    n = geo_limits[1]
    w = geo_limits[2]
    e = geo_limits[3]

    date_here = start_date
    while date_here <= end_date:
        if args.verbose:
            print(f'[INFO] Working with date: {date_here}')
        year_str = date_here.strftime('%Y')
        day_str = date_here.strftime('%j')
        input_path_date = os.path.join(input_dir, year_str, day_str)
        if os.path.exists(input_path_date):
            output_path_year = os.path.join(output_dir, year_str)
            if not os.path.exists(output_path_year):
                os.mkdir(output_path_year)
            output_path_jday = os.path.join(output_path_year, day_str)
            if not os.path.exists(output_path_jday):
                os.mkdir(output_path_jday)
            if args.verbose:
                print('*************************************************')
            for f in os.listdir(input_path_date):
                prod_path = os.path.join(input_path_date, f)
                prod_path_u, delete = check_path_prod(f, prod_path, wce, geo_limits, temporal_dir)
                if prod_path_u is not None:
                    if args.verbose:
                        print(f'[INFO] Trimming product: {prod_path_u}')
                    trimtool.make_trim(s, n, w, e, prod_path_u, None, False, output_path_jday, args.verbose)
                    if delete:
                        if args.verbose:
                            print(f'[INFO] Deleting unzipped path prod {prod_path_u}')
                        cmd = f'rm -rf {prod_path_u}'
                        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
                        prog.communicate()
        date_here = date_here + timedelta(hours=24)

    if args.verbose:
        print(f'[INFO] Deleting temporary paths')
    for f in os.listdir(temporal_dir):
        tpath = os.path.join(temporal_dir,f)
        if os.path.isdir(tpath):
            cmd = f'rmdir {tpath}'
            prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
            prog.communicate()
    cmd = f'rmdir {temporal_dir}'
    prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
    prog.communicate()

    print('[INFO] TRIM COMPLETED')

def check_path_prod(f, prod_path, wce, geo_limits, temporal_dir):
    prod_path_u = None
    delete = False
    if wce is None:
        if os.path.isdir(prod_path) and f.endswith('.SEN3'):
            cgeo = check_geo_limits(prod_path, geo_limits, False)
            if cgeo == 1:
                prod_path_u = prod_path
            elif cgeo == 0:
                prod_path_u = 'OUT'
        if not os.path.isdir(prod_path) and f.endswith('.zip') > 0:
            if args.verbose:
                print(f'[INFO] Working with zip path: {prod_path}')
            cgeo = check_geo_limits(prod_path, geo_limits, True)
            if cgeo == 1:
                with zp.ZipFile(prod_path, 'r') as zprod:
                    if args.verbose:
                        print(f'[INFO] Unziping {f} to {temporal_dir}')
                    zprod.extractall(path=temporal_dir)
                fname = f[0:-4]
                if not fname.endswith('.SEN3'):
                    fname = fname + '.SEN3'
                prod_path_u = os.path.join(temporal_dir, fname)
                delete = True
            elif cgeo == 0:
                prod_path_u = 'OUT'
    else:
        if os.path.isdir(prod_path) and f.endswith('.SEN3') and f.find(wce) > 0:
            cgeo = check_geo_limits(prod_path, geo_limits, False)
            if cgeo == 1:
                prod_path_u = prod_path
            elif cgeo == 0:
                prod_path_u = 'OUT'
        if not os.path.isdir(prod_path) and f.endswith('.zip') and f.find(wce) > 0:
            if args.verbose:
                print(f'[INFO] Working with zip path: {prod_path}')
            cgeo = check_geo_limits(prod_path, geo_limits, True)
            if cgeo == 1:
                with zp.ZipFile(prod_path, 'r') as zprod:
                    if args.verbose:
                        print(f'[INFO] Unziping {f} to {temporal_dir}')
                    zprod.extractall(path=temporal_dir)
                fname = f[0:-4]
                if not fname.endswith('.SEN3'):
                    fname = fname + '.SEN3'
                prod_path_u = os.path.join(temporal_dir, fname)
                delete = True
            elif cgeo == 0:
                prod_path_u = 'OUT'

    if prod_path_u is None:
        print(f'[WARNING] Product: {prod_path} is not valid. Skiping...')
        return None, False
    if prod_path_u == 'OUT':
        print(f'[WARNING] Product: {prod_path} is out of the trimmming geographic limits. Skiping...')
        return None, False
    if not os.path.exists(prod_path_u):
        print(f'[WARNING] Product: {prod_path_u} does not exists. Problem upzipping file. Skiping...')
        return None, False
    return prod_path_u, delete


def check_geo_limits(prod_path, geo_limits, iszipped):
    if geo_limits is None:
        return 1
    cgeo = CHECK_GEO()
    if iszipped:
        if not cgeo.check_zip_file(prod_path):
            return -1
        cgeo.start_polygon_image_from_zip_manifest_file(prod_path)
        check_geo = cgeo.check_geo_area(geo_limits[0], geo_limits[1], geo_limits[2], geo_limits[3])
        return check_geo

    if not iszipped:
        cgeo.start_polygon_from_prod_manifest_file(prod_path)
        check_geo = cgeo.check_geo_area(geo_limits[0], geo_limits[1], geo_limits[2], geo_limits[3])
        return check_geo


def check_prod_site(path_prod, point_site):
    flag_location = 0
    if path_prod.endswith('SEN3') and os.path.isdir(path_prod):
        geoname = os.path.join(path_prod, 'xfdumanifest.xml')
        if os.path.exists(geoname):
            fgeo = open(geoname, 'r')
            for line in fgeo:
                line_str = line.strip()
                if line_str.startswith('<gml:posList>'):
                    flag_location = get_flag_location_from_line_geo(line_str, point_site)
            fgeo.close()
    if path_prod.endswith('.zip') and zp.is_zipfile(path_prod):
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
    return flag_location


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


def check(input_path, output_path):
    for file in os.listdir(input_path):
        name = file[:-5]
        idepixname = f'{name}_IDEPIX.nc'
        path_out = os.path.join(output_path, idepixname)
        if not os.path.exists(path_out):
            print(name)
    return True


def get_dates():
    start_date_p = args.start_date
    if args.end_date:
        end_date_p = args.end_date
    else:
        end_date_p = start_date_p
    start_date = get_date_from_param(start_date_p)
    end_date = get_date_from_param(end_date_p)
    if start_date is None:
        print(
            f'[ERROR] Start date {start_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return 'ERROR_DATE', 'ERROR_DATE'
    if end_date is None:
        print(
            f'[ERROR] End date {end_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return 'ERROR_DATE', 'ERROR_DATE'
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return 'ERROR_DATE', 'ERROR_DATE'
    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    return start_date, end_date


def get_date_from_param(dateparam):
    datefin = None
    try:
        ndays = int(dateparam)
        datefin = dt.now().replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(days=ndays)
    except:
        try:
            datefin = dt.strptime(dateparam, '%Y-%m-%d')
        except:
            pass
    return datefin


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
