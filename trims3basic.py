import datetime
import os
import zipfile as zp
from shapely.geometry.polygon import Polygon
import s3olcitrim_bal_frompy as trimtool
import argparse
from shapely.geometry import Point

parser = argparse.ArgumentParser(description="Search and trim S3 Level 1B products around a in-situ location")
parser.add_argument('-s', "--sourcedir", help="Source directory")
parser.add_argument('-p', "--sourceproduct", help="Source directory")
parser.add_argument('-o', "--outputdir", help="Output directory", required=True)
parser.add_argument('-geo', "--geo_limits", help="Geo limits", required=True)
parser.add_argument('-fdates', "--list_dates", help="Date list")
parser.add_argument('-wce', "--wce", help="Wild card expression")
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
        s = 50
        n = 65
        w = 5
        e = 35
    elif geo_coords == 'BAL_GDT':
        s = 58
        n = 59
        w = 17
        e = 18
        insitu_lat = 58.59417
        insitu_lon =  17.46683
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
    else:
        geo_list = geo_coords.split('_')
        s = float(geo_list[0].strip())
        n = float(geo_list[1].strip())
        w = float(geo_list[2].strip())
        e = float(geo_list[3].strip())
        insitu_lon = (w+e)/2
        insitu_lat = (s+n)/2

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
                date_here = datetime.datetime.strptime(line.strip(),'%Y-%m-%d')
                path_year = os.path.join(args.sourcedir,date_here.strftime('%Y'))
                path_jday = os.path.join(path_year,date_here.strftime('%j'))
                if os.path.exists(path_jday):
                    for name in os.listdir(path_jday):
                        path_prod = os.path.join(path_jday,name)
                        if path_prod.find(wce)>0:
                            flag_location = check_prod_site(path_prod,point_site)
                            if flag_location==1:
                                print(f'[INFO] Trimming product: {path_prod}')
                                trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir, args.verbose)
                            else:
                                print(f'[INFO] Product {path_prod} does not contain flag location')
                else:
                    print(f'[WARNING] Path for date: {line} -> {path_jday} was not found in source directory')


def check_prod_site(path_prod,point_site):
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
