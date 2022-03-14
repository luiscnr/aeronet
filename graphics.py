import argparse
import os
from datetime import datetime
from datetime import timedelta
import numpy as np

parser = argparse.ArgumentParser(description="Aeronet Graphics")
parser.add_argument('-sites', "--sites",
                    help="Aeronet sites (comma-separated) (Default Gustav_Dalen_Tower, Helsinki_Lighthouse, Irbe_Lighthouse")
parser.add_argument('-anc', "--anet_nc_directory", help="Aeronet source directory")
# parser.add_argument('-fsit', "--sites_file", help="Sites File")
parser.add_argument('-s', "--sourcedir", help="Source directory with site/trim_files")
parser.add_argument('-type', "--type_graphic", help="Graphic type", choices=["DATE1"], required=True)
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()

from base.anet_nc_reader import AERONETReader
from s3_product_list import S3ProductList


def main():
    print('STARTED')
    sites = 'Gustav_Dalen_Tower, Helsinki_Lighthouse, Irbe_Lighthouse'
    if args.sites:
        sites = args.sites
    anet_nc_directory = '/store3/HYPERNETS/INSITU_AOC/NC/'
    if args.anet_nc_directory:
        anet_nc_directory = args.anet_nc_directory
    source_dir = None
    if args.sourcedir:
        source_dir = args.sourcedir
    if args.type_graphic == 'DATE1':
        if not os.path.exists(anet_nc_directory):
            print(f'[ERROR] Aeronet NC source directory: {anet_nc_directory} does not exist')
            exit(1)
        if source_dir is None:
            print(f'[ERROR] Source directory must be passed as a parameter')
            exit(1)
        if not os.path.exists(source_dir):
            print(f'[ERROR] Source directory: {source_dir} does not exist')
            exit(1)

        make_graphic_date1(sites, anet_nc_directory, source_dir)


def make_graphic_date1(sites, source_nc, source_site):
    if args.verbose:
        print('[INFO] Started date graphic...')

    site_list = sites.split(',')
    infod = {}
    value = 1
    valid_sites = True
    date_ini_anet = datetime.now()
    date_fin_anet = datetime(1970, 1, 1)
    date_ini_sat = datetime.now()
    date_fin_sat = datetime(1970, 1, 1)
    for site in site_list:
        s = site.strip()
        filenc = None
        for f in os.listdir(source_nc):
            if f.find(s) > 0:
                filenc = os.path.join(source_nc, f)
                break
        dirsite = os.path.join(source_site, s, 'trim')

        if os.path.exists(dirsite) and not filenc is None:
            areader = AERONETReader(filenc)
            dini, dfin = areader.get_time_ini_fin()
            print(filenc, dini, dfin)
            if dini < date_ini_anet:
                date_ini_anet = dini
            if dfin > date_fin_anet:
                date_fin_anet = dfin
            plist = S3ProductList(dirsite)
            if plist.time_ini < date_ini_sat:
                date_ini_sat = plist.time_ini
            if plist.time_fin > date_fin_sat:
                date_fin_sat = plist.time_fin
            infod[s] = {
                'FileNC': filenc,
                'DirSite': dirsite,
                'Value': value,
                'RadTime': areader.time_list,
                'SatTime': plist.time_list
            }

        else:
            valid_sites = False
        value = value + 1
    print(date_ini_anet, date_fin_anet)
    print(date_ini_sat, date_fin_sat)
    print(valid_sites)

    date_ini_graphics = date_ini_anet
    if date_ini_sat > date_ini_graphics:
        date_ini_graphics = date_ini_sat
    date_fin_graphics = date_fin_anet
    if date_fin_sat < date_fin_graphics:
        date_fin_graphics = date_fin_sat

    print(date_ini_graphics, date_fin_graphics)
    darray = np.arange(date_ini_graphics.replace(hour=0, minute=0, second=0).isoformat(),
                       date_fin_graphics.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
                       dtype='datetime64[D]')
    print(darray.shape, darray.dtype)
    nsites = len(site_list)
    ndates = len(darray)
    varray = np.empty((nsites * 2, ndates))
    date_ref = date_ini_graphics.replace(hour=0, minute=0, second=0, microsecond=0)
    iref = 0
    for s in infod:
        for t in infod[s]['RadTime']:
            itime = int((t.replace(hour=0, minute=0, second=0, microsecond=0) - date_ref).total_seconds() / 86400)
            print(t, itime)
            if itime >= 0:
                varray[iref][itime] = infod[s]['Value']

    date_tal = datetime(2016, 4, 29)
    dif = int((date_tal - date_ref).total_seconds() / 86400)


if __name__ == '__main__':
    main()
