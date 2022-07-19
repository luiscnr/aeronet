import argparse
import configparser
import os
from datetime import datetime as dt
from base.anet_nc_reader import AERONETReader
import pandas

parser = argparse.ArgumentParser(description="Search and trim S3 Level 1B products around a in-situ location")
parser.add_argument('-site', "--site", help="Aeronet site")
parser.add_argument('-anc', "--anet_nc_file", help="Aeronet NC File")
parser.add_argument('-fsit', "--sites_file", help="Sites File")
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD ")
parser.add_argument('-o', "--outputfile", help="Output directory", required=True)
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


def main():
    print('[INFO] Started')
    ANET_SOURCE_DIR = '/store3/HYPERNETS/INSITU_AOC/NC/'
    file_sites = '/store3/HYPERNETS/INSITU_AOC/site_list.ini'
    if args.sites_file:
        file_sites = args.sites_file
    if args.site:
        site = args.site
    else:
        print(f'[ERROR] Site is not defined')
    if not os.path.exists(file_sites):
        print(f'ERROR: Sites file: {file_sites} does not exist')
        return
    options = configparser.ConfigParser()
    options.read(file_sites)
    insitu_lat = float(options[site]['Latitude'])
    insitu_lon = float(options[site]['Longitude'])
    if args.verbose:
        print(f'[INFO] SITE: {site} latitude:{insitu_lat}, longitude:{insitu_lon}')

    file_nc = None
    if args.anet_nc_file:
        file_nc = args.anet_nc_file
    else:
        if not site is None and os.path.exists(ANET_SOURCE_DIR):
            site_search = site
            if site == 'VEIT':
                site_search = 'Venise'
            for f in os.listdir(ANET_SOURCE_DIR):
                if f.find(site_search) > 0:
                    file_nc = os.path.join(ANET_SOURCE_DIR, f)
                    break
    if file_nc is None:
        print(f'[ERROR] Aeronet NC file does not exist')
    if args.verbose:
        print(f'[INFO] Aeronet NC file: {file_nc}')

    if args.outputfile:
        outputfile = args.outputfile
    else:
        print('ERROR: Output file is required')
        return

    if args.verbose:
        print(f'[INFO] Output file: {outputfile}')

    start_date = dt(2000,1,1)
    end_date = dt.now()
    if args.startdate:
        start_date = dt.strptime(args.startdate, '%Y-%m-%d').date()
    if args.startdate:
        end_date = dt.strptime(args.enddate, '%Y-%m-%d').date()
    areader = AERONETReader(file_nc)
    print(f'[INFO] Obtaining date list...')
    date_list = areader.get_available_dates(start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'))
    print(f'[INFO] Writting file list...')
    with open(outputfile, 'w') as file:
        for date in date_list:
            file.writelines(date.strftime('%Y-%m-%d'))
            file.writelines('\n')




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
