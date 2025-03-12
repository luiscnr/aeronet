import argparse
import configparser
import os
from datetime import datetime as dt
from base.anet_nc_reader import AERONETReader
import pandas

parser = argparse.ArgumentParser(description="Date list from an AERONET_OC nc file")
parser.add_argument('-site', "--site_ref", help="AERONET-OC site")
parser.add_argument('-anc', "--anet_nc_file", help="AERONET-OC NC file, or AERONET-OC NC directory", required=True)
parser.add_argument('-sd', "--startdate", help="The Start Date - format YYYY-MM-DD ")
parser.add_argument('-ed', "--enddate", help="The End Date - format YYYY-MM-DD ")
parser.add_argument('-o', "--outputfile", help="Output directory", required=True)
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


def combine_venise_and_aaot():
    dir_base = '/mnt/c/DATA_LUIS/AERONET_OC/DATE_LISTS/MULTI'
    file_venise = os.path.join(dir_base, 'Venise_DateList.csv')
    file_aaot = os.path.join(dir_base, 'AAOT_DateList_All.csv')
    file_out = os.path.join(dir_base, 'AAOT_DateList.csv')
    venise_dates = []
    fr = open(file_venise, 'r')
    for line in fr:
        datehere = line.strip()
        venise_dates.append(datehere)
    fr.close()

    fr = open(file_aaot, 'r')
    fw = open(file_out, 'w')
    for line in fr:
        datehere = line.strip()
        if datehere not in venise_dates:
            fw.write(line)

    fr.close()
    fw.close()

    return True


def distribute_by_year():
    input_file = '/mnt/c/DATA_LUIS/AERONET_OC/DATE_LISTS/OLCI/Venise_DateList.csv'
    dates_by_year = {}
    fr = open(input_file, 'r')
    for line in fr:
        date_here = dt.strptime(line.strip(), '%Y-%m-%d')
        year = date_here.strftime('%Y')
        if year not in dates_by_year:
            dates_by_year[year] = [line]
        else:
            dates_by_year[year].append(line)
    fr.close()

    for year in dates_by_year:
        output_file = input_file[:-4] + '_' + year + '.csv'
        print(output_file)
        list_lines = dates_by_year[year]
        fw = open(output_file, 'w')
        for line in list_lines:
            fw.write(line)
        fw.close()

    return True


def main():
    ##TEST, COMBINE VENISE and AAOT
    # if combine_venise_and_aaot():
    #     return
    if distribute_by_year():
        return

    print('[INFO] Started')
    file_nc = None
    if os.path.isfile(args.anet_nc_file):
        file_nc = args.anet_nc_file
    elif os.path.isdir(args.anet_nc_file):
        if not args.site_ref:
            print(
                f'[ERROR] Argument --site_ref (-site) is required if -anc -anc argument ( or --anet_nc_file) is a directory')
            return
        ANET_SOURCE_DIR = args.anet_nc_file
        proc_date_prev = None
        for name in os.listdir(ANET_SOURCE_DIR):
            site = name[18:name.index('LWN') - 1]
            if site != args.site_ref:
                continue
            proc_date = dt.strptime(name.split('_')[1], '%Y%m%d')
            if proc_date_prev is None or proc_date > proc_date_prev:
                file_nc = os.path.join(ANET_SOURCE_DIR, name)
    else:
        print(f'[ERROR] Argument -anc (--anet_nc_file) should be a valid file or directory. ')
        return
    # ANET_SOURCE_DIR = '/store3/HYPERNETS/INSITU_AOC/NC/'
    # file_sites = '/store3/HYPERNETS/INSITU_AOC/site_list.ini'
    # if args.sites_file:
    #     file_sites = args.sites_file
    # if args.site:
    #     site = args.site
    # else:
    #     print(f'[ERROR] Site is not defined')
    # if not os.path.exists(file_sites):
    #     print(f'ERROR: Sites file: {file_sites} does not exist')
    #     return
    # options = configparser.ConfigParser()
    # options.read(file_sites)
    # insitu_lat = float(options[site]['Latitude'])
    # insitu_lon = float(options[site]['Longitude'])
    # if args.verbose:
    #     print(f'[INFO] SITE: {site} latitude:{insitu_lat}, longitude:{insitu_lon}')

    if file_nc is None:
        print(f'[ERROR] Aeronet NC file does not exist')
    if args.verbose:
        print(f'[INFO] Aeronet NC file: {file_nc}')

    if os.path.isdir(args.outputfile) and args.site_ref:
        outputfile = os.path.join(args.outputfile, f'{args.site_ref}_DateList.csv')
    else:
        outputfile = args.outputfile
    if not os.path.isdir(os.path.dirname(outputfile)):
        try:
            os.mkdir(os.path.dirname(outputfile))
        except:
            print(
                f'[ERROR] Outputfile {outputfile} can not be created. Please review if the folder exists and writting persissions.')
            return

    if args.verbose:
        print(f'[INFO] Output file: {outputfile}')

    start_date = dt(2000, 1, 1)
    end_date = dt.now()
    if args.startdate:
        start_date = dt.strptime(args.startdate, '%Y-%m-%d').date()
    if args.enddate:
        end_date = dt.strptime(args.enddate, '%Y-%m-%d').date()
    areader = AERONETReader(file_nc)
    print(f'[INFO] Obtaining date list...')
    date_list = areader.get_available_dates(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    print(f'[INFO] Writting file list...')
    with open(outputfile, 'w') as file:
        for date in date_list:
            file.writelines(date.strftime('%Y-%m-%d'))
            file.writelines('\n')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
