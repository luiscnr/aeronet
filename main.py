import argparse
import datetime
import os
import tarfile

import numpy as np
import numpy.ma as ma
import pandas as pd
import requests
import subprocess

parser = argparse.ArgumentParser(
    description="Generation of NC Aeronet Files using CSV files downloaded from the Aeronet web site")
parser.add_argument("-d", "--download", help="Download data.", action="store_true")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument('-i', "--inputpath", help="Input (download) directory")
parser.add_argument('-o', "--outputpath", help="Output directory")
parser.add_argument('-s', "--sites", help="Aeronet site. BAL for Baltic Sites")
parser.add_argument('-t', "--thuillier", help="Thulier method", choices=['interp'])
args = parser.parse_args()

from base.anet_file import ANETFile


def only_test_two():
    file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/CHLA_DATA/Baltic_CHLA_Valid.csv'
    from insitu.csv_insitu_file import CSVInsituFile
    ipv = CSVInsituFile(file)
    return True


def only_test_three():
    from restoweb.resto import RESTO_WEB
    fout = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/ResTO_WispWeb/Test.nc'
    rweb = RESTO_WEB(True)
    rweb.retrive_data(datetime.datetime.strptime('2021-05-10', '%Y-%m-%d'),
                      datetime.datetime.strptime('2021-05-15', '%Y-%m-%d'), None)
    rweb.save_data_as_ncfile(fout)
    return True


def only_test_four_prev():
    from datetime import timedelta
    import pandas as pd
    from datetime import datetime as dt
    dir_base = '/mnt/c/DATA_LUIS/octac_work/bal_evolution/EXAMPLES/TRIMMED/MDBs/PLOT_OTHER'
    file_chla = os.path.join(dir_base, 'Baltic_CHLA_Valid_WithBrando2021.csv')
    df = pd.read_csv(file_chla, sep=';')
    date_array = np.array(df['DATE'])
    time_array = np.array(df['HOUR'])
    source = np.array(df['LONGITUDE'])
    times = []
    for idx in range(len(date_array)):
        datestr = f'{date_array[idx]}T{time_array[idx]}'
        datehere = dt.strptime(datestr, '%d/%m/%YT%H:%M:%S')
        times.append(datehere.strftime('%Y%m%d%H%M'))
        print(times[idx])

    # file_out = os.path.join(dir_base, 'MDB___CCI_INSITU_19970101_20221231_4P5hours.csv')
    file_out = os.path.join(dir_base, 'MDB_S3AB_OLCI_POLYMER_INSITU_20160401_20220531_valid.csv')
    fs = os.path.join(dir_base, 'longitudespolymer.csv')
    f1 = open(fs, 'w')
    f1.write('LONGITUDE')
    dfout = pd.read_csv(file_out, sep=';')
    dateout = dfout['Ins_Time']
    for dout in dateout:
        datehere = dt.strptime(dout, '%Y-%m-%d %H:%M')
        dateherestr = datehere.strftime('%Y%m%d%H%M')
        sourcew = '-1'
        if dateherestr in times:
            itime = times.index(dateherestr)
            sourcew = str(float(source[itime]))
        else:
            datehere = datehere + timedelta(hours=2)
            dateherestr = datehere.strftime('%Y%m%d%H%M')
            if dateherestr in times:
                itime = times.index(dateherestr)
                sourcew = str(float(source[itime]))
            else:
                datehere = datehere - timedelta(hours=1)
                dateherestr = datehere.strftime('%Y%m%d%H%M')
                if dateherestr in times:
                    itime = times.index(dateherestr)
                    sourcew = str(float(source[itime]))
        f1.write('\n')
        f1.write(sourcew)

    f1.close()

    return True


def only_test_six():
    import shutil
    # file_lat_lon = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/CHLA_COMMON_LATLON.csv'
    file_lat_lon = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/MDB_S3AB_OLCI_POLYMER_INSITU_20160401_20220531_valid.csv'
    dir_images = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/L1B'
    dir_copy = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/L1B_MATCH-UPS'
    import pandas as pd
    df = pd.read_csv(file_lat_lon, sep=';')
    for index, row in df.iterrows():
        granule_s3a = row['GRANULES_S3A']
        granule_s3b = row['GRANULES_S3B']
        # print(granule_s3a,granule_s3b)
        if granule_s3a != 'NODATA':
            file_s3a = os.path.join(dir_images, granule_s3a)
            # print(file_s3a,os.path.exists(file_s3a))
            if not os.path.exists(file_s3a):
                print('ATTENTIONS MISSING: ', file_s3a)
            else:
                print(file_s3a)
                fnew = os.path.join(dir_copy, granule_s3a)
                if not os.path.exists(fnew):
                    shutil.copytree(file_s3a, fnew)
        if granule_s3b != 'NODATA':
            file_s3b = os.path.join(dir_images, granule_s3b)
            # print(file_s3b,os.path.exists(file_s3b))
            if not os.path.exists(file_s3b):
                print('ATTENTIONS MISSING: ', file_s3b)
            else:
                print(file_s3b)
                fnew = os.path.join(dir_copy, granule_s3b)
                if not os.path.exists(fnew):
                    shutil.copytree(file_s3b, fnew)
    return True


def only_test_six_prev():
    # file_lat_lon = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/CHLA_COMMON_LATLON.csv'
    file_lat_lon = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/MDB_S3AB_OLCI_POLYMER_INSITU_20160401_20220531_valid.csv'
    dir_images = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/PUBLICATION/L1B'
    import pandas as pd
    from datetime import datetime as dt
    from netCDF4 import Dataset
    import numpy as np

    limits_granule = {}
    for name in os.listdir(dir_images):
        file_geo = os.path.join(dir_images, name, 'geo_coordinates.nc')
        dgeo = Dataset(file_geo)
        lat_array = np.array(dgeo.variables['latitude'])
        lon_array = np.array(dgeo.variables['longitude'])
        limits_granule[name] = {
            'minlat': lat_array.min(),
            'maxlat': lat_array.max(),
            'minlon': lon_array.min(),
            'maxlon': lon_array.max(),
        }
        # print(name,limits_granule[name])
        dgeo.close()

    df = pd.read_csv(file_lat_lon, sep=';')

    granules_s3a = []
    granules_s3b = []
    for index, row in df.iterrows():
        latp = row['LATITUDE']
        lonp = row['LONGITUDE']
        timestr = row['Ins_Time']
        time = dt.strptime(timestr, '%Y-%m-%d %H:%M')
        datestr = time.strftime('%Y%m%d')

        wce_s3a = f'S3A_OL_1_EFR____{datestr}'
        wce_s3b = f'S3B_OL_1_EFR____{datestr}'
        print('----------------------------------------------------')
        print(latp, lonp, timestr, wce_s3a, wce_s3b)
        granule_s3a = 'N/A'
        granule_s3b = 'N/A'
        for name in limits_granule:
            # if not name.startswith(wce_s3a) or not name.startswith(wce_s3b):
            #     continue

            if limits_granule[name]['minlat'] <= latp <= limits_granule[name]['maxlat']:
                if limits_granule[name]['minlon'] <= lonp <= limits_granule[name]['maxlon']:
                    if name.startswith(wce_s3a) and granule_s3a == 'N/A':
                        granule_s3a = name
                        print('**** ', name, granule_s3a)
                    if name.startswith(wce_s3b) and granule_s3b == 'N/A':
                        granule_s3b = name
                        print('**** ', name, granule_s3b)
        granules_s3a.append(granule_s3a)
        granules_s3b.append(granule_s3b)
    # print('*********************************')
    # for ga in granules_s3a:
    #     print(ga)
    print('**************************')
    for gb in granules_s3b:
        print(gb)

    return True


def only_test_five():
    file_with_latlot = '/mnt/c/DATA_LUIS/octac_work/bal_evolution/EXAMPLES/TRIMMED/MDBs/PLOT_OTHER/MDB___CCI_INSITU_19970101_20221231_4P5hours.csv'
    dir_base = '/mnt/c/data_luis/octac_work/bal_evolution/examples/chla/publication'
    name_in = 'MDB_S3AB_OLCI_POLYMER_INSITU_20160401_20220531_valid.csv'
    name_out = 'MDB_S3AB_OLCI_POLYMER_INSITU_20160401_20220531_valid_LATLON.csv'
    file_in = os.path.join(dir_base, name_in)
    file_out = os.path.join(dir_base, name_out)
    import pandas as pd
    datall = pd.read_csv(file_with_latlot, sep=';')
    dataf = pd.read_csv(file_in, sep=';')
    dataout = dataf.copy()
    latout = []
    lonout = []
    for index, row in dataf.iterrows():
        index_mu = row['Index_MU']
        # if index_mu>220:
        #     continue
        row_ll = datall.loc[datall['Index_MU'] == index_mu]
        lat_here = row_ll['LATITUDE'].tolist()
        lon_here = row_ll['LONGITUDE'].tolist()
        latout.append(lat_here[0])
        lonout.append(lon_here[0])
    dataout['LATITUDE'] = latout
    dataout['LONGITUDE'] = lonout

    dataout.to_csv(file_out, sep=';')

    return True


def trajectory_figure():
    print('TRAJECTORY FIGURE')
    import matplotlib.pyplot as plt
    from netCDF4 import Dataset
    import cartopy.crs as ccrs
    import cartopy
    # extract_file = '/mnt/c/DATA_LUIS/TARA_TEST/extracts/S3A_OL_2_WFR____20230405T105805_20230405T110105_20230406T233602_0179_097_208_2160_MAR_O_NT_003_SEN3_extract_2_0.nc'
    # dataset = Dataset(extract_file)
    # lat = np.array(dataset.variables['satellite_latitude'])
    # lon = np.array(dataset.variables['satellite_longitude'])
    #
    # dataset.close()

    # file_img = '/mnt/c/DATA_LUIS/TARA_TEST/extracts/pixels.png'
    # rgb = np.ma.zeros((25, 25, 3))
    #
    # rgb[:, :, 0] = 255
    # rgb[:, :, 1] = 255
    # rgb[:, :, 2] = 255
    #
    # extent = [0, 25, 0, 25]
    # plt.imshow(rgb, interpolation=None, extent=extent)
    # for y in range(25):
    #     plt.hlines(y,0,25,colors=['black'])
    # for x in range(25):
    #     plt.vlines(x,0,25,colors=['black'])
    # plt.hlines(12, 12, 13, colors=['r'])
    # plt.hlines(13, 12, 13, colors=['r'])
    # plt.vlines(12, 12, 13, colors=['r'])
    # plt.vlines(13, 12, 13, colors=['r'])
    # plt.savefig(file_img)
    # plt.close()

    file_points = '/mnt/c/DATA_LUIS/TARA_TEST/TaraEuropa_StationGPS_Map_October_11.csv'
    fout = '/mnt/c/DATA_LUIS/TARA_TEST/trajectory.tif'
    import pandas as pd
    df = pd.read_csv(file_points, sep=';')

    array = df.to_numpy()
    lat_points = array[:, 2]
    lon_points = array[:, 3]

    ax = plt.axes(projection=ccrs.Miller())
    ax.set_extent([-10, 30, 35, 65], crs=ccrs.PlateCarree())

    hline = ax.plot(lon_points, lat_points, color='black', linewidth=1, marker='.', transform=ccrs.Geodetic())
    plt.savefig(fout, dpi=300, bbox_inches='tight', pil_kwargs={"compression": "tiff_lzw"})
    # lat_points = [43.9329, 43.9169]

    return True


def baltic_figure1():
    print('BALTIC FIGURE')
    dir_base = '/mnt/c/DATA_LUIS/octac_work/bal_evolution/EXAMPLES/TRIMMED/MDBs/PLOT_OTHER'
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy

    ax = plt.axes(projection=ccrs.Miller())
    ax.set_extent([10, 31, 53, 67], crs=ccrs.PlateCarree())
    # ax.coastlines()

    import cartopy.feature as cfeature
    land_50m = cfeature.NaturalEarthFeature('physical', 'land', '10m',
                                            edgecolor='black',
                                            facecolor=cfeature.COLORS['land'])

    ax.add_feature(land_50m)

    # ax.add_feature(cartopy.feature.LAND, zorder=0, edgecolor='black', scale='50m')

    file_transects = os.path.join(dir_base, 'RFLEX_2016_Stefan_NIR_corrected.csv')
    import pandas as pd
    from datetime import datetime as dt
    df = pd.read_csv(file_transects, sep=',')
    times = np.array(df['time'])
    latitude = np.array(df['lat'])
    longitude = np.array(df['lon'])
    dates = []
    alldates = []
    latdates = []
    londates = []
    for idx in range(len(times)):
        time = times[idx]
        time_here = dt.strptime(time, '%Y-%m-%d %H:%M:%S')
        date_here = time_here.replace(hour=0, minute=0, second=0, microsecond=0)
        if time_here.year == 2016 and time_here.month >= 5:
            alldates.append(time_here.timestamp())
            dates.append(date_here.timestamp())
            latdates.append(latitude[idx])
            londates.append(longitude[idx])
        # print(time_here)

    alldates = np.array(alldates)
    dates = np.array(dates)
    latdates = np.array(latdates)
    londates = np.array(londates)
    dates_unique = np.unique(dates)

    for date in dates_unique:
        print(date)
        dateshere = alldates[dates == date]
        lat_date = latdates[dates == date]
        lon_date = londates[dates == date]

        lat_date_plot = []
        lon_date_plot = []

        for idx in range(1, len(lat_date)):
            time_dif = dateshere[idx] - dateshere[idx - 1]
            if time_dif > 0:
                lat_date_plot.append(lat_date[idx])
                lon_date_plot.append(lon_date[idx])
            else:
                if len(lat_date) > 1:
                    ax.plot(lon_date_plot, lat_date_plot, color='gray', linewidth=1, marker=None,
                            transform=ccrs.Geodetic())
                lat_date_plot = []
                lon_date_plot = []

        if len(lat_date) > 1:
            hline = ax.plot(lon_date_plot, lat_date_plot, color='gray', linewidth=1, marker=None,
                            transform=ccrs.Geodetic())

    file_chla = os.path.join(dir_base, 'MDB___CCI_INSITU_19970101_20221231_4P5hours.csv')
    df = pd.read_csv(file_chla, sep=';')
    lat_array = np.array(df['LATITUDE'])
    lon_array = np.array(df['LONGITUDE'])
    year = np.array(df['YEAR'])
    source = np.array(df['SOURCENUM'])
    sources = [1, 2]
    symbols = ['x', '+']
    periods = ['MULTI', 'OLCI']
    handles = []
    for period in periods:
        if period == 'MULTI':
            lat_p = lat_array[year < 2016]
            lon_p = lon_array[year < 2016]
            source_p = source[year < 2016]
            color_p = 'r'
        if period == 'OLCI':
            lat_p = lat_array[year >= 2016]
            lon_p = lon_array[year >= 2016]
            source_p = source[year >= 2016]
            color_p = 'b'
        for idx in range(2):
            s = sources[idx]
            symbol = symbols[idx]
            lat_plot = lat_p[source_p == s]
            lon_plot = lon_p[source_p == s]
            print(period, s, len(lat_plot))
            hp = ax.plot(lon_plot, lat_plot, color=color_p, linewidth=0, marker=symbol, markersize=4,
                         transform=ccrs.Geodetic())
            handles.append(hp[0])

    # stations
    h = ax.plot([17.467], [58.594], color='green', marker='o', linewidth=0, markersize=5,
                transform=ccrs.Geodetic())  # GDL
    ax.plot([24.926], [59.949], color='green', marker='o', markersize=5, transform=ccrs.Geodetic())  # HLH
    ax.plot([21.723], [57.751], color='green', marker='o', markersize=5, transform=ccrs.Geodetic())  # ILH

    handles.append(h[0])
    handles.append((hline[0]))

    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=0.5, color='black', alpha=0.6, linestyle=':')

    import matplotlib.ticker as mticker
    gl.xlocator = mticker.FixedLocator([15, 12, 18, 21, 24, 27, 30])
    gl.ylocator = mticker.FixedLocator([54, 57, 60, 63, 66])
    gl.xlabel_style = {'size': 10}
    gl.ylabel_style = {'size': 10}

    fout = os.path.join(dir_base, 'Figure1.tif')

    print(len(handles))
    print(handles)
    str_legend = ['Alg@line 1997-2015', 'COMBINE 1997-2015', 'Alg@line 2016-2019', 'COMBINE 2016-2019',
                  'AERONET-OC sites', 'Alg@line radiometry']
    plt.legend(handles, str_legend, fontsize=8, loc='upper left', markerscale=1.0, bbox_to_anchor=(-0.01, 1.01),
               framealpha=1.0)

    plt.text(16.0, 58.7, 'GDT', transform=ccrs.Geodetic(), fontsize=8)
    plt.text(24.5, 60.25, 'HL', transform=ccrs.Geodetic(), fontsize=8)
    plt.text(21.7, 57.1, 'IL', transform=ccrs.Geodetic(), fontsize=8)

    print(fout)
    plt.savefig(fout, dpi=300, bbox_inches='tight', pil_kwargs={"compression": "tiff_lzw"})

    return True


def only_test_seven():
    file_nc = '/mnt/c/DATA_LUIS/AERONET_OC/AERONET_NC/20020101_20231111_AAOT.LWN_lev20_15.nc'
    import base.anet_nc_reader
    reader = base.anet_nc_reader.AERONETReader(file_nc)
    reader.extract_time_list()
    for t in reader.time_list:
        print(t)

    return True

def only_test_eight():
    from base.anet_nc_reader import AERONETReader

    #site = 'Galata_Platform'
    #site = 'Gloria'
    site = 'Section-7_Platform'

    #system = 'Black Sea'
    system = 'Danube Delta'

    #file_nc = '/mnt/c/DATA_LUIS/AERONET_OC/AERONET_NC/20020101_20231111_Galata_Platform.LWN_lev20_15.nc'
    #file_nc = '/mnt/c/DATA_LUIS/AERONET_OC/AERONET_NC/20020101_20231111_Gloria.LWN_lev20_15.nc'
    file_nc = '/mnt/c/DATA_LUIS/AERONET_OC/AERONET_NC/20020101_20231111_Section-7_Platform.LWN_lev20_15.nc'

    areader = AERONETReader(file_nc)
    date_list = areader.get_available_dates('2016-01-01', None)

    #file_out = '/mnt/c/DATA_LUIS/DOORS_WORK/DOOR_insitu_BlackSea_AeronetOC_Galata_Platform.csv'
    #file_out = '/mnt/c/DATA_LUIS/DOORS_WORK/DOOR_insitu_BlackSea_AeronetOC_Gloria.csv'
    file_out = '/mnt/c/DATA_LUIS/DOORS_WORK/DOOR_insitu_BlackSea_AeronetOC_Section-7_Platform.csv'

    first_line = 'System;Station;Date;Time;Lat;Long;Source'

    prename = f'{system};{site}'

    #post_name = '12:00;43.044624;28.193190;AERONET-OC' ##Galata_Platform
    #post_name = '12:00;44.599970;29.359670;AERONET-OC' ##Gloria
    post_name = '12:00;44.5458;29.4466;AERONET-OC' ##Section-7
    f1 = open(file_out,'w')
    f1.write(first_line)
    for d in date_list:
        print(d)
        line = f'{prename};{d};{post_name}'
        f1.write('\n')
        f1.write(line)
    f1.close()
    return True

def main():
    # b = baltic_figure1()
    # b = trajectory_figure()
    # b = only_test_eight()
    # if b:
    #     return
    # to run script loca:
    # python3 main.py -i /mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/AERONET_INPUT -o /mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/AERONET_NC
    print('STARTED...')  # Press Ctrl+F8 to toggle the breakpoint
    dir_base = args.inputpath
    dir_output = args.outputpath

    if dir_base is None or dir_output is None:
        print('[ERROR] Input and output path are required...')
        return

    # #dir_base = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/prueba'
    # # dir_output = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES'

    make_download = False
    if args.download:
        make_download = True

    if make_download:
        if args.verbose:
            print('Downloading level 2')
        url = 'https://aeronet.gsfc.nasa.gov/data_push/V3/LWN/LWN_Level20_All_Points_V3.tar.gz'
        r = requests.get(url, allow_redirects=True)
        file_out = os.path.join(dir_base, 'LWN_Level20_All_Points_V3.tar.gz')
        open(file_out, 'wb').write(r.content)
        if args.verbose:
            print('Uncompressing level 2')
        file_tar = tarfile.open(file_out)
        file_tar.extractall(dir_base)

        if args.verbose:
            print('Downloading level 1.5')
        url = 'https://aeronet.gsfc.nasa.gov/data_push/V3/LWN/LWN_Level15_All_Points_V3.tar.gz'
        r = requests.get(url, allow_redirects=True)
        file_out = os.path.join(dir_base, 'LWN_Level15_All_Points_V3.tar.gz')
        open(file_out, 'wb').write(r.content)
        if args.verbose:
            print('Uncompressing level 1.5')
        file_tar = tarfile.open(file_out)
        file_tar.extractall(dir_base)

    sites = None
    if args.sites:
        if args.sites == 'BAL':
            sites = ['Gustav_Dalen_Tower', 'Irbe_Lighthouse', 'Helsinki_Lighthouse']
        elif args.sites == 'BLK':
            sites = ['Galata_Platform', 'Gloria', 'Section-7_Platform']
        else:
            sites = [args.sites]

    dir_level15 = os.path.join(dir_base, 'LWN', 'LWN15', 'ALL_POINTS')
    dir_level20 = os.path.join(dir_base, 'LWN', 'LWN20', 'ALL_POINTS')
    files_level20 = os.listdir(dir_level20)
    for f in files_level20:
        f20 = os.path.join(dir_level20, f)
        f15 = os.path.join(dir_level15, f.replace('lev20', 'lev15'))
        site = f[f.find('_') + 1:f.find('.')]
        site = site[site.find('_') + 1:len(site)]

        do_site = True
        if sites is not None:
            if site not in sites:
                do_site = False

        # if site == 'Gloria':
        if do_site:
            if args.verbose:
                print('[INFO] DOING SITE:', site, '-----------------------------------------')
            afilel20 = ANETFile(f20, None, False)
            afilel15 = ANETFile(f15, None, False)
            if args.verbose:
                print(f'[INFO] File 2.0: {f20}')
                print(f'[INFO] File 1.5: {f15}')
            dfcombined = afilel20.check_and_append_df(afilel15)
            aeronet_combined = ANETFile(None, dfcombined, True)
            file_out = os.path.join(dir_output, f.replace('lev20', 'lev20_15') + '.nc')
            aeronet_combined.create_aeorent_ncfile(file_out)


def only_test():
    print(['TEST'])
    path_folder = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/PANTHYR'
    path_out = os.path.join(path_folder, 'VALID_SPECTRA')
    # path_file = 'AAOT_20190923_025406_20210416_110051_AZI_225_data.csv'
    # from panthyr.panthyr_file import Panthyr_File
    from panthyr.panthyr_file_list import Panthyr_List

    # pfile = Panthyr_File(path_file)
    # pfile.add_th_validation_criteria(None,500,0,'lt')
    # print(pfile.check_validation_criteria())

    # plist.add_time_validation_criteria(None,0,300)

    # plist.create_list_from_folder(path_folder,[135, 225])
    # for l in plist.list_pfiles:
    #     if plist.list_pfiles[l]['valid']:
    #         print(plist.list_pfiles[l]['time'].strftime('%H:%M'))
    # plist.get_dfvalid_spectra('')

    # plist.create_list_from_folder_dates(path_folder, datetime.datetime(2021, 4, 1), datetime.datetime(2021, 12, 21),
    #                                     [225])
    # last_day = [0,31,28,31,30,31,30,31,31,30,31,30,31]
    # for month in range(4,5):
    #     print('Month',month)
    #     plist = Panthyr_List()
    #     plist.add_th_validation_criteria(None, 500, 0, 'lt')
    #     path_outliers = os.path.join(path_folder, 'Outliers_225.csv')
    #     plist.add_outliers_df(None, path_outliers)
    #     start_date = datetime.datetime(2021, month, 1)
    #     end_date = datetime.datetime(2021,month,last_day[month])
    #     plist.create_list_from_folder_dates(path_folder,start_date, end_date,[225])
    #     print('Nvalidos: ', plist.nfilesvalid)
    #     df = plist.get_dfvalid_spectra(None)
    #     start_date_str = start_date.strftime('%Y%m%d')
    #     end_date_str = end_date.strftime('%Y%m%d')
    #     file_out = os.path.join(path_out,f'ValidSpectra_225_{start_date_str}_{end_date_str}.csv')
    #     df.to_csv(file_out,sep=';')

    # CONCATENAMOS SPECTRA DE PANTHYR
    # import numpy as np
    # wce = '_270'
    # dffin = None
    # for fname in os.listdir(path_out):
    #     if fname.find(wce)>=0:
    #         print(fname)
    #         dfhere = pd.read_csv(os.path.join(path_out,fname),sep=';')
    #         nrows = len(dfhere.index)
    #         valid = [False]*nrows
    #         for index,row in dfhere.iterrows():
    #             try:
    #                 date_here = datetime.datetime.strptime(row['TIME'], '%Y-%m-%d %H:%M:%S')
    #             except:
    #                 print('ERRROR: ', row['TIME'])
    #             date_here_h = date_here.replace(minute=0)
    #             dif = (date_here-date_here_h).total_seconds()/60
    #             #print(date_here,date_here_h,dif)
    #             if dif<5:
    #                 valid[index] = True
    #
    #         dfhere_limited = dfhere[np.array(valid)==True][:]
    #         if dffin is None:
    #             dffin = dfhere_limited
    #         else:
    #             dffin = pd.concat([dffin,dfhere_limited],ignore_index=True)
    # dffin.to_csv(os.path.join(path_out,f'Valid_spectra{wce}.csv'),sep=';')

    # ASOCIAMOS PANTHYR WITH HYPERNETS
    import numpy as np
    file_panthyr = os.path.join(path_out, 'Valid_spectra_270.csv')
    file_out = os.path.join(path_out, 'Validation_Hypstar_Panthir_270.csv')
    dfpanthyr = pd.read_csv(file_panthyr, sep=';')
    wlvalues = list(dfpanthyr.columns[5:])
    wllist = []
    iwl_start = -1
    iwl_end = -1
    for iwl in range(0, len(wlvalues)):
        if float(wlvalues[iwl]) >= 400 and iwl_start == -1:
            iwl_start = iwl
        if iwl_start >= 0:
            wllist.append(float(wlvalues[iwl]))
        if iwl_end == -1:
            if float(wlvalues[iwl]) == 800:
                iwl_end = iwl
                break
            elif float(wlvalues[iwl]) > 800:
                iwl_end = iwl
                break

    iwl_start = iwl_start + 5
    iwl_end = iwl_end + 6
    wllist = np.array(wllist)
    df_fin = None
    idx = 0
    for i in range(len(dfpanthyr.index)):
        date_here = datetime.datetime.strptime(dfpanthyr.iloc[i].at['TIME'], '%Y-%m-%d %H:%M:%S')
        print(i, date_here)
        rrs_panthyr_values = np.array(dfpanthyr.iloc[i, iwl_start:iwl_end])
        rrs_hysptar_values, hypstar_time = get_nearest_hypernets_spectra(date_here, wllist)
        if rrs_hysptar_values is not None:
            df_here = pd.DataFrame(
                columns=['ID', 'PanthyrTime', 'HypstarTime', 'Wavelength', 'PanthyrRRS', 'HypstarRRS'],
                index=range(len(wllist)))
            indices = np.linspace(idx, idx + len(wllist), num=len(wllist), endpoint=False)
            # print(indices)
            idx = idx + len(wllist)
            df_here.loc[:, 'ID'] = indices
            df_here.loc[:, 'PanthyrTime'] = date_here.strftime('%Y-%m-%d %H:%M')
            df_here.loc[:, 'HypstarTime'] = hypstar_time.strftime('%Y-%m-%d %H:%M')
            df_here.loc[:, 'Wavelength'] = wllist
            df_here.loc[:, 'PanthyrRRS'] = rrs_panthyr_values
            df_here.loc[:, 'HypstarRRS'] = rrs_hysptar_values
            # print(rrs_hysptar_values, len(rrs_hysptar_values))
            if df_fin is None:
                df_fin = df_here
            else:
                df_fin = pd.concat([df_fin, df_here])
    df_fin.to_csv(file_out, sep=';')
    return True


def get_nearest_hypernets_spectra(date_here, wllist):
    from netCDF4 import Dataset
    path_hypernets = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/HYPSTAR'

    path_day = os.path.join(path_hypernets, date_here.strftime('%Y'), date_here.strftime('%m'),
                            date_here.strftime('%d'))
    if not os.path.exists(path_day):
        return None, None
    wce = 'L2A'
    hyp_rrs_fin = None
    hyp_time_fin = None
    for f in os.listdir(path_day):

        if f.find(wce) >= 0 and f.endswith('.nc'):
            hyp_rrs_fin = np.zeros(len(wllist))
            date_hypstar_str = f.split('_')[5]
            date_hypstar = datetime.datetime.strptime(date_hypstar_str, '%Y%m%dT%H%M')
            time_dif_min = abs((date_hypstar - date_here).total_seconds()) / 60
            if time_dif_min < 5:
                file_hypstar = os.path.join(path_day, f)
                nchyp = Dataset(file_hypstar)
                hyp_wl = np.array(nchyp.variables['wavelength'])
                hyp_rhow_vec = [x for x, in nchyp.variables['reflectance'][:]]
                hyp_rrs = ma.array(hyp_rhow_vec) / np.pi
                hyp_valid = True
                for ival in range(len(hyp_rrs)):
                    val = hyp_rrs[ival]
                    wlhere = hyp_wl[ival]
                    if ma.is_masked(val):
                        hyp_valid = False
                        break
                    if val < 0 and wlhere < 500:
                        hyp_valid = False
                        break
                    if val > 0.01:
                        hyp_valid = False
                        break
                if hyp_valid:
                    print('VALIDO')
                    for idx in range(len(wllist)):
                        wlref = wllist[idx]
                        inear = np.argmin(np.abs(wlref - hyp_wl))
                        hyp_rrs_fin[idx] = hyp_rrs[inear]
                    hyp_time_fin = date_hypstar
                    return hyp_rrs_fin, hyp_time_fin
                else:
                    hyp_rrs_fin = None

            else:
                hyp_rrs_fin = None

    return hyp_rrs_fin, hyp_time_fin

    # fout = os.path.join(path_folder,'Outliers_135.csv')
    # plist.save_outliers_asfile(1.5,fout)

    # folder = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/MDBs/MDB_S3A_OLCI_WFR_STANDARD_L2_HYPERNETS_VEIT_all'
    # for f in os.listdir(folder):
    #     print(f)

    # from skie.skie_csv import SKIE_CSV
    # path_skie = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/SKIE/Rflex_datadump_2019_2021_filterQin.csv'
    # skie_file = SKIE_CSV(path_skie)
    # skie_file.extract_wl_colnames()
    # print(skie_file.col_wl)

    # path_home = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/HYPSTAR/'7
    #
    # cmd = 'scp -P 9022 hypstar@enhydra.naturalsciences.be:/waterhypernet/HYPSTAR/Processed/VEIT/'
    #
    # for m  in range(1,4):
    #     for d in range(1,32):
    #         if m==3 and d>20:
    #             continue
    #         try:
    #             fecha = datetime.datetime(2022,m,d)
    #         except:
    #             continue
    #         yearstr = fecha.strftime(('%Y'))
    #         messtr = fecha.strftime('%m')
    #         diastr = fecha.strftime('%d')
    #         path_year = os.path.join(path_home,yearstr)
    #         if not os.path.exists(path_year):
    #             os.mkdir(path_year)
    #         path_month = os.path.join(path_year, messtr)
    #         if not os.path.exists(path_month):
    #             os.mkdir(path_month)
    #         path_day = os.path.join(path_month, diastr)
    #         if not os.path.exists(path_day):
    #             os.mkdir(path_day)
    #
    #         cmdfin = f'{cmd}{yearstr}/{messtr}/{diastr}/*L2* {path_day}'
    #
    #         print(cmdfin)
    #         prog = subprocess.Popen(cmdfin, shell=True, stderr=subprocess.PIPE)
    #         out, err = prog.communicate()
    #         if err:
    #             print(err)

    # wl = np.array([450.25, 480.5, 510.66, 620.145])
    # wlout = foc.get_F0_array(wl,'interp')
    # print(wlout,type(wlout))

    # aeronet_path = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS'
    # aeronet_file = os.path.join(aeronet_path, '02020101_20211120_Venise.LWN_lev20')

    # aeronet_filel2 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS/20020101_20211120_Venise.LWN_lev20'
    # aeronet_filel15 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN15/ALL_POINTS/20020101_20211127_Venise.LWN_lev15'
    # afilel2 = ANETFile(aeronet_filel2,None,False)
    # afilel15 = ANETFile(aeronet_filel15,None,False)
    # dfcombined = afilel2.check_and_append_df(afilel15)
    # aeronet_combined = ANETFile(None,dfcombined,True)
    # file_out = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # aeronet_combined.create_aeorent_ncfile(file_out)

    # file_nc = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # areader = AERONETReader(file_nc)
    # areader.prepare_data_fordate('2020-08-09')
    # #areader.extract_spectral_data('Lwn_f_Q')
    # areader.extract_rrs()
    # areader.out_spectral_data()
    # #areader.plot_spectra(None)

    # from base.anet_nc_reader import AERONETReader
    # file_nc = '/mnt/d/LUIS/OCTAC_WORK/BALTIC/20020101_20220129_Gustav_Dalen_Tower.LWN_lev20_15.nc'
    # areader = AERONETReader(file_nc)
    # date_list = areader.get_available_dates('01-04-2016', None)
    # for d in date_list:
    #     print(d)

    # wl = np.array([450.25, 480.5, 510.66, 620.145])
    # wlout = foc.get_F0_array(wl,'interp')
    # print(wlout,type(wlout))

    # aeronet_path = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS'
    # aeronet_file = os.path.join(aeronet_path, '02020101_20211120_Venise.LWN_lev20')

    # aeronet_filel2 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN20/ALL_POINTS/20020101_20211120_Venise.LWN_lev20'
    # aeronet_filel15 = '/home/lois/PycharmProjects/aeronets/DATA/INPUT_FILES/LWN/LWN15/ALL_POINTS/20020101_20211127_Venise.LWN_lev15'
    # afilel2 = ANETFile(aeronet_filel2,None,False)
    # afilel15 = ANETFile(aeronet_filel15,None,False)
    # dfcombined = afilel2.check_and_append_df(afilel15)
    # aeronet_combined = ANETFile(None,dfcombined,True)
    # file_out = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # aeronet_combined.create_aeorent_ncfile(file_out)

    # file_nc = '/home/lois/PycharmProjects/aeronets/DATA/OUTPUT_FILES/VEIT_Aeronet_Out.nc'
    # areader = AERONETReader(file_nc)
    # areader.prepare_data_fordate('2020-08-09')
    # #areader.extract_spectral_data('Lwn_f_Q')
    # areader.extract_rrs()
    # areader.out_spectral_data()
    # #areader.plot_spectra(None)

    # from base.anet_nc_reader import AERONETReader
    # file_nc = '/mnt/d/LUIS/OCTAC_WORK/BALTIC/20020101_20220129_Gustav_Dalen_Tower.LWN_lev20_15.nc'
    # areader = AERONETReader(file_nc)
    # date_list = areader.get_available_dates('01-04-2016', None)
    # for d in date_list:
    #     print(d)
    return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
