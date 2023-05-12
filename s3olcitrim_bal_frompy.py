#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Created on Thu Jun 20 16:06:38 2019

@author: mario
mario.benincasa@artov.ismar.cnr.it
mbeninca@gmail.com
"""

"""
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

"""
"""

"""
"""

release, release_date_string = '0.2', '20191124'
# 20191124 - release_date_string

import datetime
import hashlib
import os
import shutil
import sys
import tarfile
import zipfile

import numpy as np
from lxml import etree
from netCDF4 import Dataset

# unit of time
onesecond = datetime.timedelta(seconds=1)

# default configuration file in same directory where we reside
sdir = os.path.dirname(os.path.realpath(__file__))  # where we reside!


# configfile=os.path.join(sdir,'s3olci.yaml')

class Point:
    def __init__(self, abscissa, ordinate):
        self.x = abscissa
        self.y = ordinate


def round_to_second(its: datetime.datetime) -> datetime.datetime:
    "Round the input timestamp (type datetime.datetime) to the nearest second"
    rts = datetime.datetime(its.year, its.month, its.day, its.hour, its.minute, its.second)
    if its.microsecond >= 500000:
        rts += onesecond
    return rts


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def make_trim(south, north, west, east, arg_iprod, arg_tardir, arg_delete, arg_outdir, arg_verbose):
    release_date = datetime.datetime.strptime(release_date_string, '%Y%m%d')

    # # Command line parsing
    # parser = argparse.ArgumentParser(description='Trim OLCI RR and FR files (sen3 format) to box, \
    #                                rel. %s of %s.' % (release, release_date.strftime('%d %B %Y')), \
    #                                  add_help=False)
    # required = parser.add_argument_group('required arguments')
    # required.add_argument("-n", "--north", action="store", \
    #                       help="Northern limit area to trim the product to",
    #                       required=True)
    # required.add_argument("-s", "--south", action="store", \
    #                       help="Southern limit area to trim the product to",
    #                       required=True)
    # required.add_argument("-w", "--west", action="store", \
    #                       help="Westhern limit area to trim the product to",
    #                       required=True)
    # required.add_argument("-e", "--east", action="store", \
    #                       help="Easthern limit area to trim the product to",
    #                       required=True)
    # required.add_argument('iprod', action='store', \
    #                       help='input OLCI product (directory or tarfile or zipfile)')
    # optional = parser.add_argument_group('optional arguments')
    # optional.add_argument("-t", "--tardir", action="store", \
    #                       help="directory where to un-tar or unzip PRODUCT.tar/.zip, default is current dir")
    # optional.add_argument("-del", "--delete", action="store_true", \
    #                       help="delete un-tar-ed or unzipped files, default keeps them")
    # optional.add_argument("-od", "--outdir", action="store", \
    #                       help="directory where to save the trimmed product, default is cwd")
    # optional.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    # optional.add_argument("-h", "--help", action="help", \
    #                       help="show this help message and exit")
    # args = parser.parse_args()

    w_bound_deg, s_bound_deg, e_bound_deg, n_bound_deg = [np.float_(west), np.float_(south), np.float_(east),
                                                          np.float_(north)]

    swcrn = Point(w_bound_deg, s_bound_deg)  # southwest corner
    necrn = Point(e_bound_deg, n_bound_deg)  # northeast corner

    is_zip_or_tar = False
    if not arg_tardir is None and os.path.isdir(arg_tardir):
        tardir = arg_tardir
    else:
        tardir = os.getcwd()

    iprod = os.path.abspath(arg_iprod)
    prodname = os.path.basename(iprod)
    if prodname.endswith('.tar') or prodname.endswith('.zip'):
        prodname = prodname[:-4]
    # zip files from CODA
    if not prodname.endswith('.SEN3'):
        prodname += '.SEN3'
    if os.path.isdir(iprod):
        pass
    elif zipfile.is_zipfile(arg_iprod):
        is_zip_or_tar = True
        zipf = zipfile.ZipFile(arg_iprod, mode='r')
        zipf.extractall(path=tardir)
        zipf.close()
        iprod = os.path.join(tardir, prodname)
    elif tarfile.is_tarfile(arg_iprod):
        is_zip_or_tar = True
        tarf = tarfile.open(arg_iprod, mode='r')
        tarf.extractall(path=tardir)
        tarf.close()
        iprod = os.path.join(tardir, prodname)
    else:
        if arg_verbose:
            print("## not a valid product: " + arg_iprod)
        #sys.exit()
        return
    prod_dir = os.path.dirname(iprod)

    if not arg_outdir is None and os.path.isdir(arg_outdir):
        outdir = arg_outdir
    else:
        outdir = os.getcwd()

    # read geographic and time coordinates of product
    geoc = Dataset(os.path.join(iprod, 'geo_coordinates.nc'), 'r')
    cols = geoc.dimensions['columns'].size
    rows = geoc.dimensions['rows'].size
    lats = geoc.variables['latitude'][:]
    lons = geoc.variables['longitude'][:]
    start_time = datetime.datetime.strptime(geoc.start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    stop_time = datetime.datetime.strptime(geoc.stop_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    creat_time = datetime.datetime.strptime(geoc.creation_time, "%Y-%m-%dT%H:%M:%SZ")
    preamble = geoc.product_name[0:16]
    timeliness = geoc.product_name[88:90]
    cicle_number = geoc.product_name[69:72]
    orbit_number = geoc.product_name[73:76]
    pname_suffix = geoc.product_name[82:]
    res = geoc.product_name[10:12]
    timec = Dataset(os.path.join(iprod, 'time_coordinates.nc'), 'r')
    # read the epoch, from time_stamp variable attributes
    # should be 2000-01-01T00:00:00
    epoch = datetime.datetime.strptime(timec.variables["time_stamp"].units, "microseconds since %Y-%m-%d %H:%M:%S")

    if res == 'FR':
        ncolmul = 64
    else:
        ncolmul = 16

    # read tie_points
    geoc = Dataset(os.path.join(iprod, 'tie_geo_coordinates.nc'), 'r')
    tie_lats = geoc.variables['latitude'][:]
    tie_lons = geoc.variables['longitude'][:]
    tie_cols = geoc.dimensions['tie_columns'].size
    tie_rows = geoc.dimensions['tie_rows'].size

    # select area on tie coordinates
    g_tie = np.where((tie_lons >= swcrn.x) & (tie_lons <= necrn.x) & (tie_lats >= swcrn.y) & (tie_lats <= necrn.y))
    ng = np.array(np.shape(g_tie))

    if ng[1] <= 0:  # no point in area
        if arg_verbose:
            print("No pixels on selected area")
        if is_zip_or_tar and arg_delete:
            shutil.rmtree(iprod)
        #sys.exit()
        return

    # select first and last rows and cols on tie coordinates
    firsttierow = np.min(g_tie[0])
    if np.min(g_tie[0]) > 0:
        firsttierow = np.min(g_tie[0]) - 1

    lasttierow = np.max(g_tie[0])
    if np.max(g_tie[0]) < tie_rows - 1:
        lasttierow = np.max(g_tie[0]) + 1

    firsttiecol = np.min(g_tie[1])
    if np.min(g_tie[1]) > 0:
        firsttiecol = np.min(g_tie[1]) - 1

    lasttiecol = np.max(g_tie[1])
    if np.max(g_tie[1]) < tie_cols - 1:
        lasttiecol = np.max(g_tie[1]) + 1

    # select first and last rows and cols on the basis of tie rows and cols
    # (tie coordinates derive from lats and lons, 1 column each 64(or 16 in RR) and 1 by 1 for rows)
    firstrow = firsttierow
    lastrow = lasttierow
    if firsttiecol == 0:
        firstcol = firsttiecol
    else:
        firstcol = 1 + (firsttiecol * ncolmul)

    if lasttiecol == 76:
        lastcol = (lasttiecol * ncolmul) - 1
    else:
        lastcol = 1 + (lasttiecol * ncolmul)

    # checking lastcol
    if lastcol >= cols:
        lastcol = cols - 1

    new_row = lastrow - firstrow + 1
    new_col = lastcol - firstcol + 1
    new_tie_row = lasttierow - firsttierow + 1
    new_tie_col = lasttiecol - firsttiecol + 1

    # start and stop times of the trimmed product
    trim_start_time = epoch + datetime.timedelta(microseconds=int(timec.variables["time_stamp"][firstrow]))
    trim_stop_time = epoch + datetime.timedelta(microseconds=int(timec.variables["time_stamp"][lastrow]))
    trimprod_duration = int(np.round((trim_stop_time - trim_start_time).total_seconds()))
    geoc.close()
    timec.close()

    # create trimmed product name
    tts1 = datetime.datetime.strftime(round_to_second(trim_start_time), "%Y%m%dT%H%M%S")
    tts2 = datetime.datetime.strftime(round_to_second(trim_stop_time), "%Y%m%dT%H%M%S")
    ctts = datetime.datetime.strftime(round_to_second(creat_time), "%Y%m%dT%H%M%S")
    new_instance_id = '_trim_EXT_' + cicle_number + '_' + orbit_number  # _trim_BAL_046_022 ===> 17 chars
    trimprodname = preamble + tts1 + '_' + tts2 + '_' + ctts + '_' + new_instance_id + '_' + pname_suffix
    trimprod = os.path.join(outdir, trimprodname)
    if os.path.exists(trimprod):
        if arg_verbose:
            print(f'[WARNING] Trim product: {trimprodname} already exists. Skyping...')
        return trimprodname
    os.makedirs(trimprod, exist_ok=True)
    trimprod = os.path.abspath(trimprod)

    # trim ncfiles from iprod directory to trimprod directory, and copy xfdumanifest.xml

    for pfile in os.listdir(iprod):
        if pfile.endswith(".nc"):
            iprodfile = os.path.join(iprod, pfile)
            trimprodfile = os.path.join(trimprod, pfile)
            src = Dataset(iprodfile, "r")
            dst = Dataset(trimprodfile, "w")
            dst.setncatts(src.__dict__)
            dst.product_name = trimprodname
            dst.start_time = trim_start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            dst.stop_time = trim_stop_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            dst.note = "Trim to selected area of the original product %s" % (src.product_name)
            for name, dimension in src.dimensions.items():
                if "rows" in name:
                    if "tie" in name:
                        dst.createDimension(name, new_tie_row)
                    else:
                        dst.createDimension(name, new_row)
                elif "columns" in name:
                    if "tie" in name:
                        dst.createDimension(name, new_tie_col)
                    else:
                        dst.createDimension(name, new_col)
                else:
                    dst.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))

            for name, variable in src.variables.items():
                x = dst.createVariable(name, variable.datatype, variable.dimensions, zlib=True)
                dst[name].setncatts(src[name].__dict__)
                nvardim = np.array(np.shape(variable.dimensions))
                if "rows" in variable.dimensions[0]:
                    if (nvardim >= 2):
                        if ("columns" in variable.dimensions[1]):
                            if ("tie" in variable.dimensions[0]) & ("tie" in variable.dimensions[1]):
                                dst[name][:] = src[name][firsttierow:lasttierow + 1, firsttiecol:lasttiecol + 1]
                            else:
                                dst[name][:] = src[name][firstrow:lastrow + 1, firstcol:lastcol + 1]
                    else:
                        if ("tie" in variable.dimensions[0]):
                            dst[name][:] = src[name][firsttierow:lasttierow + 1]
                        else:
                            dst[name][:] = src[name][firstrow:lastrow + 1]
                else:
                    dst[name][:] = src[name][:]
            src.close()
            dst.close()
        elif pfile.endswith(".xml"):
            iprodfile = os.path.join(iprod, pfile)
            trimprodfile = os.path.join(trimprod, pfile)
            shutil.copyfile(iprodfile, trimprodfile)

    # modify xfdumanifest.xml file in trimprod directory
    trimprod_size = 0
    xfdumanifest_file = os.path.join(trimprod, 'xfdumanifest.xml')
    xfdumanifest = etree.parse(xfdumanifest_file)
    root = xfdumanifest.getroot()
    # get the namespaces, 'sentinel-safe': 'http://www.esa.int/safe/sentinel/1.1'
    #  'sentinel3' and 'olci', as in first row of xml, xmlns statements
    ns1 = root.nsmap['sentinel-safe']
    ns2 = root.nsmap['sentinel3']
    ns3 = root.nsmap['olci']
    # fix netcdf files (i.e. dataObjects) sizes and checksums
    for dataObject in xfdumanifest.find('/dataObjectSection'):
        ncfilename = dataObject.find('byteStream/fileLocation').attrib['href']
        ncfilename = os.path.join(trimprod, ncfilename)
        ncfilesize = os.path.getsize(ncfilename)
        trimprod_size += ncfilesize
        dataObject.find('byteStream').attrib['size'] = str(ncfilesize)
        if dataObject.find('byteStream/checksum').attrib['checksumName'] == 'MD5':
            dataObject.find('byteStream/checksum').text = md5(ncfilename)
    # go through metadataSection and fix timestamps and names, p
    for metadataObject in xfdumanifest.find('/metadataSection'):
        if metadataObject.attrib['ID'] == 'acquisitionPeriod':
            metadataObject.find(
                'metadataWrap/xmlData/{%s}acquisitionPeriod/{%s}startTime' % (
                    ns1, ns1)).text = trim_start_time.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ")
            metadataObject.find(
                'metadataWrap/xmlData/{%s}acquisitionPeriod/{%s}stopTime' % (ns1, ns1)).text = trim_stop_time.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ")
        if metadataObject.attrib['ID'] == 'generalProductInformation':
            genProdInfo = metadataObject.find('metadataWrap/xmlData/{%s}generalProductInformation' % ns2)
            for prodInfo in genProdInfo.getchildren():
                if prodInfo.tag == '{%s}productName' % ns2:
                    prodInfo.text = trimprodname
                if prodInfo.tag == '{%s}productSize' % ns2:
                    prodInfo.text = str(trimprod_size)
                if prodInfo.tag == '{%s}productUnit' % ns2:
                    prodInfo.find('{%s}duration' % ns2).text = str(trimprod_duration)
        if metadataObject.attrib['ID'] == 'olciProductInformation':
            olciProdInfo = metadataObject.find('metadataWrap/xmlData/{%s}olciProductInformation' % ns3)
            imagesize = olciProdInfo.find('{%s}imageSize' % ns3)
            imagesize.find('{%s}rows' % ns2).text = str(new_row)
            imagesize.find('{%s}columns' % ns2).text = str(new_col)
    # write back the xfdumanifest.xml file
    xfdumanifest.write(xfdumanifest_file)

    # housekeeping
    if is_zip_or_tar and arg_delete:
        shutil.rmtree(iprod)

    # notify output file name
    if arg_verbose:
        print(f'[INFO]Trim product is: {trimprodname}')

    return trimprodname
