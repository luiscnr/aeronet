import zipfile as zp
import tarfile as tp
import os
from shapely.geometry import Point
from shapely.geometry import Polygon
from netCDF4 import Dataset
import shutil


class CHECK_GEO():
    def __init__(self):
        self.polygon_image = None

    def start_polygon_image_from_line_gml_pos_list(self, line_str):
        clist = line_str[len('<gml:posList>'):line_str.index('</gml:posList>')].split()
        coords = []
        for i in range(0, len(clist), 2):
            coord_here = (float(clist[i + 1]), float(clist[i]))
            coords.append(coord_here)
        self.polygon_image = Polygon(coords)  # create polygon

    def start_polygon_image_from_folder_manifest_file(self, prod_path):
        geoname = os.path.join(prod_path, 'xfdumanifest.xml')
        if os.path.exists(geoname):
            fgeo = open(geoname, 'r')
            for line in fgeo:
                line_str = line.strip()
                if line_str.startswith('<gml:posList>'):
                    self.start_polygon_image_from_line_gml_pos_list(line_str)
            fgeo.close()

    def start_polygon_image_from_zip_manifest_file(self, prod_path):
        with zp.ZipFile(prod_path, 'r') as zprod:
            fname = prod_path.split('/')[-1][0:-4]
            if not fname.endswith('SEN3'):
                fname = fname + '.SEN3'
            geoname = os.path.join(fname, 'xfdumanifest.xml')
            if geoname in zprod.namelist():
                gc = zprod.open(geoname)
                for line in gc:
                    line_str = line.decode().strip()
                    if line_str.startswith('<gml:posList>'):
                        self.start_polygon_image_from_line_gml_pos_list(line_str)
                gc.close()

    def start_polygon_image_from_tar_manifest_file(self, prod_path, unzip_path):
        with tp.TarFile(prod_path, 'r') as tprod:
            fname = prod_path.split('/')[-1][0:-4]
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
                                self.start_polygon_image_from_line_gml_pos_list(line)

    def check_point(self, point_site):
        if not isinstance(point_site, Point):
            return -1
        if point_site.within(self.polygon_image):
            flag_location = 1
        else:
            flag_location = 0
        return flag_location

    def check_point_lat_lon(self, lat_point, lon_point):
        point_site = Point(lon_point, lat_point)
        return self.check_point(point_site)

    def check_polygon(self, polygon_area):
        if not isinstance(polygon_area, Polygon):
            return -1
        if self.polygon_image is None:
            return -1
        if self.polygon_image.intersects(polygon_area):
            flag_location = 1
        else:
            flag_location = 0

        return flag_location

    def check_geo_area(self, south, north, west, east):
        point_list = [Point(west, south), Point(west, north), Point(east, north), Point(east, south),
                      Point(west, south)]
        polygon_area = Polygon([[p.x, p.y] for p in point_list])
        return self.check_polygon(polygon_area)

    def unzip_product(self, path_prod, unzip_path, verbose):
        with zp.ZipFile(path_prod, 'r') as zprod:
            if verbose:
                print(f'Unziping to: {unzip_path}')
            zprod.extractall(path=unzip_path)
        path_prod_u = path_prod.split('/')[-1][0:-4]
        if not path_prod_u.endswith('.SEN3'):
            path_prod_u = path_prod_u + '.SEN3'
        path_prod_u = os.path.join(unzip_path, path_prod_u)
        return path_prod_u

    def untar_product(self, path_prod, unzip_path, verbose):
        with tp.TarFile(path_prod, 'r') as tprod:
            if verbose:
                print(f'Untar to: {unzip_path}')
            tprod.extractall(path=unzip_path)
        path_prod_u = path_prod.split('/')[-1][0:-4]
        if not path_prod_u.endswith('.SEN3'):
            path_prod_u = path_prod_u + '.SEN3'
        path_prod_u = os.path.join(unzip_path, path_prod_u)
        return path_prod_u

    def check_uncompressed_product(self, path_product, year, jday, verbose):
        try:
            for f in os.listdir(path_product):
                if f.endswith('nc'):
                    Dataset(os.path.join(path_product, f))
            return True
        except OSError:
            if verbose:
                print(f'[INFO] Checking path: {path_product}')
            name_dir = path_product.split(('/'))[-1]
            path_base = os.path.dirname(path_product)
            path_no_trimmed = os.path.join(path_base, 'NOTRIMMED')
            path_year = os.path.join(path_no_trimmed, year)
            path_jday = os.path.join(path_year, jday)
            path_end = os.path.join(path_jday, name_dir)
            if not os.path.exists(path_end):
                os.makedirs(path_end)

            for f in os.listdir(path_product):
                shutil.copyfile(os.path.join(path_product, f), os.path.join(path_end, f))
            print(f'[ERROR] Product can not be trimmed. Saved to NOTRIMMED folder {path_end}')
            return False

    def delete_folder_content(self,path_folder):
        res = True
        for f in os.listdir(path_folder):
            try:
                os.remove(os.path.join(path_folder, f))
            except OSError:
                res = False
        return res