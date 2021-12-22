import math
import os
import sys

from netCDF4 import Dataset
import numpy as np


def get_F0(wl, method):
    if method == 'interp':
        return get_F0_interp(wl)


def get_F0_array(wlarray, method):
    wlout = np.array(wlarray, dtype=float)
    if method == 'interp':
        for i in range(len(wlarray)):
            if wlarray[i] > 0:
                wlout[i] = get_F0_interp(wlarray[i])
    return wlout


# Method intep: Make an interpolation (point)
def get_F0_interp(wl):
    path_to_file = os.path.join(sys.path[0], 'thuillier', 'Thuillier_F0.nc')
    nc_f0 = Dataset(path_to_file, 'r')

    Wavelength = nc_f0.variables['Wavelength'][:]
    F0= nc_f0.variables['F0'][:]
    F0_val = np.interp(wl, Wavelength, F0)

    return F0_val
