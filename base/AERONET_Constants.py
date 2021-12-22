from datetime import datetime

class AERONETConstants:
    def __init__(self):


        self.DATEREF = datetime(1970, 1, 1, 0, 0, 0)
        self.ThuillierMethodDefault = 'interp'

        #CSV Original File

        #NCFile
        self.dim_time = 'TimeIndex'
        self.dim_wl = 'Central_wavelenghts'