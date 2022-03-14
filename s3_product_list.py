import os
from datetime import datetime
from s3_lois import S3Product


class S3ProductList:
    def __init__(self, path_list):
        self.product_list = []
        self.time_list = []
        self.time_ini = datetime.now()
        self.time_fin = datetime(1970, 1, 1)
        if os.path.exists(path_list) and os.path.isdir(path_list):
            for f in os.listdir(path_list):
                path_prod = os.path.join(path_list, f)
                prod = S3Product(path_prod)
                if prod.VALID:
                    self.product_list.append(prod)
                    self.time_list.append(prod.date)
                    if prod.date < self.time_ini:
                        self.time_ini = prod.date
                    if prod.date > self.time_fin:
                        self.time_fin = prod.date
