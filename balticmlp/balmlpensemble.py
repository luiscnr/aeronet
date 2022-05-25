import numpy as np
from scipy.io import loadmat
import pandas as pd
import os
pd.set_option('display.precision', 4)


class BalMLP():
    def __init__(self,path_file):

        # Six bands: Table 2
        parFile = "blts_rrs412-rrs443-rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file,parFile)
        self.mlp_412_443_490_510_555_670 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Five bands (412 incl.) Table 2
        parFile = "blts_rrs412-rrs443-rrs490-rrs510-rrs555-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file,parFile)
        self.mlp_412_443_490_510_555 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Five bands (670 incl.): Table 2
        parFile = "blts_rrs443-rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file,parFile)
        self.mlp_443_490_510_555_670 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Four bands: Table 3
        parFile = "blts_rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file,parFile)
        self.mlp_490_510_555_670 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Three bands: Table 4
        parFile = "blts_rrs490-rrs510-rrs555-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file,parFile)
        self.mlp_490_510_555 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

    # Compute Chl
    def mlpChl(self,mlpPar, rrs):
        # Data pre-processing
        l = np.log10(rrs)
        x = (l - mlpPar['model'].muIn) / mlpPar['model'].stdIn

        # MLP forward
        z = np.tanh(x.dot(mlpPar['model'].par.w1) + mlpPar['model'].par.b1)
        y = z.dot(mlpPar['model'].par.w2) + mlpPar['model'].par.b2

        # Data post-processing
        chl = 10 ** ((y * mlpPar['model'].stdOut) + mlpPar['model'].muOut)

        # Novelty
        xi = np.inner((l - mlpPar['model'].muIn), mlpPar['model'].pcvecs.T)
        zeta = xi / np.sqrt(mlpPar['model'].pcvals)
        eta = np.transpose(np.linalg.norm(zeta, axis=1, keepdims=True) / l.shape[1])

        # Weight
        wgt = 1 / eta

        return chl, wgt

    def mlp_6_bands(self,rrs):
        return self.mlpChl(self.mlp_412_443_490_510_555_670,rrs)

    def mlp_5_bands_412(self,rrs):
        return self.mlpChl(self.mlp_412_443_490_510_555,rrs)

    def mlp_5_bands_670(self,rrs):
        return self.mlpChl(self.mlp_443_490_510_555_670,rrs)

    def mlp_4_bands(self,rrs):
        return self.mlpChl(self.mlp_490_510_555_670,rrs)

    def mlp_3_bands(self,rrs):
        return self.mlpChl(self.mlp_490_510_555,rrs)

    def compute_chla_ensemble(self,rrs):
        # Chla and weight  (one for each spectral configuration)
        chl_6, wgt_6 = self.mlp_6_bands(rrs[:, [0, 1, 2, 3, 4, 5]])
        chl_5, wgt_5 = self.mlp_5_bands_412(rrs[:, [0, 1, 2, 3, 4]])
        chl_3, wgt_3 = self.mlp_3_bands(rrs[:, [2, 3, 4]])

        # Ensemble
        tmp = np.multiply(chl_6, wgt_6) + np.multiply(chl_5, wgt_5) + np.multiply(chl_3, wgt_3)
        ens = np.divide(tmp, wgt_6 + wgt_5 + wgt_3)

        return ens