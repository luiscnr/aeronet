from balmlpensemble import BalMLP
import numpy as np
def main():
    print('STARTED')
    balmlp = BalMLP(None)
    rrs = np.array([[0.001901, 0.002393, 0.003571, 0.003793, 0.003952, 0.000817], \
                    [0.002598, 0.002946, 0.003723, 0.003385, 0.002716, 0.000433]])

    ens = balmlp.compute_chla_ensemble(rrs)

    print(ens)

if __name__ == '__main__':
    main()