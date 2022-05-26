import os

import s3olcitrim_bal_frompy as trimtool
import argparse

parser = argparse.ArgumentParser(description="Search and trim S3 Level 1B products around a in-situ location")
parser.add_argument('-s', "--sourcedir", help="Source directory")
parser.add_argument('-p', "--sourceproduct", help="Source directory")
parser.add_argument('-o', "--outputdir", help="Output directory", required=True)
parser.add_argument('-geo', "--geo_limits", help="Geo limits", required=True)
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()


def main():
    ##TO CHECK REMAINING IDEPIX FILE
    input_path = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Irbe_Lighthouse/trim'
    output_path = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/TRIMMED/Irbe_Lighthouse/IDEPIX/results'
    b = check(input_path, output_path)
    if b:
        return
    ####################
    geo_coords = args.geo_limits
    if geo_coords == 'BAL':
        s = 50
        n = 65
        w = 5
        e = 35
    elif geo_coords == 'BAL_GDT':
        s = 58
        n = 59
        w = 17
        e = 18
    elif geo_coords == 'BAL_ILH':
        s = 57.4
        n = 58.4
        w = 21.4
        e = 22.4
    else:
        geo_list = geo_coords.split('_')
        s = float(geo_list[0].strip())
        n = float(geo_list[1].strip())
        w = float(geo_list[2].strip())
        e = float(geo_list[3].strip())
    out_dir = args.outputdir
    if args.sourceproduct:
        path_prod = args.sourceproduct
        trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir, args.verbose)
    elif args.sourcedir:
        for path in os.listdir(args.sourcedir):
            path_prod = os.path.join(args.sourcedir, path)
            print(f'[INFO] Trimming product: {path_prod}')
            trimtool.make_trim(s, n, w, e, path_prod, None, False, out_dir, args.verbose)


def check(input_path, output_path):
    for file in os.listdir(input_path):
        name = file[:-5]
        idepixname = f'{name}_IDEPIX.nc'
        path_out = os.path.join(output_path, idepixname)
        if not os.path.exists(path_out):
            print(name)
    return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
