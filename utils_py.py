import argparse
import os

import pandas as pd

parser = argparse.ArgumentParser(
    description="General utils")

parser.add_argument('-m', "--mode", help="Mode", choices=['concatdf'])
parser.add_argument('-i', "--input", help="Input", required=True)
parser.add_argument('-o', "--output", help="Output", required=True)
parser.add_argument('-wce', "--wce", help="Wild Card Expression", required=True)
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
args = parser.parse_args()

def main():
    print('[INFO] Started')
    if args.mode=='concatdf':
        input_path = args.input
        output_file = args.output
        wce = '.'
        if args.wce:
            wce = args.wce
        dffin  = None
        for f in os.listdir(input_path):

            if f.endswith('.csv') and f.find(wce)>0:
                if args.verbose:
                    print(f'Input file: {f}')
                input_file = os.path.join(input_path,f)
                dfhere = pd.read_csv(input_file,sep=';')
                if dffin is None:
                    dffin = dfhere
                else:
                    dffin = pd.concat([dffin,dfhere],ignore_index=True)
        dffin.to_csv(output_file,sep=';')
        if args.verbose:
            print(f'Results saved to: {output_file}')



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()