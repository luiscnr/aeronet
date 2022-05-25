import os
from datetime import datetime
import shutil

def main():
    print('STARTED')
    fbase = '/mnt/c/DATA_LUIS/HYPERNETS_WORK/OLCI_VEIT_UPDATED/HYPSTAR/2022/05'
    year = 2022
    month = 5
    for day in range(1,9):
        datehere = datetime(year,month,day)
        print(datehere)
        dayfolder = datehere.strftime('%d')
        fhere = os.path.join(fbase,dayfolder)
        for seq in os.listdir(fhere):
            seqfolder = os.path.join(fhere,seq)
            if os.path.isdir(seqfolder):
                #print('SEQFOLDER', seqfolder)
                os.rmdir(seqfolder)
                # for fl in os.listdir(seqfolder):
                #     fspectra = os.path.join(seqfolder,fl)
                #     os.remove(fspectra)
                    # fspectra_dest = os.path.join(fhere,fl)
                    # shutil.copyfile(fspectra,fspectra_dest)




if __name__ == '__main__':
    main()