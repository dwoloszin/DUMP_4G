import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import warnings
warnings.simplefilter("ignore")

def processArchive():
    fields = ['NodeId','ENodeBFunctionId','EUtranCellFDDId','sectorCarrierRef']
    fields2 = fields
    Folder = 'SiteID'

    
    pathImport = '/import/' + Folder
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+ archiveName +'/' + archiveName + '.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=3, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str,sep = '\t',iterator=True, chunksize=10000, usecols = fields)
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df = df.fillna(0)
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.astype(str)


    frameSI['SITE'] = frameSI['NodeId']
    frameSI.loc[(frameSI['NodeId'].str[-2:-1].isin(['-','_'])),['SITE']] = frameSI['NodeId'].str[:-2]

 

    #print(frameSI)
    
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO