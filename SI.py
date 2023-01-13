import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
from datetime import datetime
#import unique

def processArchive():
    fields = ['LOCATION','CS_NAME','MS_TYPE','CS_STATUS','LATITUDE','LONGITUDE','REGIONAL','ANF','MUNICIPIO','IBGE_ID','ANTENA_MODEL','AZIMUTH','ALTURA','MECHANICAL_TILT','DT_ATIV_MOBILE_SITE']
    fields2 = ['LOCATION','CS_NAME','SITE_TYPE','NGNIS_CELL','LAT','LONG','REGIONAL','ANF','CIDADE','IBGE_ID','ANTENA_MODEL','AZIMUTH','ALTURA','MECHANICAL_TILT','DT_ATIV_MOBILE_SITE']
    Folder = 'SI'

    pathImport = '/import/' + Folder
    #filtrolabel = 'REGIONAL'
    #filtroValue = 'TSP'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+ archiveName +'/' + archiveName + '.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    lastData = all_filesSI[0][len(all_filesSI[0])-19:len(all_filesSI[0])-11]
    for filename in all_filesSI:
        dataArchive = datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv]) # WORKS
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels   
        li.append(df2)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.astype(str)
    frameSI = frameSI.drop_duplicates()

    frameSI = frameSI.apply(lambda x: x.str.replace('.',','))
    #frameSI.rename(columns={'LOCATION': 'Endereço ID','LATITUDE': 'Latitude','LONGITUDE': 'Longitude','MUNICIPIO':'Município'},inplace = True)
    




    frameSI.dropna(subset = ['LOCATION','LAT','LONG'], inplace=True)
    frameSI = frameSI.reset_index(drop=True)

    frameSI.to_csv(csv_path,encoding='UTF-8',index=True,header=True,sep=';')

