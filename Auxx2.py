import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import re
import datetime
import ShortName


def processArchive():
    Folder = 'AUxx'
    pathImport = '/import/' + Folder
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+ archiveName +'/' + archiveName + '.csv')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    #print (all_filesSI)
    li = []
    
    for filename in all_filesSI:
        dataArchive = filename[len(pathImportSI)+9:len(filename)-4]
        iter_csv = pd.read_csv(filename, index_col=None, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = '/t',iterator=True, chunksize=10000)
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        listData = []
        for index, row in df.iterrows():
          #print(row[0])
          #stringA = row[0].replace('\t',';')
          #print(stringA)
          
          if 'serialNumber=' in row[0]:
            stringA = row[0].replace('\t',';')
            stringA = stringA.replace('{','')
            stringA = stringA.replace('}','')
            stringA = stringA.replace('serialNumber=','')
            stringA = stringA.replace('productionDate=','')
            stringA = stringA.replace('productNumber=','')
            stringA = stringA.replace('productName=','')
            stringA = stringA.replace('productRevision=','')
            #stringA = stringA.replace(' ',';')
            #stringA = stringA.replace(',',';')
            stringA = stringA.split(';')
            stringB = ''
            for i in stringA:
              if i != '':
                stringB += i + ';'
            #print(stringB[:-1].split(';'))
            listData.append(stringB[:-1].split(';'))

        
            #listData.append(stringB[:-1].split(';'))

        #df = pd.DataFrame (listData, columns = ['RNC','MO','drop1','administrativeState','cId','drop2','maximumTransmissionPower','drop3','operationalState','primaryCpichPower','primaryScramblingCode','LocationArea','serviceAreaRef','uarfcnDl','uarfcnUl','Test1','Test2','Test3','Test4','Test5','Test6','Test7','Test8','Test9','Test10','Test11','Test12','Test13','Test14'])
        df = pd.DataFrame (listData, columns = ['NodeId','drop1','EquipmentId','serialNumber','Mover1','Mover2'])
        
        li.append(df)
        
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    
    #tratar Colunas fora
    #frameSI.loc[frameSI['EquipmentId'] == '1',['EquipmentId']] = frameSI['Mover1']
    #frameSI.loc[frameSI['EquipmentId'] == '1',['EquipmentId']] = frameSI['Mover1']
    frameSI.loc[~frameSI['Mover1'].isnull(),['EquipmentId']] = frameSI['Mover1']
    frameSI.loc[~frameSI['Mover2'].isnull(),['serialNumber']] = frameSI['Mover2']
    frameSI = frameSI.drop(['drop1','Mover1','Mover2'],1)
    print(frameSI)
    frameSI[['serial_Number','productionDate','productNumber','productName','productRevision']] = frameSI['serialNumber'].str.split(',',expand=True)
    frameSI = frameSI.drop(['serialNumber'],1)

    #remover 5G
    frameSI = frameSI.loc[~(frameSI['NodeId'].str[:2] == '5G')]
    frameSI = ShortName.tratarShortNumber(frameSI,'NodeId')
    frameSI['productionDate2'] = frameSI['productionDate']
    frameSI.loc[~(frameSI['productionDate'].str[:2] == 'CA'),['productionDate']] = frameSI['serial_Number']
    frameSI.loc[~(frameSI['serial_Number'].str[:2] != 'CA'),['serial_Number']] = frameSI['productionDate2']
    frameSI['Ref_1'] = frameSI['NodeId'] + frameSI['EquipmentId']

    #frameSI = frameSI.drop_duplicates()
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

