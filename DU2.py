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
import ENM



def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  cmd = 'cmedit get * AuxPlugInUnit.(productData) -t'
  columnsMask = ['NodeId','drop1','FieldReplaceableUnitId','productData','Mover1','Mover2']
  pathToSave = 'HW'
  ArchiveName = 'DU_2'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName)
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI = tratarArchive(frameSI)
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')



def tratarArchive(frameSI):  
  frameSI = ShortName.tratarShortNumber(frameSI,'NodeId')
  frameSI = frameSI.astype(str)
  frameSI.loc[frameSI['Mover1'] != 'None',['FieldReplaceableUnitId']] = frameSI['Mover1']
  frameSI.loc[frameSI['Mover2'] != 'None',['productData']] = frameSI['Mover2']
  datalist = ['serialNumber','productionDate','productNumber','productName','productRevision']
  frameSI[datalist] = pd.DataFrame([ x.split(',') for x in frameSI['productData'].tolist() ])
  frameSI = frameSI.astype(str)
  #remover 5G
  frameSI = frameSI.loc[~(frameSI['NodeId'].str[:2] == '5G')]
  for i in datalist:
    frameSI[i] = [x.split('=')[-1] for x in frameSI[i]]
  frameSI = frameSI.loc[frameSI['serialNumber'].astype(str) != '']
  #frameSI['FieldReplaceableUnitId'] = '1'

  #Corrigir CA
  frameSI.loc[frameSI['productionDate'].str[:1].isin(['B','C','D','E','T']),['temp1']] = frameSI['serialNumber']
  frameSI.loc[frameSI['productionDate'].str[:1].isin(['B','C','D','E','T']),['temp2']] = frameSI['productionDate']
  frameSI.loc[~frameSI['temp1'].isna(),['productionDate']] = frameSI['temp1']
  frameSI.loc[~frameSI['temp2'].isna(),['serialNumber']] = frameSI['temp2']

  frameSI = frameSI.drop(['Mover1','Mover1','SHORT','productNumber','productRevision','temp1','temp2'],1)
  listColumn = ['NodeId','FieldReplaceableUnitId','productionDate','serialNumber','productName']
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  frameSI = frameSI[listColumn]
  return frameSI

#Baixar arquivo como csv, alterAR CODIGO
























'''

def processArchive():
    Folder = 'DU2'
    pathImport = '/import/' + Folder
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+ 'HW' +'/' + archiveName + '.csv')
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
        df = pd.DataFrame (listData, columns = ['NodeId','drop1','FieldReplaceableUnitId','serialNumber','Mover1','Mover2'])
        
        li.append(df)
        
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    
    #tratar Colunas fora
    #frameSI.loc[frameSI['FieldReplaceableUnitId'] == '1',['FieldReplaceableUnitId']] = frameSI['Mover1']
    #frameSI.loc[frameSI['FieldReplaceableUnitId'] == '1',['FieldReplaceableUnitId']] = frameSI['Mover1']
    frameSI.loc[~frameSI['Mover1'].isnull(),['FieldReplaceableUnitId']] = frameSI['Mover1']
    frameSI.loc[~frameSI['Mover2'].isnull(),['serialNumber']] = frameSI['Mover2']
    frameSI = frameSI.drop(['drop1','Mover1','Mover2'],1)
    print(frameSI)
    frameSI[['serial_Number','productionDate','productNumber','productName','productRevision']] = frameSI['serialNumber'].str.split(',',expand=True)
    frameSI = frameSI.drop(['serialNumber'],1)
    frameSI.rename(columns={'serial_Number':'serialNumber'},inplace=True)
    

    #remover 5G
    frameSI = frameSI.loc[~(frameSI['NodeId'].str[:2] == '5G')]
    frameSI = frameSI.drop(['productNumber','productRevision'],1)

    #reorder colunmns
    listColumn = ['NodeId','FieldReplaceableUnitId','productionDate','serialNumber','productName']
    frameSI = frameSI[listColumn]
    
    #frameSI = frameSI.drop_duplicates()
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

'''