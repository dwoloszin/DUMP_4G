import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import re
import datetime
import ENM


def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  cmd = 'cmedit get * SectorCarrier.(reservedBy,sectorFunctionRef) -t'
  columnsMask = ['NodeId','ENodeBFunctionId','SectorCarrierId','reservedBy','sectorFunctionRef']
  pathToSave = 'RadioSector2'
  ArchiveName = 'RadioSector_2'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName)
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI = tratarArchive(frameSI)
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')



def tratarArchive(frameSI):
  df = frameSI.astype(str)
  #remover 5G
  listData = []
  frameSI = frameSI.loc[~(frameSI['NodeId'].str[:2] == '5G')]
  li = []
  for index, row in df.iterrows(): 
    #print(row['rfPortRef'])  
    if 'MeContext=' in row['sectorFunctionRef']:
      string0 = row['reservedBy'].replace('\t',';')
      stringA = row['sectorFunctionRef'].replace('\t',';')
      SectorEqui = string0.partition('UlCompGroup=')[2].split(',')[0]
      SectorEqui = SectorEqui[:-1]  
      AuxPlugInUnit = stringA.partition('SectorEquipmentFunction=')[2].split(',')[0]
      siteid = stringA.partition(',MeContext=')[2].split(',')[0]
      MeContext = string0.partition('EUtranCellFDD=')[2].split(',')[0]
      stringA = stringA.split(';')
      stringB = AuxPlugInUnit+';'+siteid+';'+MeContext+';'+SectorEqui+';'
      
      for i in stringA:
        if i != '':
          stringB += i + ';'
      #print(stringB[:-1].split(';'))
      listData.append(stringB[:-1].split(';'))

  #df = pd.DataFrame (listData, columns = ['AuxPlugInUnit','FieldReplaceableUnit','NodeId','EquipmentId','AntennaUnitGroupId','RfBranchId','reservedBy','rfPortRef'])      
  df = pd.DataFrame (listData, columns = ['SectorEquipmentFunction','MeContext','EUtranCellFDD','Freq','ref'])      
  li.append(df)
  frameSI2 = pd.concat(li, axis=0, ignore_index=True)

  
  frameSI2['Ref_2'] = frameSI2['MeContext'].astype(str) + frameSI2['SectorEquipmentFunction'].astype(str)
  frameSI2.loc[frameSI2['EUtranCellFDD'].str[-1:] == ']',['EUtranCellFDD']] = frameSI2['EUtranCellFDD'].str[:-1]

  frameSI2 = frameSI2.drop(['SectorEquipmentFunction','MeContext','ref','Freq'],1)
  frameSI2 = frameSI2.loc[(np.array(list(map(len,frameSI2['EUtranCellFDD'].values))) >= 1)]
  frameSI2 = frameSI2.drop_duplicates()
  return frameSI2


























'''
def processArchive():
    Folder = 'RadioSector2'
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
          
          if 'MeContext=' in row[0]:
            stringA = row[0].replace('\t',';')
            AuxPlugInUnit = stringA.partition('EUtranCellFDD=')[2].split(',')[0]
            AuxPlugInUnit = AuxPlugInUnit.split(']')[0]
            FieldReplaceableUnit = stringA.partition('UlCompGroup=')[2].split(']')[0]
            SectorEquipmentFunction = stringA.partition('SectorEquipmentFunction=')[2].split(',')[0]

            stringA = stringA.split(';')
            stringB = AuxPlugInUnit+';'+FieldReplaceableUnit+';' + SectorEquipmentFunction + ';'
            
            for i in stringA:
              if i != '':
                stringB += i + ';'
            #print(stringB[:-1].split(';'))
            listData.append(stringB[:-1].split(';'))

        
            #listData.append(stringB[:-1].split(';'))

        #df = pd.DataFrame (listData, columns = ['RNC','MO','drop1','administrativeState','cId','drop2','maximumTransmissionPower','drop3','operationalState','primaryCpichPower','primaryScramblingCode','LocationArea','serviceAreaRef','uarfcnDl','uarfcnUl','Test1','Test2','Test3','Test4','Test5','Test6','Test7','Test8','Test9','Test10','Test11','Test12','Test13','Test14'])
        df = pd.DataFrame (listData, columns = ['EUtranCellFDD','UlCompGroup','SectorEquipmentFunction','NodeId','ENodeBFunctionId','SectorCarrierId','reservedBy','sectorFunctionRef'])
        
        li.append(df)
        
    frameSI = pd.concat(li, axis=0, ignore_index=True)

    frameSI.loc[frameSI['EUtranCellFDD'] == '',['EUtranCellFDD']] = frameSI['NodeId']
    frameSI['Ref_2'] = frameSI['NodeId'] + frameSI['SectorEquipmentFunction']
    frameSI = frameSI.drop(['NodeId','SectorEquipmentFunction','UlCompGroup','ENodeBFunctionId','SectorCarrierId','reservedBy','sectorFunctionRef'],1)
    
    frameSI = frameSI.drop_duplicates()
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')
'''
