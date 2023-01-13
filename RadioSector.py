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
  cmd = 'cmedit get * RfBranch.(reservedBy,rfPortRef) -t'
  columnsMask = ['NodeId','EquipmentId','AntennaUnitGroupId','RfBranchId','reservedBy','rfPortRef']
  pathToSave = 'RadioSector'
  ArchiveName = 'RadioSector_1'
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
    if 'MeContext=' in row['rfPortRef']:
      string0 = row['reservedBy'].replace('\t',';')
      stringA = row['rfPortRef'].replace('\t',';')
      SectorEqui = string0.partition('SectorEquipmentFunction=')[2].split(',')[0]
      SectorEqui = SectorEqui[:-1]  
      AuxPlugInUnit = stringA.partition('AuxPlugInUnit=')[2].split(',')[0]
      FieldReplaceableUnit = stringA.partition('FieldReplaceableUnit=')[2].split(',')[0]
      MeContext = stringA.partition('MeContext=')[2].split(',')[0]
      stringA = stringA.split(';')
      stringB = AuxPlugInUnit+';'+FieldReplaceableUnit+';'+MeContext+';'+SectorEqui+';'
      
      for i in stringA:
        if i != '':
          stringB += i + ';'
      #print(stringB[:-1].split(';'))
      listData.append(stringB[:-1].split(';'))

  #df = pd.DataFrame (listData, columns = ['AuxPlugInUnit','FieldReplaceableUnit','NodeId','EquipmentId','AntennaUnitGroupId','RfBranchId','reservedBy','rfPortRef'])      
  df = pd.DataFrame (listData, columns = ['RRU1','RRU2','MeContext','AntennaUnitGroupId','teste'])      
  li.append(df)
  frameSI2 = pd.concat(li, axis=0, ignore_index=True)
  #frameSI2.loc[frameSI2['RRU1'].isna(),['RRU1']] = frameSI2['RRU2']
  #frameSI2['RRU1'] = np.where(frameSI2['RRU1'].isnull(), frameSI2['RRU2'], frameSI2['RRU1'])
  frameSI2.loc[(np.array(list(map(len,frameSI2['RRU1'].values))) <= 1),['RRU1']] = frameSI2['RRU2']
  
  frameSI2['Ref1'] = frameSI2['MeContext'].astype(str) + frameSI2['RRU1'].astype(str)
  frameSI2['Ref2'] = frameSI2['MeContext'].astype(str) + frameSI2['AntennaUnitGroupId'].astype(str)

  frameSI2 = frameSI2.drop(['RRU1','RRU2','MeContext','teste'],1)
  frameSI2 = frameSI2.drop_duplicates()
  frameSI2 = frameSI2.loc[(np.array(list(map(len,frameSI2['AntennaUnitGroupId'].values))) >= 1)]
  return frameSI2
















