import ENM
import os
import sys
import pandas as pd


def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP SectorCarrier.(noOfTxAntennas,noOfrxAntennas,sectorShape) -t'
  columnsMask = ['NodeId','drop1','SectorCarrierId','noOfRxAntennas','noOfTxAntennas','sectorShape'] # use drop to exclude columns that u don't need
  pathToSave = 'ANTENNA'
  ArchiveName = 'ANTENNA_1'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName) 
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI = tratarArchive(frameSI)
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')


  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP SectorCarrier.(noOfTxAntennas,noOfrxAntennas,sectorShape) -t'
  ArchiveName = 'ANTENNA_2'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName) 
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI = tratarArchive(frameSI)
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')



  
  
  


def tratarArchive(frameSI):
  frameSI['SITE'] = frameSI['NodeId']
  frameSI.loc[(frameSI['NodeId'].str[-2:-1].isin(['-','_'])),['SITE']] = frameSI['NodeId'].str[:-2]
  
  frameSI['CELL'] = frameSI['SITE'] + frameSI['SectorCarrierId']
  frameSI.loc[(frameSI['SectorCarrierId'].str[:2].isin(['07','21','26','18'])),['CELL']] = frameSI['SITE'] + '-' +  frameSI['SectorCarrierId']
  frameSI['MIMO'] = frameSI['noOfTxAntennas'].astype(str) + 'x' + frameSI['noOfRxAntennas'].astype(str)

  #Verificar Setorização
  frameSI['[S]'] = 'N'
  frameSI.loc[(frameSI['CELL'].str[-2:-1].isin(['A','B','C'])),['[S]']] = 'S'
  return frameSI
