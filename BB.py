import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import warnings
import ShortName
import unique
import GroupBy
import ENM
warnings.simplefilter("ignore")

def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  cmd = 'cmedit get * FieldReplaceableUnit.productData -t'
  columnsMask = ['NodeId','EquipmentId','FieldReplaceableUnitId','productData']
  pathToSave = 'HW'
  ArchiveName = 'BB'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName)
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI = tratarArchive(frameSI)
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')



def tratarArchive(frameSI):    
  datalist = ['productionDate','serialNumber','productNumber','productName','productRevision']
  frameSI[datalist] = pd.DataFrame([ x.split(',') for x in frameSI['productData'].tolist() ])
  frameSI = frameSI.astype(str)
  #remover 5G
  frameSI = frameSI.loc[~(frameSI['NodeId'].str[:2] == '5G')]
  
  for i in datalist:
    frameSI[i] = [x.split('=')[-1] for x in frameSI[i]]

  #Corrigir CA
  frameSI.loc[frameSI['productionDate'].str[:1].isin(['B','C','D','E','T']),['temp1']] = frameSI['serialNumber']
  frameSI.loc[frameSI['productionDate'].str[:1].isin(['B','C','D','E','T']),['temp2']] = frameSI['productionDate']
  frameSI.loc[~frameSI['temp1'].isna(),['productionDate']] = frameSI['temp1']
  frameSI.loc[~frameSI['temp2'].isna(),['serialNumber']] = frameSI['temp2']
  frameSI = frameSI.drop(['temp1','temp2'],1)

  frameSI = frameSI.drop(['EquipmentId','productData','productNumber','productRevision'],1)
  return frameSI
