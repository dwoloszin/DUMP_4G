import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime
import ImportDF
import unique
import ShortName
import CountColumnItem
import GroupBy



def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  csv_path = os.path.join(script_dir, 'export/'+ 'HW_ALL' +'/' + 'HW_ALL' + '.csv')
  


  HW_Col_list = ['NodeId','FieldReplaceableUnitId','productionDate','serialNumber','productName']
  HW_path = '/export/HW'
  HW = ImportDF.ImportDF(HW_Col_list,HW_path)
  HW['Ref'] = HW['NodeId']+HW['FieldReplaceableUnitId']
  HW = HW.apply(lambda x: x.str.strip() if x.dtype == "object" else x)


  RadioSector_list = ['Ref1','Ref2']
  RadioSector_path = '/export/RadioSector'
  RadioSector = ImportDF.ImportDF(RadioSector_list,RadioSector_path)


  RadioSector2_list = ['Ref_2','EUtranCellFDD']
  RadioSector2_path = '/export/RadioSector2'
  RadioSector2 = ImportDF.ImportDF(RadioSector2_list,RadioSector2_path)


  Radio_All = pd.merge(RadioSector2,RadioSector, how='left',left_on=['Ref_2'],right_on=['Ref2'])

  HW = pd.merge(HW,Radio_All, how='left',left_on=['Ref'],right_on=['Ref1'])
  HW = HW.drop(['Ref','Ref_2','Ref1','Ref2'],1)

  searchfor = ['B','C','D','E','T']
  m = HW['productionDate'].str.contains('|'.join(searchfor))
  #m = HW['productionDate'].str.contains('C')
  #ini_string2.isdigit()
  #s[s.str.contains('|'.join(searchfor))]
  





  '''    

  HW.loc[m, ['productionDate', 'serialNumber']] = (
    HW.loc[m, ['serialNumber', 'productionDate']].values)
  '''


  HW = ShortName.tratarShortNumber(HW,'NodeId')
  #HW = HW.sort_values(['SHORT'], ascending = [True])
  HW = CountColumnItem.processArchive(HW,'productName','Baseband')
  HW = CountColumnItem.processArchive(HW,'productName','AIR')
  HW = CountColumnItem.processArchive(HW,'productName','DUS')


  productName2 = HW['productName'].unique()
  lista3 = []
  for i in productName2:
    x = i.split(' ')
    #print(x)
    if x[0] not in lista3 and len(x[0]) > 1 and x[0] != 'None':
      lista3.append(x[0])

  print(lista3)
  #Separar os itens
  for y in lista3:
    HW.loc[HW['productName'].str.contains(y),[y]] = HW['productName']
    
  


  #Groupby
  HW1 = GroupBy.processArchive(HW,'SHORT')

  csv_path2 = os.path.join(script_dir, 'export/'+ 'HW_ALL_SHORT' +'/' + 'HW_ALL_SHORT' + '.csv')
  HW1.to_csv(csv_path2,index=False,header=True,sep=';')

  '''
  KeepListCompared = ['SHORT','productName']
  locationBase_comparePMO = list(HW1.columns)
  DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
  HW1 = HW1.drop(DellListComparede,1)
  HW1.rename(columns={'productName':'productName_ALL'},inplace=True)
  print(HW1)

  HW = pd.merge(HW,HW1, how='left',left_on=['SHORT'],right_on=['SHORT'])
  '''







  HW.to_csv(csv_path,index=False,header=True,sep=';')
  

