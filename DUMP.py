import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime
import ImportDF
import unique
import Verificar_OI
import ShortName
import CheckPCI
import HWBlanks
import Mod
import HW_ConsystemCheck


def processArchive():
  
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  csv_path = os.path.join(script_dir, 'export/'+ 'DUMP4G' +'/' + 'DUMP4G' + '.csv')


  MOBILESITE_Col_list = ['Short1','NAME','ANF','CIDADE','LOCATION','LAT','LONG','PROVISION','IMPLEMENTATION']
  MOBILESITE_path = '/export/MOBILESITE'
  MOBILESITE = ImportDF.ImportDF(MOBILESITE_Col_list,MOBILESITE_path)


  ERICSSON_Col_list = ['VENDOR','NodeId','SHORT','SITE','CELL','SYNCSTATUS','ADM STATE','OP STATE','FREQ CELL','EARFCNDL','BW DL','TAC','CELL ID','PCI','transmissionMode']
  ERICSSON_path = '/export/ERICSSON'
  ERICSSON = ImportDF.ImportDF(ERICSSON_Col_list,ERICSSON_path)

  freq_Short = ERICSSON.copy()
  #freq_Short['FREQ CELL'].astype(int) #VERIFICAR ERROR SEM FREQ na coluna
  freq_Short = freq_Short.sort_values(['SHORT','FREQ CELL'], ascending = [True,True])
  freq_Short['BW SITE'] = freq_Short['EARFCNDL'] + '[' + freq_Short['BW DL'] + ']'

  freq_Short['SHORT2'] = freq_Short['SHORT']
  freq_Short = freq_Short.fillna('').groupby(['SHORT2'], as_index=True).agg('|'.join)

  KeepListCompared = ['SHORT','FREQ CELL','BW SITE']
  locationBase_comparePMO = list(freq_Short.columns)
  DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
  freq_Short = freq_Short.drop(DellListComparede,1)
  freq_Short = freq_Short.drop_duplicates()


  removefromloop = []
  locationBase_top = list(freq_Short.columns)
  res = list(set(locationBase_top)^set(removefromloop))
  for i in res: 
      for index, row in freq_Short.iterrows():
          freq_Short.at[index, i] = '|'.join(unique.unique_list(freq_Short.at[index, i].split('|'))) 
  freq_Short.rename(columns={'FREQ CELL':'FREQ SITE'},inplace=True)





  eNBId_Col_list = ['NodeId','ENODEB ID']
  eNBId_path = '/export/eNBId'
  eNBId = ImportDF.ImportDF(eNBId_Col_list,eNBId_path)

  
  ANTENNA_Col_list = ['CELL','[S]','MIMO']
  ANTENNA_path = '/export/ANTENNA'
  ANTENNA = ImportDF.ImportDF(ANTENNA_Col_list,ANTENNA_path)


  #PLMN_Col_list = ['NodeId','PLMN'] keep
  PLMN_Col_list = ['NodeId','eNodeBPlmnId']
  PLMN_path = '/export/PLMN'
  PLMN = ImportDF.ImportDF(PLMN_Col_list,PLMN_path)
  PLMN['PLMN'] = PLMN['eNodeBPlmnId'].str[-3:-1]
  PLMN = PLMN.drop(['eNodeBPlmnId'],1)
  

  Type_Col_list = ['CELL','MIMO2']
  Type_path = '/export/Type'
  Type = ImportDF.ImportDF(Type_Col_list,Type_path)

  
  TypeQtdMimo_Col_list = ['SHORT','MIMO(QTD)']
  TypeQtdMimo_path = '/export/Type'
  TypeQtdMimo = ImportDF.ImportDF(TypeQtdMimo_Col_list,TypeQtdMimo_path)
  

  SI_Col_list = ['CS_NAME','NGNIS_CELL','ANTENA_MODEL','AZIMUTH','ALTURA','MECHANICAL_TILT','DT_ATIV_MOBILE_SITE']
  SI_path = '/export/SI'
  SI = ImportDF.ImportDF(SI_Col_list,SI_path)

  Ancora5G_Col_list = ['EUtranCellFDDId','endcAllowedPlmnList']
  Ancora5G_path = '/export/Ancora5G'
  Ancora5G = ImportDF.ImportDF(Ancora5G_Col_list,Ancora5G_path)


  Sinal5G_Col_list = ['EUtranCellFDDId','primaryUpperLayerInd','additionalUpperLayerIndList']
  Sinal5G_path = '/export/Sinal5G'
  Sinal5G = ImportDF.ImportDF(Sinal5G_Col_list,Sinal5G_path)
  Sinal5G['additionalUpperLayerIndList2'] = Sinal5G['additionalUpperLayerIndList'].str.split(',').str[1]


  HW_Col_list = ['NodeId','EUtranCellFDD','FieldReplaceableUnitId','productName','serialNumber','productionDate','Baseband(QTD)','DUS(QTD)','AIR(QTD)']
  HW_path = '/export/HW_ALL'
  HW = ImportDF.ImportDF(HW_Col_list,HW_path)
  HW.rename(columns={'NodeId':'NodeId2'},inplace=True)


  HW_ALL_SHORT_Col_list = ['SHORT','productName']
  HW_ALL_SHORT_path = '/export/HW_ALL_SHORT'
  HW_ALL_SHORT = ImportDF.ImportDF(HW_ALL_SHORT_Col_list,HW_ALL_SHORT_path)
  HW_ALL_SHORT.rename(columns={'productName':'productName_ALL'},inplace=True)


  RWRtoNR_Col_list = ['EUtranCellFDDId','endcCheckForRwrToNRDisabled','rwrToNRAllowed']
  RWRtoNR_path = '/export/RWRtoNR'
  RWRtoNR = ImportDF.ImportDF(RWRtoNR_Col_list,RWRtoNR_path)
  RWRtoNR.rename(columns={'EUtranCellFDDId':'EUtranCellFDDId_RWRtoNR'},inplace=True)


  B1GUtra_Col_list = ['EUtranCellFDDId','B1GUtra']
  B1GUtra_path = '/export/B1GUtra'
  B1GUtra = ImportDF.ImportDF(B1GUtra_Col_list,B1GUtra_path)
  B1GUtra.rename(columns={'EUtranCellFDDId':'EUtranCellFDDId_B1GUtra'},inplace=True)

  B1NR_Col_list = ['EUtranCellFDDId','B1NR']
  B1NR_path = '/export/B1NR'
  B1NR = ImportDF.ImportDF(B1NR_Col_list,B1NR_path)
  B1NR.rename(columns={'EUtranCellFDDId':'EUtranCellFDDId_B1NR'},inplace=True)




  HW_Mimo = HW.loc[HW['FieldReplaceableUnitId'].str.contains('AIR')]
  HW_Mimo_Col = HW_Mimo.columns
  for i in HW_Mimo_Col:
    HW_Mimo.rename(columns={i:i+'_Mimo'},inplace=True)

  

  #Tratar SLS
  ERICSSON.loc[((ERICSSON['CELL'].str[:3] == '4D-') | (ERICSSON['CELL'].str[:3] == '4C-'))& (np.array(list(map(len,ERICSSON['CELL'].astype(str).values))) == 17),['NodeId']] = ERICSSON['CELL'].str[:9]
  
  ERICSSON.loc[((ERICSSON['CELL'].str[:3] == '4D-') | (ERICSSON['CELL'].str[:3] == '4C-'))& (np.array(list(map(len,ERICSSON['CELL'].astype(str).values))) == 17),['SITE']] = '4D-' + ERICSSON['CELL'].str[3:12]

  

  ERICSSON = pd.merge(ERICSSON,eNBId, how='left',left_on=['NodeId'],right_on=['NodeId'])
  ERICSSON = pd.merge(ERICSSON,freq_Short, how='left',left_on=['SHORT'],right_on=['SHORT'])

  #corrigir 4C-
  ERICSSON['CELL2']= ERICSSON['CELL']
  ERICSSON.loc[ERICSSON['CELL2'].str[:3] == '4C-',['CELL2']] = '4G-' + ERICSSON['CELL2'].str[3:]
  ERICSSON = pd.merge(ERICSSON,ANTENNA, how='left',left_on=['CELL2'],right_on=['CELL'])
  ERICSSON = ERICSSON.drop(['CELL2','CELL_y'],1)
  ERICSSON.rename(columns = {'CELL_x':'CELL'}, inplace = True)

  
  ERICSSON = pd.merge(ERICSSON,Type, how='left',left_on=['CELL'],right_on=['CELL'])
  ERICSSON.loc[~ERICSSON['MIMO2'].isna(),['MIMO']] = ERICSSON['MIMO2']
  ERICSSON = ERICSSON.drop(['MIMO2'],1)

  #Corrigir short no Mobilesite antes de usar como referencia
  ERICSSON = pd.merge(ERICSSON,PLMN, how='left',left_on=['NodeId'],right_on=['NodeId'])
  ERICSSON = pd.merge(ERICSSON,MOBILESITE, how='left',left_on=['SITE'],right_on=['NAME'])
  ERICSSON = ERICSSON.drop(['Short1'],1)


  ERICSSON = pd.merge(ERICSSON,SI, how='left',left_on=['CELL'],right_on=['CS_NAME'])
  ERICSSON = ERICSSON.drop(['CS_NAME'],1)
  ERICSSON = ERICSSON.sort_values(['SHORT','CELL'], ascending = [True,True])

  ERICSSON = pd.merge(ERICSSON,Ancora5G, how='left',left_on=['CELL'],right_on=['EUtranCellFDDId'])

  ERICSSON = pd.merge(ERICSSON,Sinal5G, how='left',left_on=['CELL'],right_on=['EUtranCellFDDId'])

  ERICSSON = pd.merge(ERICSSON,HW, how='left',left_on=['CELL'],right_on=['EUtranCellFDD'])
  
  ERICSSON = pd.merge(ERICSSON,HW_Mimo, how='left',left_on=['NodeId'],right_on=['NodeId2_Mimo'])
  
  ght = ['NodeId2','EUtranCellFDD','FieldReplaceableUnitId','productName','serialNumber','productionDate','Baseband(QTD)','DUS(QTD)','AIR(QTD)']


  for i in ght:
    ERICSSON.loc[ERICSSON[i].isna(),[i]] = ERICSSON[i+'_Mimo']
    ERICSSON = ERICSSON.drop([i+'_Mimo'],1)

  ERICSSON = pd.merge(ERICSSON,HW_ALL_SHORT, how='left',left_on=['SHORT'],right_on=['SHORT'])
  ERICSSON = pd.merge(ERICSSON,TypeQtdMimo, how='left',left_on=['SHORT'],right_on=['SHORT'])
  

  mimoSector = ERICSSON.loc[ERICSSON['MIMO'] == 'MASSIVE']

  
  KeepList2 = ['SHORT', 'CELL']
  locationBase_top = list(mimoSector.columns)
  res = list(set(locationBase_top)^set(KeepList2))
  mimoSector = mimoSector.drop(res,axis=1)
  
  mimoSector['CELL'] = mimoSector['CELL'].str[-1:]
  mimoSector['CELL'] = mimoSector['CELL'].str.upper().map({'A':'A', 'B':'B', 'C':'C','I':'A', 'J':'B', 'K':'C'})
  mimoSector = mimoSector.drop_duplicates()
  mimoSector = mimoSector.reset_index(drop=True)
  mimoSector = mimoSector.sort_values(['SHORT','CELL'], ascending = [True,True])

  mimoSector['CELL2'] = mimoSector['SHORT']
  mimoSector = mimoSector.fillna('').groupby(['CELL2'], as_index=True).agg('|'.join)
  removefromloop = []
  locationBase_top = list(mimoSector.columns)
  res = list(set(locationBase_top)^set(removefromloop))
  for i in res: 
      for index, row in mimoSector.iterrows():
          mimoSector.at[index, i] = '|'.join(unique.unique_list(mimoSector.at[index, i].split('|')))
  mimoSector.rename(columns={'CELL':'CELL(Mimo)','SHORT':'SHORTM'},inplace=True)

  ERICSSON = pd.merge(ERICSSON,mimoSector, how='left',left_on=['SHORT'],right_on=['SHORTM'])


  ERICSSON = pd.merge(ERICSSON,RWRtoNR, how='left',left_on=['CELL'],right_on=['EUtranCellFDDId_RWRtoNR'])
  ERICSSON = pd.merge(ERICSSON,B1GUtra, how='left',left_on=['CELL'],right_on=['EUtranCellFDDId_B1GUtra'])
  ERICSSON = pd.merge(ERICSSON,B1NR, how='left',left_on=['CELL'],right_on=['EUtranCellFDDId_B1NR'])

  
  #tratar HW blanks
  ERICSSON = HWBlanks.processArchive(ERICSSON)

  #Consystem check [Em desenvolvimento]
  #ERICSSON = HW_ConsystemCheck.processArchive(ERICSSON)
  #Verificar Modernizados[em desenvolvimento]
  #ERICSSON = Mod.processArchive(ERICSSON)

  
  ERICSSON = Verificar_OI.processArchive(ERICSSON)
  ERICSSON = CheckPCI.processArchive(ERICSSON)
  
  






  ERICSSON.to_csv(csv_path,index=False,header=True,sep=';')
  
  


