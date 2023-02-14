import ENM
import os
import sys
import pandas as pd
import ShortName

'''
HW 3G
cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=WCDMA,SubNetwork=TSP AuxPlugInUnit.(productName,productNumber,productRevision) -t -s
cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=WCDMA,SubNetwork=TSP PlugInUnit.(unitType) -t -s    


'''



def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=WCDMA,SubNetwork=TSP LocationArea.(lac) -t -s'
  columnsMask = ['RNC','syncStatus','RncFunctionId','LocationArea','LAC'] # use drop to exclude columns that u don't need
  pathToSave = 'DUMP_3G'
  ArchiveName = 'LAC'
  frameSI_LAC = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName)
  frameSI_LAC = frameSI_LAC.astype(str)
  frameSI_LAC = frameSI_LAC.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

  frameSI_LAC['ref2'] = frameSI_LAC['RNC'].astype(str) + frameSI_LAC['LocationArea'].astype(str)
  frameSI_LAC = frameSI_LAC.drop(['RNC','syncStatus','RncFunctionId','LocationArea'], axis=1)
  #csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  #frameSI_LAC.to_csv(csv_path,index=False,header=True,sep=';')
  

  #get all collumns
  #cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=WCDMA,SubNetwork=TSP UtranCell.(*) -t -s
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=WCDMA,SubNetwork=TSP UtranCell.(administrativeState,cid,maximumTransmissionPower,operationalState,primaryCpichPower,primaryScramblingCode,uarfcnDl,uarfcnUl,locationarearef,routingarearef) -t -s'
  columnsMask = ['RNC','SYNCSTATUS','drop1','MO','administrativeState','cId','LocationArea','maximumTransmissionPower','operationalState','primaryCpichPower','primaryScramblingCode','routingArea','uarfcnDl','uarfcnUl']
  pathToSave = 'DUMP_3G'
  ArchiveName = 'DUMP_3G'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName)
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  frameSI = tratarArchive(frameSI)
  frameSI['ref1'] = frameSI['RNC'].astype(str) + frameSI['LocationArea'].astype(str)
  frameSI = pd.merge(frameSI,frameSI_LAC, how='left',left_on=['ref1'],right_on=['ref2'])
  frameSI = frameSI.drop(['ref2','ref1'], axis=1)

  listOrderColumn = ['RNC','MO','administrativeState','cId','maximumTransmissionPower','operationalState','primaryCpichPower','primaryScramblingCode','LocationArea','routingArea','uarfcnDl','uarfcnUl','LAC','SYNCSTATUS']
  frameSI = frameSI[listOrderColumn]


  
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')
  
  
  

  

#Baixar arquivo como csv, alterAR CODIGO

def tratarArchive(frameSI):

  #frameSI['SITE'] = frameSI['MO'].str[:-1]
  frameSI['LocationArea'] = [x.split('LocationArea=')[-1] for x in frameSI['LocationArea']]
  frameSI['routingArea'] = [x.split('RoutingArea=')[-1] for x in frameSI['routingArea']]

  #frameSI['BW DL'] = 5
  #frameSI['BW DL'] = frameSI['BW DL'].astype(int)
  #frameSI['BW DL'] = frameSI['BW DL'].astype(str) + ' M'

  #frameSI['FREQ CELL'] = frameSI['uarfcnDl'].map({'3088':'900','10588':'2100','10662':'2100','10638':'2100','10612':'2100','10738':'2100','10762':'2100'})
  
  #frameSI['Tecnologia'] = '3G'
  #frameSI['VENDOR'] = 'ERICSSON'
  

  return frameSI