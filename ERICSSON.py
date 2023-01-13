import ENM
import os
import sys
import pandas as pd
import ShortName


def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP eutrancellfdd.(administrativeState,operationalState,earfcndl,dlChannelBandwidth,tac,cellid,physicalLayerCellIdGroup,physicalLayerSubCellId,freqBand,transmissionmode) -t -s'
  columnsMask = ['NodeId','SYNCSTATUS','drop1','CELL','ADM STATE','CELL ID','BW DL','EARFCNDL','freqBand','OP STATE','physicalLayerCellIdGroup','physicalLayerSubCellId','TAC','transmissionMode'] # use drop to exclude columns that u don't need
  pathToSave = 'ERICSSON'
  ArchiveName = 'ERICSSON_1'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName)
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI = tratarArchive(frameSI)
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP eutrancellfdd.(administrativeState,operationalState,earfcndl,dlChannelBandwidth,tac,cellid,physicalLayerCellIdGroup,physicalLayerSubCellId,freqBand,transmissionmode) -t -s'
  #columnsMask = ['NodeId','drop1','CELL','administrativeState','cellId','BW DL','earfcndl','freqBand','operationalState','physicalLayerCellIdGroup','physicalLayerSubCellId','tac'] # use drop to exclude columns that u don't need
  #pathToSave = 'ERICSSON'
  ArchiveName = 'ERICSSON_2'
  frameSI = ENM.processArchiveReturn(cmd,columnsMask,pathToSave,ArchiveName)
  frameSI = frameSI.astype(str)
  frameSI = frameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
  frameSI = tratarArchive(frameSI)
  frameSI.to_csv(csv_path,index=False,header=True,sep=';')


  

#Baixar arquivo como csv, alterAR CODIGO

def tratarArchive(frameSI):
  frameSI['SITE'] = frameSI['NodeId']
  frameSI.loc[(frameSI['NodeId'].str[-2:-1].isin(['-','_'])),['SITE']] = frameSI['NodeId'].str[:-2]

 
  frameSI['BW DL'] = frameSI['BW DL'].astype(float)/1000
  frameSI['BW DL'] = frameSI['BW DL'].astype(int)
  frameSI['BW DL'] = frameSI['BW DL'].astype(str) + ' M'
  
  frameSI['FREQ CELL'] = frameSI['freqBand'].map({'1':'2100','3':'1800','7':'2600','28':'700'})
  
  frameSI['Tecnologia'] = '4G'
  frameSI = frameSI.loc[frameSI['CELL'] != '0']
  frameSI['PCI'] = frameSI['physicalLayerCellIdGroup'].astype(int)*3 + frameSI['physicalLayerSubCellId'].astype(int)
  frameSI['PCI'] = frameSI['PCI'].astype(int)
  frameSI['VENDOR'] = 'ERICSSON'
  frameSI = ShortName.tratarShortNumber(frameSI,'SITE')
  

  return frameSI