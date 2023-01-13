import ENM
import os
import sys
import pandas as pd
import ShortName


def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP SectorCarrier.(SectorCarrierType) -t'
  columnsMask = ['NodeId','drop1','SectorCarrierId','sectorCarrierType'] # use drop to exclude columns that u don't need
  pathToSave = 'Type'
  ArchiveName = 'Type_1'
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
  frameSI.loc[frameSI['sectorCarrierType'] != 'NORMAL_SECTOR',['MIMO2']] = 'MASSIVE'
  frameSI = frameSI.loc[~frameSI['MIMO2'].isna()]
  frameSI['Celula'] = frameSI['CELL'].str[-1:]
  frameSI['Celula'] = frameSI['Celula'].map({'0':'0', '1':'1', '2':'2', '3':'3', 'A':'1', 'B':'2', 'C':'3', 'D':'4', 'E':'1', 'F':'2', 'G':'3', 'H':'4', 'I':'1', 'J':'2', 'K':'3', 'L':'4', 'M':'1', 'N':'2', 'P':'3', 'Q':'1', 'R':'2', 'S':'3', 'T':'4', 'U':'2', 'V':'3', 'W':'0', 'X':'1', 'Y':'2', 'Z':'3'})
  

  frameSI = ShortName.tratarShortNumber(frameSI,'NodeId')

  countMmimo = frameSI.copy()
  KeepListCompared = ['SHORT','Celula']
  locationBase_comparePMO = list(countMmimo.columns)
  DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
  countMmimo = countMmimo.drop(DellListComparede,1)
  countMmimo = countMmimo.drop_duplicates()
  countMmimo.rename(columns={'Celula':'MIMO'+'(QTD)'},inplace=True)
  countMmimo = countMmimo.groupby(['SHORT'])['MIMO'+'(QTD)'].count()

  frameSI = pd.merge(frameSI,countMmimo, how='left',left_on=['SHORT'],right_on=['SHORT'])
  return frameSI
  