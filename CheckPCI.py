import os
import sys
import glob
import numpy as np
import pandas as pd
import CountColumnItem
import time


def processArchive(frameSI):
  timeexport = time.strftime("%Y%m%d_")
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  csv_path = os.path.join(script_dir, 'export/'+ 'CheckPCI' +'/' +timeexport+ '_Co_PCI' + '.csv')
  
  frameSI['ref_PCI'] = frameSI['SITE'].astype(str) + frameSI['FREQ CELL'].astype(str) + frameSI['EARFCNDL'].astype(str) + frameSI['PCI'].astype(str)
  ERICSSON = frameSI.copy()
  removefromloop = ['LOCATION','CIDADE','SITE','CELL','FREQ CELL','EARFCNDL','PCI','ref_PCI','NodeId2','SYNCSTATUS']
  locationBase_top = list(ERICSSON.columns)
  res = list(set(locationBase_top)^set(removefromloop))
  ERICSSON = ERICSSON.drop(res,1)
  ERICSSON = ERICSSON.drop_duplicates()
  ERICSSON = ERICSSON.reset_index(drop=True)  

  f = CountColumnItem.count2(ERICSSON,'ref_PCI')
  f.loc[f['count'] > 1,['ChannelCheck']] = 'Co-PCI [SameSite]'
  #f.rename(columns={'count':'ChannelCheck'},inplace=True)
  ERICSSON = pd.merge(ERICSSON,f, how='left',left_on=['ref_PCI'],right_on=['ref_PCI'])
  ERICSSON = ERICSSON.loc[ERICSSON['count'] > 1]
  ERICSSON = ERICSSON.drop_duplicates()
  frameSI = pd.merge(frameSI,f, how='left',left_on=['ref_PCI'],right_on=['ref_PCI'])


  



  ERICSSON.to_csv(csv_path,index=False,header=True,sep=';')
  return frameSI
