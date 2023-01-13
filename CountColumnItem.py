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


def processArchive(frameSI,columnref,itemFilter):
  frameBB = frameSI.copy()
  frameBB = frameSI.loc[frameSI[columnref].str.contains(itemFilter)]
  frameBB = frameBB.loc[frameBB['SHORT'] != 'nan']
  frameBB.rename(columns={columnref:itemFilter+'(QTD)'},inplace=True)
  frameBB = frameBB.groupby(['SHORT'])[itemFilter+'(QTD)'].count()
  
  frameSI = pd.merge(frameSI,frameBB, how='left',left_on=['SHORT'],right_on=['SHORT'])

  return frameSI


def count2(df,ref):
    
  dataframe = df.copy()
  removefromloop = [ref]
  locationBase_top = list(dataframe.columns)
  res = list(set(locationBase_top)^set(removefromloop))
  dataframe = dataframe.drop(res,1) 
  dataframe['count'] = dataframe.groupby(ref)[ref].transform('count')
  return dataframe  