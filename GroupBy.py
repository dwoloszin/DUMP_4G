import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
import warnings
import unique


def processArchive(frameSI,Column_group):
  frameSI = frameSI.astype(str)
  frameSI['GroupByColumn'] = frameSI[Column_group]
    
  frameSI = frameSI.fillna('').groupby(['GroupByColumn'], as_index=True).agg('|'.join)

  removefromloop = []
  locationBase_top = list(frameSI.columns)
  res = list(set(locationBase_top)^set(removefromloop))
  
  
  for i in res: 
      for index, row in frameSI.iterrows():
          frameSI.at[index, i] = '|'.join(unique.unique_list(frameSI.at[index, i].split('|')))            
  
  return frameSI