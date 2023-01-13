import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime
import ImportDF
import unique
import time


def processArchive(FrameSI):
  #Tratar espaços Removing all spaces from 
  #To remove consecutive spaces from a column in Pandas 
  FrameSI = FrameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  frame2 = FrameSI.loc[FrameSI['productName'].isna()]

  KeepListCompared = ['CELL','productName','productName_ALL','FREQ CELL']
  locationBase_comparePMO = list(frame2.columns)
  DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
  frame2 = frame2.drop(DellListComparede,1)

  frame2 = verificar(frame2,'B1','2100')
  frame2 = verificar(frame2,'B3','1800')
  frame2 = verificar(frame2,'B7','2600')
  frame2 = verificar(frame2,'B28','700')
  frame2 = frame2.drop(['productName_ALL','FREQ CELL'],1)
  frame2.rename(columns={'CELL':'CELL2','productName':'productName2'},inplace=True)

  FrameSI = pd.merge(FrameSI,frame2, how='left',left_on=['CELL'],right_on=['CELL2'])
  FrameSI = FrameSI.drop(['CELL2'], axis=1)

  FrameSI.loc[FrameSI['productName'].isna(),['productName']] = FrameSI['productName2']
  FrameSI = FrameSI.drop(['productName2'], axis=1)


  return FrameSI
  
def common_member(a, b):
    a_set = set(a)
    b_set = set(b)
    if len(a_set.intersection(b_set)) > 0:
        return(True) 
    return(False) 

def verificar(frame2,band,freq):
  for index, row in frame2.iterrows():
    if row['productName_ALL']:
      splitValue = str(row['productName_ALL']).split('|')
      xref = []
      for i in splitValue:
        if band in i:
          xref.append(i)
      stringList = '|'.join(map(str,xref))
      if len(stringList) > 0 and row['FREQ CELL'] == freq:
        frame2.at[index,['productName']] = stringList
        #print(stringList)
  return frame2