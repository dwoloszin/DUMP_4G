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

  
  keep = ['DUS','RRUS','RUS','2203']
  FrameSI.loc[FrameSI['productName'].str.contains('|'.join(keep),na=False),['MOD']] = 'No Mod'
  for index, row in FrameSI.iterrows():
    if row['productName_ALL']:
      xref = []
      productAll = str(row['productName_ALL']).split('|')
      for i in keep:
        for j in productAll:
          if i in j:
            if j not in xref:
              xref.append(j)
    xref = listToString(xref)
    FrameSI.at[index,['No MOD']] = xref
    #print(xref)
  FrameSI.loc[(FrameSI['MOD'].isna()) & (~FrameSI['No MOD'].isna()),['No MOD']] = ''

  return FrameSI

'''  
def common_member(a, b):
    a_set = set(a)
    b_set = set(b)
    if len(a_set.intersection(b_set)) > 0:
        return(True) 
    return(False) 
'''

def listToString(list1):
  if len(list1)> 0:
    stringList = '|'.join(map(str,list1))
  else:
    stringList = ''
  return stringList    

