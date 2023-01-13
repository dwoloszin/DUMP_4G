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
  #keep = ['B1']
  #FrameSI.loc[(FrameSI['productName'].str.contains('|'.join(keep),na=False))&(~FrameSI['FREQ CELL'].str.contains('2100')),['Verificar']] = 'Verificar'


  #separar HW por banda
  for index, row in FrameSI.iterrows():
    if row['productName']:
      xref = []
      productAll = str(row['productName']).split(' ')
    
      for i in keep:
        for j in productAll:
          if i in j:
            if j not in xref:
              xref.append(j)
    xref = listToString(xref)
    FrameSI.at[index,['No MOD']] = xref
    #print(xref)



  return FrameSI
      



