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

'''
email: CHECK ESPECTRO OI - DUMP 4G


## BB OI EXTRA ##
@ TSP 11 1800/2100 AIR 1641:
1 mimo - BB básica + BB mimo + BB oi - 3 total
2 mimo - BB básica + 2 BB mimo + BB oi - 4 total    
3 mimo - BB básica + 3 BB mimo + 2 BB oi -  6 total
@ TSPi 1800  AIR 1641:
1 mimo - BB básica + BB mimo e Oi + 0 - 2 total
2 mimo - BB básica + BB mimo + BB oi - 3 total
3 mimo - BB básica + 2 BB mimo + BB oi - 4 total
@ TSPi 1800  AIR 3246:
1 mimo - BB básica + BB mimo e Oi + 0 - 2 total
2 mimo - BB básica + BB mimo + BB oi - 3 total
3 mimo - BB básica + 2 BB mimo + BB oi - 3 total  
!AIR 3246 só suporta 2 carriers + 2DS se tiver 3 fibras CPRI passadas da BB p AIR.
!Normalmente são apenas 2 FOs.     
'''


def processArchive(FrameSI):
  #Tratar espaços Removing all spaces from 
  #To remove consecutive spaces from a column in Pandas 
  FrameSI = FrameSI.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
  Suport_Y = ['AIR 1641 B1a B3a','AIR 3246 B3','Radio 2219 B3','Radio 2279 22B1 22B3 C','Radio 4415 B3','Radio 4443 B1 B3','Radio 4480 44B1 44B3 C']
  Suport_N = ['Radio 2203 B3','Radio 2217 B3','RRUS 01 B3','RRUS 12 B3','RUS 01 B3','RUS 02 B3','RD 4442 B3B7','RD 2242 B3']
  CTBC = ['ALTINOPOLIS','ARAMINA','BATATAIS','BRODOWSKI','BURITIZAL','CAJURU','CASSIA DOS COQUEIROS','COLOMBIA','FRANCA','GUAIRA','GUARA','IPUA','ITUVERAVA','JARDINOPOLIS','MIGUELOPOLIS','MORRO AGUDO','NUPORANGA','ORLANDIA','RIBEIRAO CORRENTE','SALES OLIVEIRA','SANTA CRUZ DA ESPERANCA','SANTO ANTONIO DA ALEGRIA','SAO JOAQUIM DA BARRA']
  Suport_IF = ['RRUS 12 B3'] #CTBC
  Suport_IF2 = ['Radio 2203 B3']
  B1800 = ['RRUS 12 B3','Radio 2219 B3','RRUS 01 B3','RUS 02 B3','RUS 01 B3','Radio 2217 B3','AIR 1641 B1a B3a','AIR 3246 B3','RD 4442 B3B7','RD 2242 B3']
  for index, row in FrameSI.iterrows():
    if row['productName_ALL']:
      splitValue = str(row['productName_ALL']).split('|')
      freqsplit = str(row['FREQ SITE']).split('|')

      if not common_member(splitValue,B1800):
        FrameSI.at[index,['CHECK HW']] = 'SEM HW 1800'
      
      if not common_member(splitValue,B1800) and common_member(freqsplit,['1800']) :
        FrameSI.at[index,['CHECK HW']] = 'SEM INFO'

      if common_member(splitValue,Suport_Y):
        FrameSI.at[index,['CHECK HW']] = 'HW Suport OI'

      if common_member(splitValue,Suport_N):
        FrameSI.at[index,['CHECK HW']] = 'HW Nao Suport OI'
      
      if common_member([row['CIDADE']],CTBC) and common_member(splitValue,Suport_IF):
        FrameSI.at[index,['CHECK HW']] = 'SUPORTA 5 MHz'
        FrameSI.at[index,['CTBC?']] = 'SIM'
      
      if row['ANF'] != '11' and common_member(splitValue,Suport_IF2):
        FrameSI.at[index,['CHECK HW']] = 'HW Suport OI'
      
      # 3246 na 11 não suporta
      if (row['ANF'] == '11') and common_member(splitValue,['AIR 3246 B3']):
        FrameSI.at[index,['CHECK HW']] = 'HW Nao Suport OI'
      

      # MIMO ANALISE
      if not float(row['MIMO(QTD)']) > 0.0:
        FrameSI.at[index,['CHECK BB MIMO']] = 'No Mimo Sector'
      
      #11 ANALISE IF mimo
      calc = 0
      if float(row['MIMO(QTD)']) <= 2.0:
        calc = float(row['MIMO(QTD)']) + 2.0 # 1 bb base + 1 bb OI
      if float(row['MIMO(QTD)']) == 3.0:
        calc = float(row['MIMO(QTD)']) + 3.0 # 1 bb base + 2 bb OI
      if (float(row['Baseband(QTD)']) >= calc) and (row['ANF'] == '11') and (common_member(splitValue,['AIR 1641 B1a B3a','AIR 3246 B3'])):
        FrameSI.at[index,['CHECK BB MIMO']] = 'Suport OI'

      # <> 11
      calc = 0
      calc = float(row['MIMO(QTD)']) + 1.0 # 1bb mimo + 1 oi
      if (float(row['Baseband(QTD)']) >= calc) and (row['ANF'] != '11') and (common_member(splitValue,['AIR 1641 B1a B3a'])):
        FrameSI.at[index,['CHECK BB MIMO']] = 'Suport OI'

      calc = 0
      if float(row['MIMO(QTD)']) <= 2.0:
        calc = float(row['MIMO(QTD)']) + 1.0 
      if float(row['MIMO(QTD)']) == 3.0:
        calc = float(row['MIMO(QTD)']) 
      if (float(row['Baseband(QTD)']) >= calc) and (row['ANF'] != '11') and (common_member(splitValue,['AIR 3246 B3'])):
        FrameSI.at[index,['CHECK BB MIMO']] = 'Suport OI'

  FrameSI.loc[FrameSI['CHECK BB MIMO'].isna(),['CHECK BB MIMO']] = 'Not Suport OI'
  
  
  #short freque 1800
  FrameSI_0 = FrameSI[['SHORT','FREQ CELL']]
  FrameSI_0 = FrameSI_0.loc[FrameSI_0['FREQ CELL'].astype(str).isin(['1800','1800s'])]
  FrameSI_0 = FrameSI_0.drop(['FREQ CELL'],1)  
  FrameSI_0 = FrameSI_0.drop_duplicates()
  FrameSI_0['Tem1800'] = 'SIM'

  #short Suport OI
  FrameSI_1 = FrameSI[['SHORT','CHECK BB MIMO']]
  FrameSI_1 = FrameSI_1.loc[FrameSI_1['CHECK BB MIMO'].isna()]
  FrameSI_1 = FrameSI_1.drop(['CHECK BB MIMO'],1)  
  FrameSI_1 = FrameSI_1.drop_duplicates()
  FrameSI_1['Suporta OI'] = 'NAO'

  # OI 1700, 1525, 1575 não Tem OI
  FrameSI_2 = FrameSI[['SHORT','EARFCNDL']]
  FrameSI_2 = FrameSI_2.loc[~FrameSI_2['EARFCNDL'].astype(str).isin(['1700','1525','1575'])]
  FrameSI_2 = FrameSI_2.drop(['EARFCNDL'],1) 
  FrameSI_2 = FrameSI_2.drop_duplicates()


   # OI 1700, 1525, 1575 Tem OI
  FrameSI_3 = FrameSI[['SHORT','EARFCNDL']]
  FrameSI_3 = FrameSI_3.loc[FrameSI_3['EARFCNDL'].astype(str).isin(['1700','1525','1575'])] 
  FrameSI_3 = FrameSI_3.drop(['EARFCNDL'],1) 
  FrameSI_3 = FrameSI_3.drop_duplicates()
  FrameSI_3['TEM OI'] = 'SIM'

  FrameSI_2 = pd.merge(FrameSI_2,FrameSI_3, how='left',left_on=['SHORT'],right_on=['SHORT'])
  #FrameSI_2 = pd.merge(FrameSI_2,FrameSI_1, how='left',left_on=['SHORT'],right_on=['SHORT'])
  FrameSI_2 = pd.merge(FrameSI_2,FrameSI_0, how='left',left_on=['SHORT'],right_on=['SHORT'])


  FrameSI = pd.merge(FrameSI,FrameSI_2, how='left',left_on=['SHORT'],right_on=['SHORT'])





  
  #analise OI
  FrameSI.loc[(~FrameSI['Tem1800'].isna())& 
              (~FrameSI['MIMO(QTD)'].isna())& 
              (FrameSI['TEM OI'].isna())&
              (FrameSI['CHECK HW'].astype(str) == 'HW Suport OI')&
              (FrameSI['CHECK BB MIMO'].astype(str) == 'Suport OI'),['STATUS FINAL ESPECTRO OI']] = 'Suporta configurar Espectro Oi'
  
  FrameSI.loc[(~FrameSI['Tem1800'].isna())& 
              (FrameSI['MIMO(QTD)'].isna())& 
              (FrameSI['TEM OI'].isna())&
              (FrameSI['CHECK HW'].astype(str) == 'HW Suport OI'),['STATUS FINAL ESPECTRO OI']] = 'Suporta configurar Espectro Oi'

  FrameSI.loc[FrameSI['STATUS FINAL ESPECTRO OI'].isna(),['STATUS FINAL ESPECTRO OI']] = 'Nao Suporta configurar Espectro Oi'
  FrameSI.loc[~FrameSI['TEM OI'].isna(),['STATUS FINAL ESPECTRO OI']] = 'Espectro Oi Configurado'
  FrameSI.loc[(FrameSI['STATUS FINAL ESPECTRO OI'] == 'Suporta configurar Espectro Oi') & (FrameSI['ANF'] != '11') & (FrameSI['productName_ALL'].str.contains('AIR 3246 B3')),['STATUS FINAL ESPECTRO OI']] = 'Suporta configurar Espectro Oi(Com FO)'
  
  FrameSI.loc[(FrameSI['STATUS FINAL ESPECTRO OI'] == 'Espectro Oi Configurado')&
              ((FrameSI['CHECK HW'] == 'HW Nao Suport OI') | (FrameSI['CHECK BB MIMO'] == 'Not Suport OI')),['STATUS FINAL ESPECTRO OI']] = 'Espectro Oi Configurado(Not Suported)'
  
  
  return FrameSI
  
def common_member(a, b):
    a_set = set(a)
    b_set = set(b)
    if len(a_set.intersection(b_set)) > 0:
        return(True) 
    return(False) 

