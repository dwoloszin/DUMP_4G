import pandas as pd
import numpy as np

def tratarShortNumber(df,siteNameCollumn):# 'Tecnologia','Station'
    df2 = df.copy()
    df2.loc[np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 11,'SHORT'] = df2[siteNameCollumn].str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),'SHORT'] = df2[siteNameCollumn].str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 12),'SHORT'] = df2[siteNameCollumn].str[4:-2]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),'SHORT'] = df2[siteNameCollumn].str[2:-2]
    df2.loc[(df2[siteNameCollumn].str[:1] == '3') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),'SHORT'] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:1] == '7') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),'SHORT'] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'SHORT'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),'SHORT'] = df2[siteNameCollumn].str[2:-2]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'SHORT'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[-3:] == '_SP') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'SHORT'] = df2[siteNameCollumn].str[:-3]
    #df2.loc[(df2[siteNameCollumn].str[:2] == '4S') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'SHORT'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'MM') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'SHORT'] = df2[siteNameCollumn].str[2:]
    #df2.loc[(np.array(list(map(len,df2[siteNameCollumn].values))) == 6),'SHORT'] = df2[siteNameCollumn]
    df2.loc[(df2[siteNameCollumn].str[2:3] == '-') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),'SHORT'] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[2:3] == '-') & (df2[siteNameCollumn].str[-2:-1] == '-'),'SHORT'] = df2[siteNameCollumn].str[3:-2]
    
    '''
    #Tratar SLS
    
    try:
      df2.loc[(df2['CELL'].str[:3] == '4D-') & (np.array(list(map(len,df2['CELL'].astype(str).values))) == 17),['SHORT']] = df2['CELL'].str[:12]
    except:
      print(Exception)

    try:
      df2.loc[(df2['CELL'].str[:3] == '4C-') & (np.array(list(map(len,df2['CELL'].astype(str).values))) == 17),['SHORT']] = '4D-' + df2['CELL'].str[3:12]
    except:
      print(Exception)

    '''

    return df2

def Short2Gto3G(df,Tecnologia,Station):
    df3 = df.copy()
    df3 = df3.reset_index(drop=True)
    df4G2G = df3.loc[(df3[Tecnologia] == '2G') | (df3[Tecnologia] == '4G')]
    KeepList_df4G2G = [Station,'Short1']
    locationBase_df4G2G = list(df4G2G.columns)
    DellListdf4G2G = list(set(locationBase_df4G2G)^set(KeepList_df4G2G))
    df4G2G = df4G2G.drop(DellListdf4G2G,1)
    df4G2G = df4G2G.drop_duplicates()
    df4G2G = df4G2G.reset_index(drop=True)

    indexI = 0
    for i in df3[Station]:
        indexJ = 0
        for j in df4G2G[Station]:
            if i == j:
                df3.at[indexI,'Short1'] = df4G2G.at[indexJ,'Short1']
            indexJ += 1
        indexI += 1    
    return df3
    

def tratarShortNumber2(df,siteNameCollumn,Tecnologia,Station):
    df2 = df.copy()
    df2.loc[np.array(list(map(len,df2[siteNameCollumn].values))) == 11,'Short1'] = df2[siteNameCollumn].str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),'Short1'] = df2[siteNameCollumn].str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 12),'Short1'] = df2[siteNameCollumn].str[4:-2]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),'Short1'] = df2[siteNameCollumn].str[2:-2]
    #df2.loc[(df2[siteNameCollumn].str[:1] == '3') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),'Short1'] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:1] == '3'),'Short1'] = df2[siteNameCollumn].str[3:]
    
    df2.loc[(df2[siteNameCollumn].str[:1] == '7') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),'Short1'] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'Short1'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'Short1'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[-3:] == '_SP') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'Short1'] = df2[siteNameCollumn].str[:-3]
    #df2.loc[(df2[siteNameCollumn].str[:2] == '4S') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'Short1'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'MM') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'Short1'] = df2[siteNameCollumn].str[2:]
    #df2.loc[(np.array(list(map(len,df2[siteNameCollumn].values))) == 6),'Short1'] = df2[siteNameCollumn]
    df2.loc[(df2[siteNameCollumn].str[2:3] == '-') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),'Short1'] = df2[siteNameCollumn].str[3:]
    #df3 = df2.loc[df2['Station'] == 'SPCAS_0055']
    #print(df3)
    df2.loc[(df2[Tecnologia] == '2G'),['Short1']] = df2[siteNameCollumn]
    #todo incluir shot 3G puro
    df2.loc[(df2[Tecnologia] == '3G'),['Short1']] = df2[siteNameCollumn]
    
    
    df2 = Short2Gto3G(df2,Tecnologia,Station)

    #puro 3G
    df2.loc[(df2[Tecnologia] == '3G') & (df2['Short1'].isna()),['Short1']] = df2[siteNameCollumn]
  
    df2.loc[(df2[siteNameCollumn].str[2:3] == '-') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),'Short1'] = df2[siteNameCollumn].str[3:]
     

    return df2
