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




def processArchive():
  timeexport = time.strftime("%Y%m%d_")


  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  csv_path = os.path.join(script_dir, 'export/'+ 'DUMP4G_REDE' +'/' + timeexport+'DUMP4G_ERICSSON' + '.csv')


  DUMP4G_Col_list = ['VENDOR','ANF','CIDADE','LOCATION','SHORT','SITE','CELL','NGNIS_CELL','ADM STATE','OP STATE','FREQ CELL','FREQ SITE','EARFCNDL','BW DL','TAC','ENODEB ID','CELL ID','PCI','[S]','PLMN','MIMO','LAT','LONG','BW SITE','ANTENA_MODEL','AZIMUTH','ALTURA','MECHANICAL_TILT','DT_ATIV_MOBILE_SITE','endcAllowedPlmnList','primaryUpperLayerInd','additionalUpperLayerIndList2','NodeId','productName','serialNumber','productionDate','Baseband(QTD)','MIMO(QTD)','productName_ALL','CELL(Mimo)','endcCheckForRwrToNRDisabled','rwrToNRAllowed','B1GUtra','B1NR','CHECK HW','CHECK BB MIMO','TEM OI','Tem1800','STATUS FINAL ESPECTRO OI','ChannelCheck','SYNCSTATUS','transmissionMode']
  DUMP4G_path = '/export/DUMP4G'
  DUMP4G = ImportDF.ImportDF(DUMP4G_Col_list,DUMP4G_path)





  DUMP4G.to_csv(csv_path,index=False,header=True,sep=';')
  


