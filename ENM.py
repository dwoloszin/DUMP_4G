import time
import enmscripting
import credentials
import pandas as pd
import os
import sys
result = {}

def processArchive(cmd,columnsList,pathToSave,ArchiveName):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  Login = True
  while Login:
    try:
      session = enmscripting.open('https://enmcvo.internal.timbrasil.com.br',credentials.login,credentials.passwd)
      command = cmd
      response = session.command().execute(command)
      listData = []
      getFistsEx = 0
      for table in response.get_output().groups():
        for row in table:
          if getFistsEx < 1:
            print(row)
          getFistsEx += 1
          listData.append(row)
      enmscripting.close(session)
      df = pd.DataFrame(listData, columns = columnsList)
      dropList = []
      for i in columnsList:
        if i.startswith('drop'):
          #print(i)
          dropList.append(i)
      df = df.drop(dropList, axis=1)
      df = df.drop_duplicates()
      df = df.reset_index(drop=True) # reset the index
      csv_path = os.path.join(script_dir, 'export/'+ pathToSave +'/' + ArchiveName + '.csv')
      df.to_csv(csv_path,index=False,header=True,sep=';')
      Login = False
    except:
      times = 10
      #stuff_in_string = "Login Failed, trying in {} seconds...".format(times)
      print ("Login Failed, trying again in {} seconds...".format(times))
      time.sleep(times)

    #return df


def processArchiveReturn(cmd,columnsList,pathToSave,ArchiveName):
  #script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  Login = True
  ok = False
  while Login:
    try:
      session = enmscripting.open('https://enmcvo.internal.timbrasil.com.br',credentials.login,credentials.passwd)
      command = cmd
      response = session.command().execute(command)
      listData = []
      getFistsEx = 0
      for table in response.get_output().groups():
        for row in table:
          if getFistsEx < 1: #check output format
            print(row,len(row))
            if len(columnsList) == 0:
              for cell in table[0]:
                columnsList.append(cell.labels()[0])
          getFistsEx += 1
          listData.append(row)
      enmscripting.close(session)
      df = pd.DataFrame(listData, columns = columnsList)
      dropList = []
      for i in columnsList:
        if i.startswith('drop'):
          dropList.append(i)
      df = df.drop(dropList, axis=1)
      df = df.drop_duplicates()
      df = df.reset_index(drop=True) # reset the index
      Login = False
      ok = True
    except:
      times = 10
      print ("Login Failed, trying again in {} seconds...".format(times))
      time.sleep(times)
    if ok:
      return df



































'''
def processArchiveReturn(cmd,columnsList,pathToSave,ArchiveName):
  #script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  Login = True
  ok = False
  while Login:
    try:
      session = enmscripting.open('https://enmcvo.internal.timbrasil.com.br',credentials.login,credentials.passwd)
      command = cmd
      response = session.command().execute(command)
      listData = []
      getFistsEx = 0
      for table in response.get_output().groups():
        for row in table:
          if getFistsEx < 1: #check output format
            print(row)
          getFistsEx += 1
          listData.append(row)
      enmscripting.close(session)
      df = pd.DataFrame(listData, columns = columnsList)
      dropList = []
      for i in columnsList:
        if i.startswith('drop'):
          dropList.append(i)
      df = df.drop(dropList, axis=1)
      df = df.drop_duplicates()
      df = df.reset_index(drop=True) # reset the index
      Login = False
      ok = True
    except:
      times = 10
      print ("Login Failed, trying again in {} seconds...".format(times))
      time.sleep(times)
    if ok:
      return df
'''      