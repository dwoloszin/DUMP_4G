import ENM

def processArchive():
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP enodebfunction.(Enodebplmnid) -t'
  columnsMask = ['NodeId','drop1','eNodeBPlmnId'] # use drop to exclude columns that u don't need
  pathToSave = 'PLMN'
  ArchiveName = 'PLMN2_1'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)


  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP enodebfunction.(Enodebplmnid) -t'
  columnsMask = ['NodeId','drop1','eNodeBPlmnId'] # use drop to exclude columns that u don't need
  pathToSave = 'PLMN'
  ArchiveName = 'PLMN2_2'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)

#Baixar arquivo como csv, alterAR CODIGO