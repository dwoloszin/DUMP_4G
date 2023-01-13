import ENM
def processArchive():
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP enodebfunction.(enbid) -t'
  columnsMask = ['NodeId','drop1','ENODEB ID'] # use drop to exclude columns that u don't need
  pathToSave = 'eNBId'
  ArchiveName = 'eNBId_1'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP enodebfunction.(enbid) -t'
  columnsMask = ['NodeId','drop1','ENODEB ID'] # use drop to exclude columns that u don't need
  pathToSave = 'eNBId'
  ArchiveName = 'eNBId_2'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)
