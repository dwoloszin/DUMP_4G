import ENM

def processArchive():
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP eutrancellfdd.(endcAllowedPlmnList) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','endcAllowedPlmnList'] # use drop to exclude columns that u don't need
  pathToSave = 'Ancora5G'
  ArchiveName = 'Ancora5G_1'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName) 

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP eutrancellfdd.(endcAllowedPlmnList) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','endcAllowedPlmnList'] # use drop to exclude columns that u don't need
  pathToSave = 'Ancora5G'
  ArchiveName = 'Ancora5G_2'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName) 
