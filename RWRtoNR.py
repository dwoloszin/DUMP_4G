import ENM

def processArchive():
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP UeMeasControl.(endcCheckForRwrToNRDisabled,rwrToNRAllowed) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','drop2','endcCheckForRwrToNRDisabled','rwrToNRAllowed'] # use drop to exclude columns that u don't need
  pathToSave = 'RWRtoNR'
  ArchiveName = 'RWRtoNR_1'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName) 

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP UeMeasControl.(endcCheckForRwrToNRDisabled,rwrToNRAllowed) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','drop2','endcCheckForRwrToNRDisabled','rwrToNRAllowed'] # use drop to exclude columns that u don't need
  pathToSave = 'RWRtoNR'
  ArchiveName = 'RWRtoNR_2'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName) 