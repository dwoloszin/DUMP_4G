import ENM




def processArchive():
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP ReportConfigB1NR.(b1ThresholdRsrp) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','drop2','drop3','B1NR'] # use drop to exclude columns that u don't need
  pathToSave = 'B1NR'
  ArchiveName = 'B1NR_1'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP ReportConfigB1NR.(b1ThresholdRsrp) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','drop2','drop3','B1NR'] # use drop to exclude columns that u don't need
  pathToSave = 'B1NR'
  ArchiveName = 'B1NR_2'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)



