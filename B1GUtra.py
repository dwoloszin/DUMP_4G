import ENM

def processArchive():
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP ReportConfigB1GUtra.(b1ThresholdRsrp) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','drop2','drop3','B1GUtra'] # use drop to exclude columns that u don't need
  pathToSave = 'B1GUtra'
  ArchiveName = 'B1GUtra_1'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP ReportConfigB1GUtra.(b1ThresholdRsrp) -t'
  columnsMask = ['NodeId','drop1','EUtranCellFDDId','drop2','drop3','B1GUtra'] # use drop to exclude columns that u don't need
  pathToSave = 'B1GUtra'
  ArchiveName = 'B1GUtra_2'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName)  