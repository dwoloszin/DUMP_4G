import ENM

def processArchive():
  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP eutrancellfdd.(primaryUpperLayerInd,additionalUpperLayerIndList) -t'
  columnsMask = ['NodeId','ENodeBFunctionId','EUtranCellFDDId','additionalUpperLayerIndList','primaryUpperLayerInd'] # use drop to exclude columns that u don't need
  pathToSave = 'Sinal5G'
  ArchiveName = 'Sinal5G_1'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName) 

  cmd = 'cmedit get SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP eutrancellfdd.(primaryUpperLayerInd,additionalUpperLayerIndList) -t'
  columnsMask = ['NodeId','ENodeBFunctionId','EUtranCellFDDId','additionalUpperLayerIndList','primaryUpperLayerInd'] # use drop to exclude columns that u don't need
  pathToSave = 'Sinal5G'
  ArchiveName = 'Sinal5G_2'
  ENM.processArchive(cmd,columnsMask,pathToSave,ArchiveName) 


