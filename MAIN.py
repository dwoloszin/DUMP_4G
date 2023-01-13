import Ancora5G
import Antenna
import Type
import Plmn
import MobileSite
import SI
import eNBId
import ERICSSON
import SiteID
import DUMP
import DUMP_Export
import BB
import DU
import DU2
import Auxx
import Auxx2
import RadioSector
import RadioSector2
import timeit
import Sinal5G
import RWRtoNR
import B1GUtra
import B1NR
import HW
import ENM

print ('\nprocessing... ')
inicio = timeit.default_timer()


MobileSite.processArchive()
SI.processArchive()
B1NR.processArchive()
B1GUtra.processArchive()
RWRtoNR.processArchive()
Sinal5G.processArchive()
Ancora5G.processArchive()
Antenna.processArchive()
Type.processArchive()
Plmn.processArchive()
eNBId.processArchive()
ERICSSON.processArchive()
BB.processArchive()
DU.processArchive()
DU2.processArchive()
RadioSector.processArchive()
RadioSector2.processArchive()

HW.processArchive()



DUMP.processArchive()


DUMP_Export.processArchive()

#about 17min to update all


fim = timeit.default_timer()
print ('duracao: %.2f' % ((fim - inicio)/60) + ' min') 
























