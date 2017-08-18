# Import libraries
import csv
import os
import shutil
from DataExtraction_Thread import dbThread

blockParam = 'ZIP'
fmeWS = 'ZIP_MTDB_EXTRACT.fmw'
fmeWS2 = 'ZIP_MTDB_EXTRACT2.fmw'
folderLoc = os.getcwd() + r'\Standardizer\OutputData'


class dataExtract():
    def run(self, blocks4DB, oracleUser):
        """
        Core method to retrieve MAF data for Matching
        :param blocks4DB: block list as a csv doc from OracleStandardizer.standardize()
        :param oracleUser: OracleStandardizer object with username and password
        :return mafData: csv file of associated maf data from zip code list
        """
        userName = oracleUser.userName
        passWord = oracleUser.passWord

        print("Checking Oracle credentials to database...")
        zipCodeList1, zipCodeList2 = self.openCSV(blocks4DB)

        threads = []
        # Create new threads
        thread1 = dbThread(1, fmeWS, userName, passWord, zipCodeList1)
        thread2 = dbThread(2, fmeWS2, userName, passWord, zipCodeList2)

        # Start new Threads
        thread1.start()
        thread2.start()

        # Add threads to thread list
        threads.append(thread1)
        threads.append(thread2)

        # Wait for all threads to complete
        for t in threads:
            t.join()
        print "Exiting Main Thread - Data Extraction Complete"

        # join the outputs and remove the split results
        with open((folderLoc + r'\Data.txt'), 'wb') as wfd:
            for f in [(folderLoc + r'\1_copy.txt'), (folderLoc + r'\2_copy.txt')]:
                with open(f, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd, 1024*1024*10)

        os.remove((folderLoc + r'\1_copy.txt'))
        os.remove((folderLoc + r'\2_copy.txt'))
        return os.path.join(folderLoc + r'\Data.txt')

    ####################################################################################################################
    # Local Methods
    def openCSV(self, blocks4MAF):
        """
        Updated openCSV method that returns 2 Strings of block list; so it cuts original list in half
        :param csv file object:
        :return Block list as a giant string:
        """

        blockString = ''
        count = 0
        inCSV = blocks4MAF
        with open(inCSV) as csvfile:
            reader = csv.reader(csvfile, delimiter='|')
            for row in reader:
                for x in row:
                    if count == 0:
                        blockString = x
                        count += 1
                    else:
                        blockString = blockString + ',' + x

        list1, list2 = blockString[:len(blockString)/2], blockString[len(blockString)/2:]
        list2 = list2[1:]

        return list1, list2

        ####################################################################################################################

########################################################################################################################
if __name__ == '__main__':
    maf_data = dataExtract()
    maf_data.run()

__author__ = 'Matt Wilchek'