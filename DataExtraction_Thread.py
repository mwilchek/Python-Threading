import os
import threading
import subprocess

env = 'Database_Name'
folderLoc = os.getcwd() + r'\Standardizer\OutputData'

class dbThread(threading.Thread):
    def __init__(self, threadID, fmeWS, userName, passWord, zipCodeString):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.fmeWS = fmeWS
        self.userName = userName
        self.passWord = passWord
        self.stringlist = zipCodeString

    def runZIPfme2(self, fileName, fmeWS, userName, passWord, zipCodeString):
        """
        Call FME ZIP workspace; will create a csv of data extracted from the MAF database
        different workspaces for different blocking types
        :param userName: username for Oracle database connection
        :param passWord: password for Oracle database connection
        :param blockString: String made from local openCSV method
        :return mafData: associated MAF data as a local csv in a temp directory
        """

        fmeWS = fmeWS
        fmeWSFolder = os.getcwd() + r'\FME_Workspaces'
        outFileName = fileName

        print 'Executing FME ETL Process...'
        fmeCommand = 'FME ' + fmeWS + \
                     ' --DB_CONNECT ' + env + \
                     ' --USERNAME ' + userName + \
                     ' --PASSWORD ' + passWord + \
                     ' --destData ' + folderLoc + \
                     ' --FileName ' + outFileName + \
                     ' --ZIP_List ' + zipCodeString

        subprocess.call(fmeCommand, shell=False, cwd=fmeWSFolder)
        dbData = os.path.join(folderLoc, outFileName)

        return dbData

    def run(self):
        print "Starting Thread: " + str(self.threadID)
        self.runZIPfme2((str(self.threadID) + "_copy"), self.fmeWS, self.userName, self.passWord, self.zipCodeString)
        print "Exiting Thread: " + str(self.threadID)

__author__ = 'Matt Wilchek'
