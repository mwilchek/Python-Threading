import os
import time
from DataExtraction import dataExtract
from OracleStandardizer import OracleStandardizer  # custom module that connects to Oracle db and standardizes data

inZipFile = os.getcwd() + r'\OutputData\zipCodeTest.txt'  # txt file with just a list of zip codes
global userName
global passWord

##############################
# Test 1: main program to test
##############################
print "***Testing Database Extraction***"
oracleUser = OracleStandardizer("userName", "passWord")
oracleUser = oracleUser.getLogin()

print "Extracting data from database based on file input..."
startTime = time.time()
dbData = dataExtract().run(inZipFile, oracleUser)
print os.path.abspath(dbData)

print 'Data Extraction done, took {0} seconds'.format(time.time() - startTime)
########################
__author__ = 'Matt Wilchek'