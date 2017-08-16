# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:25:34 2017

@author: sameer kulkarni
"""

"""
This is the wraper code to call the getRecommendations 
This wraper is called as RecommendationService
"""

import sys
import logging

def CalibrationService(pid):

    import configparser
    import getData
    import rcpalgo
    import datetime
    
    runDate = str(datetime.date.today().strftime("%d%b%Y"))
    logFileName = "CalibService_" + runDate + ".log"
    
    logging.basicConfig(level=logging.DEBUG,filename=logFileName)    
    logging.info("start calculating rate recommendations")    

    configParser = configparser.RawConfigParser()   
    configFilePath = r'.\setup.ini'
    
    try:
        logging.info("Reading setup file")
        configParser.read(configFilePath)
    except:
        logging.error('file not available')
    
    dbType   = configParser.get('db-config', 'dbtype')
    hostName = configParser.get('db-config', 'rhost')
    userID   = configParser.get('db-config', 'ruser')
    pWord    = configParser.get('db-config', 'rpwd')
    dbName   = configParser.get('db-config', 'rdb')
    
    myquery = ("SELECT capacity FROM property_details WHERE id =:prid")
    try:
        logging.info("Get the Hotel Details")
        hCapacity =  getData.getData(myquery=myquery, pid=pid, rhost = hostName, ruser = userID, rpwd = pWord, rdb = dbName, dbtype = dbType)
    except:
        logging.error("--- Unable to fetch Hotel Details ---")

    hCap = hCapacity.loc[0].values[0]
    logging.info("The Hotel Capacity - %s ",hCap)
    
    try:
        logging.info("--- Run Calibration ---")
        rcpalgo.rcpalgo(pid=pid, hCap=hCap, rhost = hostName, ruser = userID, rpwd = pWord, rdb = dbName, dbtype = dbType)
    except:
        logging.error("--- Unable to Run Calibration ---")

if __name__ == '__main__':    
    pid = sys.argv[1]        
    CalibrationService(pid)
