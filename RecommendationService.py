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


def RecommendationService(pid, start_date=None, date_range=None):

    import configparser
    import getRecommendations
    import datetime
    
    runDate = str(datetime.date.today().strftime("%d%b%Y"))
    logFileName = "RecomService_" + runDate + ".log"
    
    logging.basicConfig(level=logging.DEBUG,filename=logFileName)    
    logging.info("start calculating rate recommendations")    
    
    configParser = configparser.RawConfigParser()
    configFilePath = r'.\setup.ini'
    
    try:
        logging.info("Reading setup file")
        configParser.read(configFilePath)
    except:
        logging.error('file not available')
        
    logging.info("Got credentials for DB connection")    
    dbType   = configParser.get('db-config', 'dbtype')
    hostName = configParser.get('db-config', 'rhost')
    userID   = configParser.get('db-config', 'ruser')
    pWord    = configParser.get('db-config', 'rpwd')
    dbName   = configParser.get('db-config', 'rdb')

    #d = datetime.date.today()    
    #d.today()
    
    if start_date is None:
        start_date = str(datetime.date.today().strftime("%Y-%m-%d"))

    logging.info("Start date to generate - %s", start_date)
    
    if date_range is None:
        date_range = 90

    logging.info("Date range to generate recommendation - %s", date_range)
    
    try:
        logging.info("Generate the recommendations from %s for next %s ",start_date,date_range)
        getRecommendations.getRecommendations(rhost = hostName, ruser = userID, rpwd = pWord, rdb = dbName, dbtype = dbType, pid = pid, start_date = start_date, date_range = date_range, calc_ari = True, calc_mpi = True, calc_pqm = True)
    except:
        logging.error("--- Unable to generate the recommendations ---")
        
if __name__ == '__main__':            
    pid        = sys.argv[1]        
    start_date = sys.argv[2]
    date_range = sys.argv[3]
    RecommendationService(pid, start_date, date_range)
