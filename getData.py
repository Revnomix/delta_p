# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 11:41:59 2016

@author: Sameer.Kulkarni
"""

def getData(myquery, pid, rhost, ruser, rpwd, rdb, dbtype):
    import sqlalchemy as sa
    import pandas as pd
    import connectdb
    import logging
    
    logging.info("--- Connecting to the Database ---")    
    cnx = connectdb.conectdb(rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)

    logging.info("--- Get all raw data ---")    
        
    #print(myquery)
    df = pd.read_sql(sa.text(myquery), cnx, params={'prid': pid})
    cnx.close()
    logging.info("--- Got all raw data ---")    
    
    return df
