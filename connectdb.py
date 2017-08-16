# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 11:41:57 2016

@author: Sameer.Kulkarni
"""


def conectdb(rhost, ruser, rpwd, rdb, dbtype):
    import logging
    import sqlalchemy
    from sqlalchemy import create_engine
    
    logging.info("--- Connecting to Database ---")
    logging.debug("=== DB Type - %s ===", dbtype)
    if dbtype == 'PGS':        
        #this is the setup for PostgreSQL as the production is on PostgreSQL        
        import psycopg2
        constr = 'postgresql+psycopg2://' + ruser + ':' + rpwd + '@' + rhost + '/' + rdb        
    else:
        #this is the setup for MySQL as the research setup is on MySQL
        import mysql.connector
        constr = 'mysql+mysqlconnector://' + ruser + ':' + rpwd + '@' + rhost + '/' + rdb    

    logging.debug(constr)
    engine = create_engine(constr, echo=False)
    cnx = engine.connect()
    logging.info("--- Connected to Database ---")
    
    return cnx
    
