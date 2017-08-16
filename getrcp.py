# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 11:14:25 2017

@author: Sameer.Kulkarni
"""
import pandas as pd
import numpy  as np
import getData
import logging

def getrcp(df_raw, pid, rhost, ruser, rpwd, rdb, dbtype):
    
    myquery = ("SELECT seasonid, dow1, dow2, dow3, dow4, dow5, dow6, dow7 FROM slope_point_details WHERE propertyid =:prid and seasonid>0")
    try:
        logging.info("--- Fetching the Slope ---"   )                
        slope =  getData.getData(myquery=myquery, pid=pid, rhost=rhost, ruser=ruser, rpwd=rpwd, rdb=rdb, dbtype = dbtype)
        logging.debug(slope)
    except:
        logging.error("--- No Slope data available. Check the calibration ---"   )


    myquery = ("SELECT seasonid, dow1, dow2, dow3, dow4, dow5, dow6, dow7 FROM intercept_point_details WHERE propertyid =:prid and seasonid>0")
    try:
        logging.info("--- Fetching the Intercept ---"   )
        intercept =  getData.getData(myquery=myquery, pid=pid, rhost=rhost, ruser=ruser, rpwd=rpwd, rdb=rdb, dbtype = dbtype)
        logging.debug(intercept)
    except:
        logging.error("--- No Intercept data available. Check the calibration ---"   )


    myquery = ("select seasonnumber, concat('WkNum', fromweekcondition, fromweek, ' ',condition , ' ', 'WkNum', toweekcondition, toweek) as array_cson from season_details WHERE seasonnumber > 0 and propertyid =:prid")
    try:
        logging.info("--- Fetching the Seasonality Details ---"   )
        df_cson =  getData.getData(myquery=myquery, pid=pid, rhost=rhost, ruser=ruser, rpwd=rpwd, rdb=rdb, dbtype = dbtype)
        logging.debug(df_cson)
    except:
        logging.error("--- No Seasonality Details avilable. Please check the configuration ---"   )

    aSeas = df_cson['array_cson'].values

    logging.info("--- Identifying the Seasons for the dates in question ---"   )
    df_rcp=pd.DataFrame(df_raw,columns=['occupancydate', 'capacity', 'remcapacity', 'cmacapacity'])
    df_rcp['occupancydate'] = pd.to_datetime(df_rcp['occupancydate'])
    df_rcp['dow'] =  df_rcp['occupancydate'].dt.dayofweek+1
    df_rcp['dtyr'] = df_rcp['occupancydate'].dt.year
    df_rcp['dt1'] = pd.to_datetime(dict(year=df_rcp.dtyr,month=1,day=8))
    df_rcp['dt2'] = pd.to_datetime(dict(year=df_rcp.dtyr,month=1,day=6))
    df_rcp['dtwk'] =  df_rcp['dt2'].dt.dayofweek + 2                          
    df_rcp['dt3'] = df_rcp['dt1'] - pd.TimedeltaIndex(df_rcp['dtwk'], unit='D')
    df_rcp['dtdf'] =(df_rcp['dt3']-df_rcp['occupancydate']).apply(lambda x: x/np.timedelta64(1,'D'))    
    df_rcp.loc[(df_rcp['dtdf'] <=0.0), 'WkNum'] = df_rcp['occupancydate'].apply(lambda x: x.strftime('%W'))
    df_rcp.loc[(df_rcp['dtdf'] >0.0), 'WkNum'] = 52
    df_rcp['WkNum'] = pd.to_numeric(df_rcp['WkNum'])
	
    logging.debug("Printing the seasonality identified data")
    logging.debug(df_rcp)
    
    df_rcp_season = pd.DataFrame()

    for i in range(len(aSeas)):
        season = aSeas[i]
        cson1 = pd.DataFrame()
		#Fetching data by Season		
        cson1 = df_rcp.query(season)
        if len(cson1.index)>0:    
            logging.info(" %s - %s ",season,i)
            cson2 = pd.DataFrame(cson1)
            cson2['seasonid'] = i+1
            logging.debug(cson2)
            df_rcp_season = df_rcp_season.append(cson2,ignore_index=True)

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# This entire code will be rewritten based on the changes in the Algorithm
# The pricing will be based on pickup and number of rooms allocated by the
# hotel to sell on "On Line Distribution Platforms"
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
	
    df_rcp_season['yval'] = df_rcp_season.apply(lambda row:min([row['capacity'], max([row['remcapacity'], row['cmacapacity']])]), axis=1)

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~


	#df_rcp['yval'] = np.where((df_rcp['yval']==0),df_rcp['Capacity'],df_rcp['yval'])
    logging.info("--- Fetching the Slop Details and merging with the data ---"   )
    df_rcp_season = df_rcp_season.merge(slope,on='seasonid',how='left')
    df_rcp_season.loc[df_rcp_season['dow']==1,'slope'] = df_rcp_season['dow1']
    df_rcp_season.loc[df_rcp_season['dow']==2,'slope'] = df_rcp_season['dow2']
    df_rcp_season.loc[df_rcp_season['dow']==3,'slope'] = df_rcp_season['dow3']
    df_rcp_season.loc[df_rcp_season['dow']==4,'slope'] = df_rcp_season['dow4']
    df_rcp_season.loc[df_rcp_season['dow']==5,'slope'] = df_rcp_season['dow5']
    df_rcp_season.loc[df_rcp_season['dow']==6,'slope'] = df_rcp_season['dow6']
    df_rcp_season.loc[df_rcp_season['dow']==7,'slope'] = df_rcp_season['dow7']

    delcols = ['dtyr','dt1', 'dt2', 'dt3', 'dtwk','dtdf','dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7']
    df_rcp_season = df_rcp_season.drop(delcols,axis = 1)
    
    logging.info("--- Fetching the Intercept Details and merging with the data ---"   )
    df_rcp_season = df_rcp_season.merge(intercept,on='seasonid',how='left')
    df_rcp_season.loc[df_rcp_season['dow']==1,'intercept'] = df_rcp_season['dow1']
    df_rcp_season.loc[df_rcp_season['dow']==2,'intercept'] = df_rcp_season['dow2']
    df_rcp_season.loc[df_rcp_season['dow']==3,'intercept'] = df_rcp_season['dow3']
    df_rcp_season.loc[df_rcp_season['dow']==4,'intercept'] = df_rcp_season['dow4']
    df_rcp_season.loc[df_rcp_season['dow']==5,'intercept'] = df_rcp_season['dow5']
    df_rcp_season.loc[df_rcp_season['dow']==6,'intercept'] = df_rcp_season['dow6']
    df_rcp_season.loc[df_rcp_season['dow']==7,'intercept'] = df_rcp_season['dow7']
    
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# This Formulation will be changed according to new Enhanced Algorithm    
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    logging.info("--- Calculating the Rate Recommendation based on RCP Algo ---"   )
    df_rcp_season['rcp']= (df_rcp_season['yval'] - df_rcp_season['intercept'])/df_rcp_season['slope']

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    delcols = ['dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7','dow','WkNum','seasonid','yval','slope','intercept']
    df_rcp_season = df_rcp_season.drop(delcols,axis = 1)

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    
    #print(df_rcp)
    return df_rcp_season


