# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 12:40:30 2016

@author: Sameer.Kulkarni
"""

import sys
import pandas as pd
import numpy  as np
import logging

def rcpalgo(pid, hCap, rhost, ruser, rpwd, rdb, dbtype):
    
    #print ("--- Start ---")
    
    import getData
    import rcpfunction
    import connectdb
    
    if dbtype == 'PGS':
        myquery = ("SELECT arrivaldate as CheckInDate, numrooms as Nights, lengthofstay as LOS, totalamount as RoomRevenue FROM transactional_details WHERE propertyid =:prid" )
    else:
        myquery = ("SELECT ArrivalDate as CheckInDate, RoomSold as Nights, LOS, RoomRevenue FROM channelproduction WHERE HotelID =:prid")
        
    try:
        logging.info("--- Fetching the Booking data ---"   )
        df =  getData.getData(myquery=myquery, pid=pid, rhost=rhost, ruser=ruser, rpwd=rpwd, rdb=rdb, dbtype = dbtype)
    except:
        logging.error("--- No Booking data available. Check the database ---"   )
    
    qcson = ("select concat('WkNum', fromweekcondition, fromweek, ' ',condition , ' ', 'WkNum', toweekcondition, toweek) as array_cson from season_details WHERE propertyid =:prid" )

    try:
        logging.info("--- Fetching the Seasonal Definition ---"   )            
        df_cson =  getData.getData(myquery=qcson, pid=pid, rhost=rhost, ruser=ruser, rpwd=rpwd, rdb=rdb, dbtype = dbtype)
    except:
        logging.error("--- Seasons are not defined ---"   )

    qrdow=("select dayofweek as dow, daytype from property_day_of_week_definition WHERE propertydetailsid =:prid" )

    try:
        logging.info("--- Fetching the Day of Week Definition ---"   )            
        df_dow =  getData.getData(myquery=qrdow, pid=pid, rhost=rhost, ruser=ruser, rpwd=rpwd, rdb=rdb, dbtype = dbtype)    
    except:
        logging.error("--- Days of Week are not defined ---"   )
    
    dfwd= df_dow.query('daytype=="WD"')
    dfwe= df_dow.query('daytype=="WE"')
    
    liwd = dfwd['dow'].values
    liwe = dfwe['dow'].values
        
    aDOW = ['All','WD','WE','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    
    aSeas = df_cson['array_cson'].values
        
    logging.info("--- Calculate Week Number, Day of Week and ADR ---")    
    
    df.columns=['CheckInDate', 'Nights', 'LOS', 'RoomRevenue']
    
    df['CheckInDate'] = pd.to_datetime(df['CheckInDate'])
    df['dow'] =  df['CheckInDate'].dt.weekday_name
    df['dtyr'] = df['CheckInDate'].dt.year
    df['dt1'] = pd.to_datetime(dict(year=df.dtyr,month=1,day=8))
    df['dt2'] = pd.to_datetime(dict(year=df.dtyr,month=1,day=6))
    df['dtwk'] =  df['dt2'].dt.dayofweek + 2                          
    df['dt3'] = df['dt1'] - pd.TimedeltaIndex(df['dtwk'], unit='D')
    df['dtdf'] =(df['dt3']-df['CheckInDate']).apply(lambda x: x/np.timedelta64(1,'D'))
    df.loc[(df['dtdf'] >0.0), 'WkNum'] = 52
    df.loc[(df['dtdf'] <=0.0), 'WkNum'] = df['CheckInDate'].apply(lambda x: x.strftime('%W'))
    df['WkNum'] = pd.to_numeric(df['WkNum'])
    df['WkNum']  = np.where((df['WkNum']==53),0,df['WkNum'])
    df['ADR'] =  df['RoomRevenue']/(df['Nights']*df['LOS'])
        
    delcols = ['dtyr','dt1', 'dt2', 'dt3', 'dtwk','dtdf']
    df = df.drop(delcols,axis = 1)
    logging.info("--- Removing following columns %s ---",delcols)
    
    #for k in range(len(delcols)):
    #    del df[delcols[k]]
    
    df1= df.query('ADR > 0' or 'ADR != NaN')
    
    #Start the loop here
    #----------------------
    
    dfSlope = pd.DataFrame()
    dfIntercept = pd.DataFrame()
    
    for i in range(len(aSeas)):
        season = aSeas[i]   #assigning the seasons
        mSlope=[]           #creating a blank array to hold the slope values
        cIntercept = []     #creating a blank array to hold the intercept values
        #print(season)
        for j in range(len(aDOW)):
            DOW = aDOW[j]   #assigning day of weeks
            cson1= df1.query(season) #Fetching data by Season
            logging.info("%s - %s - %s", season, i, DOW)
            logging.info("--- Calculate Bounds for Outlier Removal ---")
            
            lbnd,ubnd = rcpfunction.LUBound(cson1['ADR'])
            
    #        print(lbnd, ubnd)
            
            logging.info("--- Calculated the Lower and Upper Bounds ---")
            
            adrDF1 = cson1[(cson1['ADR'] >= lbnd) & (cson1['ADR'] <= ubnd)]
            #disc = adrDF1.describe()
            
            logging.info("--- Calculate Percentiles and Mu Sigma ---")
            
            mu, sigma, p95, p05 = rcpfunction.perTile(adrDF1, DOW,liwd,liwe)
                        
            logging.info("--- Removing outliers using percentile values ---")        
            adrDF2 = cson1[(cson1['ADR'] >= p05) & (cson1['ADR'] <= p95)]
            
            logging.info("--- Calculating z Values ---")            
            zRate, zSale, rowCnt = rcpfunction.zValues(adrDF2, DOW, mu, sigma)
    
            logging.info("Setting the minimum number of observations." 
                        +"If there are less than 30 then not processing the data ahead")
            if rowCnt >= 30 :
                logging.info("There is enough data and proceeding with calibration")
                vrDF = pd.DataFrame({'rates': zRate, 'volume': zSale})
                                
                logging.info("Sorting the ADR data or z scores in descending order")
                vrDF1 = vrDF.sort_values(by = 'rates', axis=0, ascending =0)
                
                logging.info("Calculating Cumulative Solds")
                vrDF1['cum_sum'] = vrDF1.volume.cumsum() 
                
                logging.info("Calculating y Value")
                vrDF1['y_val'] = round(vrDF1.cum_sum/vrDF1.volume.sum()*hCap,0) 
                
                logging.info("Resorting in ascending order")
                vrDF1 = vrDF1.sort_values(by = 'rates', axis=0, ascending =1)
                
                logging.debug(vrDF1)
                logging.info("Remove the rows with Zero volume or Sold")
                mcDF = vrDF1.query('volume>0')
                
                logging.info("Calculating the Slope and Intercept")
                mval, cval = rcpfunction.mcVal(mcDF.rates,mcDF.y_val)
                
                logging.info("Calculated the Slope (m value)")
                logging.debug(mval)
                logging.info("Calculated the Intercept (c value)")
                logging.debug(cval)
            else:
                mval, cval = (0,0)
                logging.info("--- Insufficient Data ---")
                
            mSlope.append(mval)
            cIntercept.append(cval)
            
            logging.debug(mSlope)
            logging.debug(cIntercept)
            
        dfms = pd.DataFrame(mSlope,columns=['mValue'])
        dfci = pd.DataFrame(cIntercept,columns=['cValue'])
        del(mSlope,cIntercept)
        
        dfSlope = dfSlope.append(dfms.transpose(),ignore_index=True)
        dfIntercept = dfIntercept.append(dfci.transpose(),ignore_index=True)
        
        logging.debug(dfSlope)        
        del(dfms,dfci)     
        
    logging.info("--- Start the calculation of the Slope the Intercept ---")

    dfSlope['PID'] = pid
    dfIntercept['PID'] = pid    

    dfSlope['Season'] = dfSlope.index
    dfIntercept['Season'] = dfIntercept.index
    
    logging.debug(dfIntercept)
    dfSlope.columns=['All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'PID', 'Season']

    dfIntercept.columns=['All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'PID', 'Season']
    
    cols = ['PID', 'Season','All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    dfSlope = dfSlope.reindex(columns= cols)    
    dfIntercept = dfIntercept.reindex(columns= cols)        
                    
    logging.debug(dfSlope)
    dfSlope = setDF(dfSlope,liwd)
    
    logging.info("--- Calculated the Slope ---")
    logging.debug(dfSlope)    
    
    logging.debug(dfIntercept)
    dfIntercept = setDF(dfIntercept, liwd)
        
    logging.info("--- Calculated the Intercept ---")    
    logging.debug(dfIntercept)
    
    dfSlope.columns=['propertyid', 'seasonid', 'allval', 'weekdays', 'weekends', 'dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7']
    dfIntercept.columns=['propertyid', 'seasonid', 'allval', 'weekdays', 'weekends', 'dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7']

    cnx = connectdb.conectdb(rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)    
    
    try:
        logging.info("--- Deleting the old SLOPE calibration values ---")
        cnx.execute('Delete from slope_point_details where propertyid =%s', pid)    
    except:
        logging.error("--- Unable to delete the SLOPE calibration values ---")
    
    try:
        logging.info("--- Deleting the old INTERCEPT calibration values ---")
        cnx.execute('Delete from intercept_point_details where propertyid =%s', pid)
    except:
        logging.error("--- Unable to delete the INTERCEPT calibration values ---")
    
    try:
        logging.info("--- Inserting the new SLOPE calibration values ---")
        dfSlope.to_sql(con = cnx, name = 'slope_point_details', if_exists = 'append', index=False, index_label='id')
    except:
        logging.error("--- Unable to add the SLOPE calibration values ---")
    
    try:
        logging.info("--- Inserting the new INTERCEPT calibration values ---")
        dfIntercept.to_sql(con = cnx, name = 'intercept_point_details', if_exists = 'append', index=False, index_label='id')
    except:
        logging.error("--- Unable to add the INTERCEPT calibration values ---")
    
    cnx.close()        
    
    logging.debug(liwd)
    logging.debug(liwe)

    logging.debug(aSeas)
    logging.info("--- DONE ---")    
    logging.info("==============================================================")


def setDF(df_input, wkdList):    
    c0al = df_input.iloc[0,2]
    c0wd = np.where(df_input.iloc[0,3]==0,c0al,df_input.iloc[0,3])
    c0we = np.where(df_input.iloc[0,4]==0,c0al,df_input.iloc[0,4])
    
    logging.debug(c0al)
    logging.debug(c0wd)
    logging.debug(c0we)
    
    df_input['WD']= np.where(df_input['WD']==0.0,
                    np.where(df_input['All']==0.0,c0wd,df_input['All']),df_input['WD'])                        
    df_input['WE']= np.where(df_input['WE']==0.0,
                    np.where(df_input['All']==0.0,c0we,df_input['All']),df_input['WE'])
    df_input['All']=np.where(df_input['All']==0.0,c0al,df_input['All'])
    
    for row in df_input.itertuples():
        if row.Monday==0:
            isthere = 'Monday' in wkdList
            if isthere==True:
                rdow=row.WD
            else:
                rdow=row.WE                    
            df_input.set_value(row.Index, 'Monday',rdow)
        if row.Tuesday==0:
            isthere = 'Tuesday' in wkdList
            if isthere==True:
                rdow=row.WD
            else:
                rdow=row.WE
            df_input.set_value(row.Index, 'Tuesday',rdow)
        if row.Wednesday==0:
            isthere = 'Wednesday' in wkdList
            if isthere==True:
                rdow=row.WD
            else:
                rdow=row.WE
            df_input.set_value(row.Index, 'Wednesday',rdow)
        if row.Thursday==0:
            isthere = 'Thursday' in wkdList
            if isthere==True:
                rdow=row.WD
            else:
                rdow=row.WE
            df_input.set_value(row.Index, 'Thursday',rdow)
        if row.Friday==0:
            isthere = 'Friday' in wkdList
            if isthere==True:
                rdow=row.WD
            else:
                rdow=row.WE
            df_input.set_value(row.Index, 'Friday',rdow)
        if row.Saturday==0:
            isthere = 'Saturday' in wkdList
            if isthere==True:
                rdow=row.WD
            else:
                rdow=row.WE
            df_input.set_value(row.Index, 'Saturday',rdow)
        if row.Sunday==0:
            isthere = 'Sunday' in wkdList
            if isthere==True:
                rdow=row.WD
            else:
                rdow=row.WE
            df_input.set_value(row.Index, 'Sunday',rdow)
            
    return df_input                  
        
if __name__ == '__main__':    
    #pid, hCap, rhost, ruser, rpwd, rdb, dbtype
    if sys.argv[7] == 'PGS':
        pid= int(sys.argv[1])
    else:
        pid= sys.argv[1]
        
    hCap= int(sys.argv[2])
    rhost= sys.argv[3]
    ruser= sys.argv[4]
    rpwd= sys.argv[5]
    rdb= sys.argv[6]
    dbtype= sys.argv[7]
    
    rcpalgo(pid, hCap, rhost, ruser, rpwd, rdb, dbtype)