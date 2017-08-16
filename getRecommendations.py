# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:29:54 2017

@author: sameer kulkarni
"""
import pandas as pd
import numpy  as np
import logging

def getRecommendations(rhost, ruser, rpwd, rdb, dbtype, pid, start_date, date_range, calc_ari, calc_mpi, calc_pqm):
        
    logging.info("--- Start Generating Recommendations---")
    
    import getrcp
    import getData
    import mapdata
    import datetime
    import connectdb
    
    hid = int(pid)

#    start_date = str(datetime.datetime.today().strftime("%Y-%m-%d"))
#    date_range = 90
    
    logging.info("--- Calculating the End date ---")        
    edt = datetime.datetime.strptime(start_date,"%Y-%m-%d") + datetime.timedelta(days=date_range)
    #print(t)
    end_date = edt.strftime("%Y-%m-%d")
    logging.info("--- Recommendation Period is between %s and %s ---", start_date, end_date)        
    
    myquery = ("select competitordetailsid as propertydetailsid from property_competitor_mapping where propertydetailsid =:prid")
    
    try:
        logging.info("--- Fetching the Competition List ---"   )                
        df_comps =  getData.getData(myquery=myquery, pid=hid, rhost=rhost, ruser=ruser, rpwd=rpwd, rdb=rdb, dbtype = dbtype)
    except:
        logging.error("--- No Data Available. Check the data availability ---"   )
        
    df_rsh = pd.DataFrame({'propertydetailsid':[hid]})

    logging.info("--- Joining the Competition List with Hotel ---")
    df_rsh = df_rsh.append(df_comps, ignore_index=True)
    
    logging.info("--- Converting the Dataframe into a string ---"   )    
    
    x = df_rsh.to_string(header=False,
                      index=False,
                      index_names=False).split(',')
    valStr = [','.join(ele.split()) for ele in x]
    comp_ids = valStr[0]
    
    logging.info("--- List of all hotels is ready ---")        
    logging.debug(comp_ids)

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# Create the Dataframe that holds the capacity, remaining capacity and 
# CM Availability For each date into future. Generally for next 90days        
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    myquery = ("SELECT distinct cma.occupancydate as occupancydate, coalesce(sum(pod.capacity),0) as capacity, coalesce(sum(pod.vacantcount),0) as remcapacity, coalesce(sum(cma.remainingcapacity),0) as cmacapacity FROM channel_manager_availability_details as cma left join property_occupancy_details as pod using(propertyid,occupancydate) where cma.propertyid = :prid and cma.occupancydate between '" + start_date + "' and '" + end_date + "' group by cma.propertyid,cma.occupancydate order by cma.occupancydate")

    try:
        logging.info("--- Get number of rooms available to sell ---")
        df_avl =  getData.getData(myquery = myquery, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
    except:
        logging.error("--- No Data Available. Check the data availability ---"   )
    #print(df_avl)

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# Calling the RCP Algo to generate the recommendations
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logging.info("--- Generate the Recommendations based on RCP Algo ---")        
    try:
        df_rcp = getrcp.getrcp(df_raw = df_avl, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
        logging.debug(df_rcp)
    except:
        logging.error("--- No base recommendation is derived. Check the data availability ---"   )
        
    mapr=pd.DataFrame(df_rcp,columns = ['occupancydate','rcp'])
    logging.info("--- Applying Psychological Factor ---")         
    mapr['rcp'] = mapr.apply(lambda row: applyPsychologicalFactor(row['rcp']), axis=1)  
    #mapr.to_csv('G:/GDrive/Rate_Pilots/Test_Results/RCP_14APR17.csv', sep=',', encoding='utf-8')
    
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# Generate the Recommendations Market based Pricing 
# call the MPI, PQM and ARI Algorithms to generate the recommendations
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logging.info("--- Generate the Recommendations Market based Pricing ---")         
    
    myquery = ("select distinct rsd.rateshoppingpropertydetailsid, prop.propertydetailsid, rsd.checkindate as occupancydate, min(rsd.netrate) as htl_rate FROM rate_shopping_rates_by_day as rsd inner join (select id as rateshoppingpropertydetailsid, propertydetailsid from rate_shopping_property_details_mapping where propertydetailsid in (" + comp_ids +")) as prop using (rateshoppingpropertydetailsid) where rsd.checkindate between '" + start_date + "' and '" + end_date + "'  group by rsd.rateshoppingpropertydetailsid, prop.propertydetailsid, rsd.checkindate order by rsd.rateshoppingpropertydetailsid, rsd.checkindate;")
    
    try:
        logging.info("--- Fetching the Rate Shopping Data ---")            
        df_comprate = getData.getData(myquery = myquery, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
    except:
        logging.warn("--- Rate Shopping Data Not Available. Check the data availability ---"   )

    #Creating the pivot such that the RCP decisions can be merged
    
    df_rsdata = df_comprate.drop('rateshoppingpropertydetailsid',axis = 1)
    df_rsdata1= mapr.merge(df_rsdata, on='occupancydate', how='left')   
    
    logging.debug("--- Rate Shopping Data ---")
    logging.debug(df_rsdata1)
    
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# Generate the Recommendations using the MPI Algo
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logging.info("--- Generating MPI Recommendations ---")
    if calc_mpi == True:
        try:
            logging.info("--- Calculating Weights for MPI Recommendations ---")
            df_mpi = mapdata.mapdf(asc_dec = False, raw_df = df_rsdata1, pid = hid, rcp_df = mapr, algo = "MPI")
        except:
            logging.warn("--- Failed to Calculate Weights for MPI Recommendations ---")
        
        mpi_rate = mapdata.mpi_ari_pqm(df_mpi,mapr)
        logging.info("--- MPI Recommendations ---")
        mpi_rate['rtmpi'] = mpi_rate.apply(lambda row: mapdata.optRate(row['wavg']), axis=1)
        logging.info("--- Applying Psychological Factor on MPI Recommendations ---")         
        mpi_rate['rtmpi'] = mpi_rate.apply(lambda row: applyPsychologicalFactor(row['rtmpi']), axis=1)
        logging.debug("--- Printing MPI Recommendations ---")
        logging.debug(mpi_rate)
        #mpi_rate.to_csv('G:/GDrive/Rate_Pilots/Test_Results/MPI_14APR17.csv', sep=',', encoding='utf-8')
        logging.info("--- Generated MPI Recommendations ---")            
        
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# Generate the Recommendations using the ARI Algo
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logging.info("--- Generating ARI Recommendations ---")            
    if calc_ari == True:
        try:
            logging.info("--- Calculating Weights for ARI Recommendations ---")
            df_ari = mapdata.mapdf(asc_dec = True, raw_df = df_rsdata1, pid = hid, rcp_df = mapr, algo = "ARI")
        except:
            logging.warn("--- Failed to Calculate Weights for ARI Recommendations ---")
            
        ari_rate = mapdata.mpi_ari_pqm(df_ari,mapr)
        logging.info("--- ARI Recommendations ---")
        ari_rate['rtari'] = ari_rate.apply(lambda row: mapdata.optRate(row['wavg']), axis=1)
        logging.info("--- Applying Psychological Factor on ARI Recommendations ---")
        ari_rate['rtari'] = ari_rate.apply(lambda row: applyPsychologicalFactor(row['rtari']), axis=1)
        logging.debug("--- Printing ARI Recommendations ---")
        logging.debug(ari_rate)
        #ari_rate.to_csv('G:/GDrive/Rate_Pilots/Test_Results/ARI_14APR17.csv', sep=',', encoding='utf-8')
        logging.info("--- Generated ARI Recommendations ---")            
        
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# Generate the Recommendations using the PQM Algo
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# This might have to change based on the changed DB Schema
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logging.info("--- Generating PQM Recommendations ---")            
    if calc_pqm == True:
        # Write appropriate query to fetch the PQM weights
        
        myquery = ("select distinct propertydetailsid, avg(value::integer) as score from property_ota_health_mapping where propertydetailsid =:prid group by propertydetailsid")
    
        try:
            logging.info("--- Fetch the Quality data for the Client Hotel ---")
            qip =  getData.getData(myquery = myquery, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
        except:
            logging.warn("--- Fetch the Quality data for the Client Hotel ---")
                    
        myquery = ("select distinct propertydetailsid, avg(value::integer) as score from property_ota_health_mapping where propertydetailsid in (select competitordetailsid from property_competitor_mapping where propertydetailsid =:prid) group by propertydetailsid")
    
        try:
            logging.info("--- Fetch the Quality data for the Competition Hotels ---")    
            qic =  getData.getData(myquery = myquery, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
        except:
            logging.warn("--- Fetch the Quality data for the Competition Hotels ---")
            
        logging.info("--- Merge both the datasets ---")
        qi = qip.append(qic, ignore_index=True)
        logging.debug(qi)

        logging.info("--- Identify the client hotel score ---")
        clnt = qi.iloc[0,1]
        
        logging.info("--- Calculate the score distance of each hotels from the client hotel ---")
        qi['rnk'] = ((qi['score']-clnt)/clnt)+clnt
        qi = qi.drop('score',axis = 1)
        logging.debug(qi)
        
        try:
            logging.info("--- Calculating Weights for PQM Recommendations ---")            
            df_pqm = mapdata.mapdf(asc_dec=True, raw_df = df_rsdata1, pid = hid, rcp_df=mapr,  algo="PQM", pqmdf=qi)
        except:
            logging.warn("--- Failed to Calculate Weights for PQM Recommendations ---")
            
        pqm_rate = mapdata.mpi_ari_pqm(df_pqm,mapr)
        logging.info("--- PQM Recommendations ---")
        pqm_rate['rtpqm'] = pqm_rate.apply(lambda row: mapdata.optRate(row['wavg']), axis=1)
        logging.info("--- Applying Psychological Factor on PQM Recommendations ---")         
        pqm_rate['rtpqm'] = pqm_rate.apply(lambda row: applyPsychologicalFactor(row['rtpqm']), axis=1)  
        logging.debug("--- Printing PQM Recommendations ---")
        logging.debug(pqm_rate)
        #pqm_rate.to_csv('G:/GDrive/Rate_Pilots/Test_Results/PQM_14APR17.csv', sep=',', encoding='utf-8')
        logging.info("--- Generated PQM Recommendations ---")            

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
# Clubing all the 4 types of recommendations in order   
# to create a super set for either update or insert
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    
    logging.info("--- Clubing all Recommendations togather to right into the database ---") 
    
    df_bar = pd.DataFrame(mapr,columns=['occupancydate','rcp'])
    
    if calc_ari == True:
        df_bar = df_bar.merge(pd.DataFrame(ari_rate, columns=['occupancydate','rtari']), on='occupancydate', how='left')
    if calc_mpi == True:
        df_bar = df_bar.merge(pd.DataFrame(mpi_rate, columns=['occupancydate','rtmpi']), on='occupancydate', how='left')
    if calc_pqm == True:
        df_bar = df_bar.merge(pd.DataFrame(pqm_rate, columns=['occupancydate','rtpqm']), on='occupancydate', how='left')

#~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~
#          Fetch Roomtype details currently set to Hotel
#~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~
    
    myquery = ("select distinct propertydetailsid, roomtypemasterid from property_room_type_mapping where propertydetailsid =:prid and  name = 'Hotel'")
    try:
        logging.info("--- Fetching the Room Type ID for 'Hotel' room type ---")
        df_rmth =  getData.getData(myquery = myquery, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
    except:
        logging.error("--- Failed to fetch Room Type ID for 'Hotel' room type ---")
        
    df_bar['updatedate'] = pd.datetime.now().date()
    df_bar['propertydetailsid'] = hid
    
    df_bar = df_bar.merge(df_rmth, on='propertydetailsid', how='left')
    
    #df_bar.to_csv('G:/GDrive/Rate_Pilots/Test_Results/bar_actual_19APR17.csv', sep=',', encoding='utf-8')

    logging.info("--- Reseting the Column Headers ---")
    df_bar.columns=['checkindate', 'priceremainingcapacity', 'priceari', 'pricempi', 'pricepqm', 'updatedate','propertydetailsid','roomtypemasterid']    

    
    myquery = ("select propertydetailsid, checkindate, checkindate as occdate, priceari::integer as pari, pricempi::integer as pmpi, pricepqm::integer as ppqm, priceremainingcapacity as prcp, roomtypemasterid from recommended_price_all_by_day where propertydetailsid =:prid and checkindate between '" + start_date + "' and '" + end_date + "' ")
    
    try:
        logging.info("--- Fetching the existing BAR Recommendations for the given dates ---")
        df_bar1 =  getData.getData(myquery = myquery, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
    except:
        logging.warn("--- Failed to fetching BAR Recommendations. Check the database for data availability ---")
        
    logging.info("=== Existing BAR Recommendations - %s ===",len(df_bar1.index))

#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
#           Clubing all the 4 types of recommendations in order   
#           to create a super set for either update or insert
#~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    if len(df_bar1.index)>0:    
        df_bar2 = df_bar.merge(df_bar1, on=['propertydetailsid','checkindate','roomtypemasterid'], how='left')   
        
        df_bar2['new_rate']=np.where((df_bar2['priceari']!=df_bar2['pari']), 1, 
                            np.where((df_bar2['pricempi']!=df_bar2['pmpi']),1,
                            np.where((df_bar2['pricepqm']!=df_bar2['ppqm']),1,
                            np.where((df_bar2['priceremainingcapacity']!=df_bar2['prcp']),1,0)))) 
                            
        df_bar2['new_rate']=np.where((df_bar2['new_rate'] == 1),
                            np.where((df_bar2['checkindate'] == df_bar2['occdate']), 2, 
                                      df_bar2['new_rate']), df_bar2['new_rate']) 
                            
        df_bar2['priceremainingcapacity'] = df_bar2['priceremainingcapacity'].apply(float)
        df_bar2['priceari']               = df_bar2['priceari'].apply(float)
        df_bar2['pricempi']               = df_bar2['pricempi'].apply(float)
        df_bar2['pricepqm']               = df_bar2['pricepqm'].apply(float)
                
        #for inserting into the tables
        delcol    = ['pari','pmpi','ppqm','prcp','occdate']
        df_bar2   = df_bar2.drop(delcol,axis = 1)
        df_bar3   = df_bar2.query('(new_rate == 1)')    
        df_bar_in = df_bar3.drop('new_rate',axis = 1)
        logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    
        logging.info("~o~o~o~o~o~o~o~o~ Recommendations for insertion ~o~o~o~o~o~o~o~o~")    
        logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    
        
        #For Updating the tables
        df_bar4   = df_bar2.query('(new_rate == 2)')    
        df_bar_up = df_bar4.drop('new_rate',axis = 1)        
        logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    
        logging.info("~o~o~o~o~o~o~o~o~ Recommendations for update ~o~o~o~o~o~o~o~o~o~")    
        logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    
        
    else:
        df_bar_in                           = df_bar
        df_bar_in['priceremainingcapacity'] = df_bar_in['priceremainingcapacity'].apply(float)
        df_bar_in['priceari']               = df_bar_in['priceari'].apply(float)
        df_bar_in['pricempi']               = df_bar_in['pricempi'].apply(float)
        df_bar_in['pricepqm']               = df_bar_in['pricepqm'].apply(float)
        logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    
        logging.info("~o~o~o~o~o~o~o~o~ Recommendations for insertion ~o~o~o~o~o~o~o~o~")    
        logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    
        
    df_bar_final = df_bar_in
    df_bar_update = pd.DataFrame()
    
    if len(df_bar1.index)>0 and len(df_bar_up.index)>0:
         df_bar_update = df_bar_up
        
    #df_bar_final.to_csv('G:/GDrive/Rate_Pilots/Test_Results/bar_final_14APR17.csv', sep=',', encoding='utf-8')
    #df_bar_update.to_csv('G:/GDrive/Rate_Pilots/Test_Results/bar_update_14APR17.csv', sep=',', encoding='utf-8')

    #Connecting to the Database
    cnx = connectdb.conectdb(rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
    
    
    logging.info("--- insert into recommended_price_all_by_day ---")
    df_bar_final.to_sql(con = cnx, name = 'recommended_price_all_by_day', if_exists = 'append', index=False, index_label='id')
    
    logging.info("--- updating recommended_price_all_by_day ---")
    for row in df_bar_update.itertuples():    
        cnx.execute('update recommended_price_all_by_day '\
        'set priceari = %s, pricempi = %s, pricepqm= %s, '\
        'priceremainingcapacity = %s, updatedate = %s '\
        'where propertydetailsid =%s and roomtypemasterid = %s '\
        'and checkindate = %s', \
        [row.priceari, row.pricempi, row.pricepqm, row.priceremainingcapacity, row.updatedate, int(row.propertydetailsid), int(row.roomtypemasterid), row.checkindate])
    
	# insert or update into recommended_price_by_day_pace ---1
#=======================================================================    
    try:
        logging.info("--- insert new into recommended_price_by_day_pace ---1")
        df_bar_final.to_sql(con = cnx, name = 'recommended_price_by_day_pace', if_exists = 'append', index=False, index_label='id')
    except:
        logging.warn("--- data for the dates exisits in recommended_price_by_day_pace ---")
        logging.info("--- updating new recommended_price_by_day_pace ---1")
        for row in df_bar_final.itertuples():    
            cnx.execute('update recommended_price_by_day_pace '\
            'set priceari = %s, pricempi = %s, pricepqm= %s, '\
            'priceremainingcapacity = %s, updatedate = %s '\
            'where propertydetailsid =%s and roomtypemasterid = %s '\
            'and checkindate = %s ', \
            [row.priceari, row.pricempi, row.pricepqm, row.priceremainingcapacity, row.updatedate, int(row.propertydetailsid), int(row.roomtypemasterid), row.checkindate])

	# insert or update into recommended_price_by_day_pace ---2        
#=======================================================================
    try:
        logging.info("--- insert updates into recommended_price_by_day_pace ---2")
        df_bar_update.to_sql(con = cnx, name = 'recommended_price_by_day_pace', if_exists = 'append', index=False, index_label='id')
    except:
        logging.warn("--- data for the dates exisits in recommended_price_by_day_pace ---")
        try:            
            logging.info("--- update into recommended_price_by_day_pace ---2")
            for row in df_bar_update.itertuples():    
                cnx.execute('update recommended_price_by_day_pace '\
                'set priceari = %s, pricempi = %s, pricepqm= %s, '\
                'priceremainingcapacity = %s, updatedate = %s '\
                'where propertydetailsid =%s and roomtypemasterid = %s '\
                'and checkindate = %s ', \
                [row.priceari, row.pricempi, row.pricepqm, row.priceremainingcapacity, row.updatedate, int(row.propertydetailsid), int(row.roomtypemasterid), row.checkindate])
        except:
            logging.warn("--- data for the dates exisits in recommended_price_by_day_pace ---")
            logging.info("--- update into recommended_price_by_day_pace ---3")
            for row in df_bar_update.itertuples():    
                cnx.execute('update recommended_price_by_day_pace '\
                'set priceari = %s, pricempi = %s, pricepqm= %s, '\
                'priceremainingcapacity = %s, updatedate = %s '\
                'where propertydetailsid =%s and roomtypemasterid = %s '\
                'and checkindate = %s and updatedate = %s  ', \
                [row.priceari, row.pricempi, row.pricepqm, row.priceremainingcapacity, row.updatedate, int(row.propertydetailsid), int(row.roomtypemasterid), row.checkindate, row.updatedate,])
    
# insert or update into recommended_price_by_day ---1
#======================================================================

    cols = ['checkindate', 'finalrecommendedrate','overridetype', 'overwritten', 'systemdefinedrate', 'updateddate', 'propertydetailsid', 'roomtypemasterid']    
    
#======================================================================
#Fetch the existing recommendations for the future days
    myquery = ("select propertydetailsid, checkindate, roomtypemasterid, finalrecommendedrate, overwritten, systemdefinedrate, overridetype from recommended_price_by_day where propertydetailsid =:prid and checkindate between '" + start_date + "' and '" + end_date + "' order by propertydetailsid, roomtypemasterid, checkindate")
    
    try:
        logging.info("--- Fetch the existing recommendations for the future days ---")
        df_bbd =  getData.getData(myquery = myquery, pid = hid, rhost = rhost, ruser = ruser, rpwd = rpwd, rdb = rdb, dbtype = dbtype)
    except:
        logging.warn("--- Failed to fetch the existing recommendations for the future days ---")
        
    logging.debug("=== The Final Bar recommendations ===")
    logging.debug(df_bar_final)
    logging.debug("=== The existing Bar recommendations ===")
    logging.debug(df_bbd)
    logging.debug("=== The Bar recommendations for update ===")
    logging.debug(df_bar_update)    

    try:
        logging.info("--- Fetch the existing future overriders ---")
        df_bar_ind = getOverride(df_bar_final,df_bbd)
    except:
        logging.warn("--- No future overriders ---")
        
    #This line needs to be corrected in the new db schema
    df_bar_ind.rename(columns={'updatedate': 'updateddate'}, inplace=True)
    df_bar_ind = df_bar_ind.reindex(columns= cols)
    logging.info("=== The Bar recommendations after considering overrides for insertion ===")
    logging.debug(df_bar_ind)

    if len(df_bar_update.index)>0:
        logging.info("=== Setting data for Update ====")
        try:
            logging.info("--- Fetch the existing future overriders ---")
            df_bar_upd = getOverride(df_bar_update,df_bbd)
        except:
            logging.warn("--- No future overriders ---")
        
        #This line needs to be corrected in the new db schema
        df_bar_upd.rename(columns={'updatedate': 'updateddate'}, inplace=True)
        
        df_bar_upd = df_bar_upd.reindex(columns= cols)
        logging.info("=== The Bar recommendations after considering overrides for update ===")
        logging.debug(df_bar_upd)
    else:
        df_bar_upd = pd.DataFrame()
        
    logging.info("=== The Bar recommendations after considering overrides for update ===")

    try:
        logging.info("--- Inserte into recommended_price_by_day ---1")
        df_bar_ind.to_sql(con = cnx, name = 'recommended_price_by_day', if_exists = 'append', index=False, index_label='id')        
    except:
        logging.warn("--- Unable to inserte trying to update into recommended_price_by_day ---1")
        for row in df_bar_ind.itertuples():    
            cnx.execute('update recommended_price_by_day '\
            'set updateddate = %s, '\
            'finalrecommendedrate = %s, systemdefinedrate = %s '\
            'where propertydetailsid =%s and roomtypemasterid = %s '\
            'and checkindate = %s ', \
            [row.updateddate, row.finalrecommendedrate, row.systemdefinedrate, int(row.propertydetailsid), int(row.roomtypemasterid), row.checkindate])
        logging.info("--- Updated recommended_price_by_day ---1")

    try:
        logging.info("--- Inset the updates into recommended_price_by_day ---2 ")
        df_bar_upd.to_sql(con = cnx, name = 'recommended_price_by_day', if_exists = 'append', index=False, index_label='id')        
    except:
        logging.warn("--- Unable to inserte trying to update into recommended_price_by_day ---2")
        for row in df_bar_upd.itertuples():    
            cnx.execute('update recommended_price_by_day '\
            'set updateddate = %s, '\
            'finalrecommendedrate = %s, systemdefinedrate = %s '\
            'where propertydetailsid =%s and roomtypemasterid = %s '\
            'and checkindate = %s ', \
            [row.updateddate, row.finalrecommendedrate, row.systemdefinedrate, int(row.propertydetailsid), int(row.roomtypemasterid), row.checkindate])
        logging.info("--- Updated recommended_price_by_day ---2")
        
    cnx.close()
    
    #Change this to write into the the database
    #===============================================
    #df_bar.to_csv('G:/GDrive/Rate_Pilots/TRM_Test_Files/bar_dec1601.csv', sep=',', encoding='utf-8')
    #===============================================
    
    logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    
    logging.info("~o~o~o~o~o~o~o~o~o~ Recommendations Updated ~o~o~o~o~o~o~o~o~o~o~")    
    logging.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")    

    
    
#~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~
# Applying the psychological factor based 
# on the value of the recommendation
#~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~
    
def applyPsychologicalFactor(n):
    #print(int(str(n)[-3:]))
    #print(len(str(n)))
    logging.debug("--- Applying Psycological Factor ---")    
    if (115>int(str(n)[-3:])>100):
        if len(str(n))>4:            
            rval =(int((round(n,-1)-100)/100)*100)-1
            logging.debug("Len > 4, Greater than 100 and Smaller than 115 new Value - %s", rval)
        elif len(str(n))<4:
            rval =(int(round(n,-1)/10)*10)-1
            logging.debug("Len < 4, Greater than 100 and Smaller than 115 new Value - %s", rval)
        else:
            rval=(int((round(n,-1)-50)/100)*100)-1            
            logging.debug("Greater than 100 and Smaller than 115 new Value - %s", rval)
    elif (15>int(str(n)[-2:])>5):        
        rval=(int((round(n,-1))/50)*50)-1
        logging.debug("Greater than 5 and Smaller than 15 new Value - %s", rval)
    else:        
        rval =(int(round(n,-1)/10)*10)-1
        logging.debug("Outside the range new Value - %s", rval)
    logging.debug("--- Applied relevant Psycological Factor ---")            
    return rval
        
#applyPsychologicalFactor(10214)    

#~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~
# Identifying the Overrides on the BAR Recommendations
#~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~

def getOverride(df_input,df_dtls):     
    logging.info("--- Fetching the Existing Rate Overrides ---")
    if len(df_dtls.index)>0:
        df_bar_ui = df_input.merge(df_dtls, on=['propertydetailsid','checkindate','roomtypemasterid'], how='left')
        logging.info("=== Merge data for final push ===")
        logging.debug(df_bar_ui)
        df_bar_ui['systemdefinedrate'].fillna(0, inplace=True)
        df_bar_ui['finalrecommendedrate'].fillna(0, inplace=True)
        logging.info("=== STEP 2 ===")            
        #print(df_bar_ui)
        df_bar_ui['overwritten'] = np.where(df_bar_ui['overwritten']!=True, False, df_bar_ui['overwritten'])
        df_bar_ui['overridetype'] = np.where(df_bar_ui['overwritten']!=True, "None", df_bar_ui['overridetype'])
        df_bar_ui['systemdefinedrate'] =  df_bar_ui['pricepqm']
        logging.info("=== STEP 3 ===")
        #print(df_bar_ui)
        df_bar_ui['finalrecommendedrate'] = np.where((df_bar_ui['overwritten']==False), df_bar_ui['systemdefinedrate'], np.where((df_bar_ui['overridetype']=='priceari'), df_bar_ui['priceari'], np.where((df_bar_ui['overridetype']=='pricempi'), df_bar_ui['pricempi'], np.where((df_bar_ui['overridetype']=='pricepqm'), df_bar_ui['pricepqm'], np.where((df_bar_ui['overridetype']=='pricercp'), df_bar_ui['priceremainingcapacity'], df_bar_ui['finalrecommendedrate'])))))
        logging.info("=== STEP 4 ===")
        #print(df_bar_ui)
    else:
        df_bar_ui = df_input
        df_bar_ui['overwritten'] = False
        df_bar_ui['overridetype'] = "None"
        df_bar_ui['systemdefinedrate'] = df_bar_ui['pricepqm']
        df_bar_ui['finalrecommendedrate'] = df_bar_ui['pricepqm']    
    
    logging.debug(df_bar_ui)
    delcol=['priceremainingcapacity', 'priceari', 'pricempi', 'pricepqm']
    df_bar_ui = df_bar_ui.drop(delcol,axis = 1)
    logging.info("=== STEP 5 ===")
    #print(df_bar_ui)
    return df_bar_ui
    
