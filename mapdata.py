# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 12:05:19 2017

@author: Sameer.Kulkarni
"""

import pandas as pd
import numpy  as np
from scipy.stats import poisson
import logging

def mapdf(asc_dec=True, raw_df=None, pid = None, rcp_df=None, algo="MPI", pqmdf=None):
    
    logging.info("Calculating the Recommendation using Algo - %s", algo)
    df_name = raw_df
    df_name = df_name.replace('Sold',np.nan, regex=True)
    df_name = df_name.replace('Closed',np.nan, regex=True)
    df_name = df_name.sort_values(['occupancydate','htl_rate'], ascending=[True, True])
    df_name['pp']=(df_name['htl_rate'] - df_name['rcp'])/df_name['rcp']        
    logging.debug(df_name)
    
    #===============
    logging.info("Calculating the ranks of each of the hotels")
    if algo == "PQM":
        df_name = df_name.merge(pqmdf, on='propertydetailsid', how='left')        
    else:
        df_name['rnk'] = df_name.groupby(['occupancydate'])['htl_rate'].rank(ascending=asc_dec)
    #===============
    
    logging.info("Calculating the weights for each of the hotels")    
    df_name['wgt']=np.where((df_name['rnk'] < df_name['pp']),(df_name['pp'] - df_name['rnk']),(df_name['rnk'] - df_name['pp']))

    #This code is introduced to set the Weight for the hotel with ZERO rate value to ZERO
    #This is essential to set the right Weight for the hotels
    df_name['wgt']=np.where((df_name['htl_rate'] == 0),0,df_name['wgt'])        
    
    df_name = df_name.fillna(0)
    logging.debug("Dataframe with the weights")
    logging.debug(df_name)
    
    logging.info("Set the Hotel Rate to RCP in place of the market rate")
    df_name['htl_rate'] = np.where(df_name['propertydetailsid'] == pid, df_name['rcp'], df_name['htl_rate']) 

    del df_name['propertydetailsid']

    logging.debug("Final Dataframe to calculate the Market Apropriate Price")
    logging.debug(df_name)
    
    return df_name

def mpi_ari_pqm(dfin,rcp_df):
    #import datetime
    logging.info("Calculating the weighted average for each of the hotels")
    grouped = dfin.groupby('occupancydate')
    def wavg(group):
        d = group['htl_rate']
        w = group['wgt']
        if w.sum()>0:
             wSum = w.sum() 
        else :
            wSum = 1
        return (d * w).sum() /wSum
    mpir = pd.DataFrame(grouped.apply(wavg))
    mpir.columns=['wavg']
    mpir['wavg'] = pd.to_numeric(mpir['wavg'])
    mpi_df=rcp_df.join(mpir, on='occupancydate', how='left')
    logging.debug("Dataframe with the weighted averages")
    logging.debug(mpi_df)
    return mpi_df

def xrange(x):
    logging.debug("Get range to calculate Optimal rate")
    return iter(range(int(x/2),int(x)))

def optRate(mean):
    mu = mean #round(mean)
    x1= 0
    xi = 0
    logging.debug("Getting Optimal Rate")
    #print(mu)
    #x2 = range(mean)
    logging.debug("Print Optimal Rate")
    for i in xrange(mu):
        #print (i)
        optr = i * (1- poisson.cdf(i,mean))
        #print (optr)
        logging.debug("OptRate %s Max Revenue %s", i, optr)
        if optr >= x1:
            x1 = optr
            xi = i
        else:
            break
    return xi 
