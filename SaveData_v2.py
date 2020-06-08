# -*- coding: utf-8 -*-
"""
Created on Mon May 27 19:26:13 2019

@author: karl_
"""

import json
from oandapyV20 import API    # the client
from oandapyV20.exceptions import V20Error
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.trades as trades
import oandapyV20.endpoints.pricing as pricing
import oandapyV20.endpoints.instruments as instruments

import oandapyV20.endpoints.orders as orders
import oandapyV20

from oandapyV20.contrib.requests import MarketOrderRequest
from oandapyV20.contrib.requests import TakeProfitDetails, StopLossDetails

import numpy as np
import datetime as datetime
import pandas as pd
from pytz import timezone
import pytz

access_token = "d7e5ee22a1677217c07170395625f39b-714e37f3af0d68bf84f0d9d476c35048"
accountID = "101-001-11330846-001"
client = API(access_token=access_token)

api = oandapyV20.API(access_token=access_token)


start1 = [datetime.datetime(2008,1,1),datetime.datetime(2008,7,1),datetime.datetime(2009,1,1),datetime.datetime(2009,7,1),datetime.datetime(2010,1,1),datetime.datetime(2010,7,1),datetime.datetime(2011,1,1),datetime.datetime(2011,7,1),datetime.datetime(2012,1,1),datetime.datetime(2012,7,1),datetime.datetime(2013,1,1),datetime.datetime(2013,7,1),datetime.datetime(2014,1,1),datetime.datetime(2014,7,1),datetime.datetime(2015,1,1),datetime.datetime(2015,7,1),datetime.datetime(2016,1,1),datetime.datetime(2016,7,1),datetime.datetime(2017,1,1),datetime.datetime(2017,7,1),datetime.datetime(2018,1,1),datetime.datetime(2018,7,1),datetime.datetime(2019,1,1),datetime.datetime(2019,7,1),datetime.datetime(2020,1,1)]
end1 = [datetime.datetime(2008,7,1),datetime.datetime(2009,1,1),datetime.datetime(2009,7,1),datetime.datetime(2010,1,1),datetime.datetime(2010,7,1),datetime.datetime(2011,1,1),datetime.datetime(2011,7,1),datetime.datetime(2012,1,1),datetime.datetime(2012,7,1),datetime.datetime(2013,1,1),datetime.datetime(2013,7,1),datetime.datetime(2014,1,1),datetime.datetime(2014,7,1),datetime.datetime(2015,1,1),datetime.datetime(2015,7,1),datetime.datetime(2016,1,1),datetime.datetime(2016,7,1),datetime.datetime(2017,1,1),datetime.datetime(2017,7,1),datetime.datetime(2018,1,1),datetime.datetime(2018,7,1),datetime.datetime(2019,1,1),datetime.datetime(2019,7,1),datetime.datetime(2020,1,1),datetime.datetime(2020,6,1)]

step = datetime.timedelta(days=1)

currency_pair = "USD_SEK"

df = pd.DataFrame(columns = ["Time","Open","High","Low","Close"])

##start1,end1 = [datetime.datetime(2019,9,27)],[datetime.datetime(2019,9,28)]

for j in list(zip(start1,end1)):
    
    daterange = []
    start = j[0]
    end = j[1]
    
    while start <= end:
        print("ok")
        daterange.append(start.strftime('%Y-%m-%d'))
        start =start +step
    
    df1 = pd.DataFrame(columns = ["Time","Open","High","Low","Close"])
    position = 0
    
    for i in range(len(daterange)-1):
        print("Start {}".format(daterange[i]))
    
        params = {
            "granularity": "D", 
            "from":daterange[i],
            "to":daterange[i+1]
              }
    
        r= instruments.InstrumentsCandles(instrument=currency_pair, params = params)
        rv = api.request(r)
        response = r.response

        counts = len(response["candles"])
        print(counts)
    
        for i in range(counts):
            df1.loc[position,] = [response["candles"][i]["time"]] + [response["candles"][i]["mid"]['o']] +[ response["candles"][i]["mid"]['h']] + [response["candles"][i]["mid"]['l']] + [response["candles"][i]["mid"]['c'] ]

            position = position +1
        
    df = df.append(df1,ignore_index=True)

######  Check duplicate  ########3    
duplicates = df.duplicated()
print( "Duplicates Number: ",sum(1 for x in duplicates if x == True))
df = df.drop_duplicates()

##### Convert UTC to EST time
df = df.reset_index()
TimeList = df['Time']
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S %Z%z'
eastern = timezone('US/Eastern')

for i in range(len(TimeList)):
    yy = int(str(TimeList[i])[0:4])
    mo = int(str(TimeList[i])[5:7])
    dd = int(str(TimeList[i])[8:10])
    hh = int(str(TimeList[i])[11:13])
    mm = int(str(TimeList[i])[14:16])
    
    utc_dt = datetime.datetime(yy, mo, dd, hh, mm, 0, tzinfo=utc)
    loc_dt = utc_dt.astimezone(eastern)
    ESTTime = loc_dt.strftime(fmt)
    df['Time'].loc[i] = ESTTime
    print(ESTTime)

##### Convert Data String to Numeric, and convert USD/XYZ to XYZ/USD
    
df["High"] = pd.to_numeric(df["High"])
df["Low"] = pd.to_numeric(df["Low"])
df["Close"] = pd.to_numeric(df["Close"])
df["Open"] = pd.to_numeric(df["Open"])

if currency_pair[0:3] == "USD":
    df[["High","Low"]] = df [["Low","High"]]

    df["High"] = 1/df["High"]
    df["Low"] = 1/df["Low"]
    df["Close"] = 1/df["Close"]
    df["Open"] = 1/df["Open"]
    
####### !!!! Note: the time shows in the csv file is the start time of the day, so it corresponds to open price, the close price corresponds to the time+24h
    
df.to_csv("./Data/Temp.csv")

