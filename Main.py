# -*- coding: utf-8 -*-

####################################################################################################################################################################################################################
####################################################################################################################################################################################################################
#   Public DEPENDENCIES
####################################################################################################################################################################################################################
####################################################################################################################################################################################################################

import requests
import re
from datetime import date
import time
import MySQLdb
import pandas as pd
import math
import sqlalchemy

import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.collections import LineCollection
import pandas as pd
from sklearn import cluster, covariance, manifold

#####################################################################################################################################################################
#                   Public Constant
#####################################################################################################################################################################
StartNumber = 1
EndNumber = 999999
Stk_Prefix = 'sh'
URL_Prefix = 'http://hq.sinajs.cn/list='
headers = {'referer': 'http://finance.sina.com.cn'}
Lead_Stock_sh = 'http://hq.sinajs.cn/list=sh000001'
Block_Size = 800 #should be less than 873, tested on 20220114, 874 will not work
Time_Out = 1
sinajs_update_interval = 1

ShortDelay=0.01
MidDelay = 0.02
LongDelay = 0.05

Data_TimeZone = 28800#Meaning GMT + 8


#####################################################################################################################################################################
#                   MariaDB Constant
#####################################################################################################################################################################
'''
Trading_Table_Insertion_sh='(Code, Open_T, Close_Y, Current, Highest_T, Lowest_T, B, S, V, Amount, B1_V, B1_P, B2_V, B2_P, B3_V, B3_P, B4_V, B4_P, B5_V, B5_P, S1_V, S1_P, S2_V, S2_P, S3_V, S3_P, S4_V, S4_P, S5_V, S5_P, Epoch, Switch) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Open_T=VALUES(Open_T), Close_Y=VALUES(Close_Y), Current=VALUES(Current), Highest_T=VALUES(Highest_T), Lowest_T=VALUES(Lowest_T), B=VALUES(B), S=VALUES(S), V=VALUES(V), Amount=VALUES(Amount), B1_V=VALUES(B1_V), B1_P=VALUES(B1_P), B2_V=VALUES(B2_V), B2_P=VALUES(B2_P), B3_V=VALUES(B3_V), B3_P=VALUES(B3_P), B4_V=VALUES(B4_V), B4_P=VALUES(B4_P), B5_V=VALUES(B5_V), B5_P=VALUES(B5_P), S1_V=VALUES(S1_V), S1_P=VALUES(S1_P), S2_V=VALUES(S2_V), S2_P=VALUES(S2_P), S3_V=VALUES(S3_V), S3_P=VALUES(S3_P), S4_V=VALUES(S4_V), S4_P=VALUES(S4_P), S5_V=VALUES(S5_V), S5_P=VALUES(S5_P), Switch=VALUES(Switch)'

Trading_Table_Definition_sh='(Code CHAR(8), Open_T Decimal(8,2) UNSIGNED, Close_Y Decimal(8,2) UNSIGNED, Current Decimal(8,2) UNSIGNED, Highest_T Decimal(8,2) UNSIGNED, Lowest_T Decimal(8,2) UNSIGNED, B Decimal(8,2) UNSIGNED, S Decimal(8,2) UNSIGNED, V BIGINT UNSIGNED, Amount BIGINT UNSIGNED, B1_V BIGINT UNSIGNED, B1_P Decimal(8,2) UNSIGNED, B2_V BIGINT UNSIGNED, B2_P Decimal(8,2) UNSIGNED, B3_V BIGINT UNSIGNED, B3_P Decimal(8,2) UNSIGNED, B4_V BIGINT UNSIGNED, B4_P Decimal(8,2) UNSIGNED, B5_V BIGINT UNSIGNED, B5_P Decimal(8,2) UNSIGNED, S1_V BIGINT UNSIGNED, S1_P Decimal(8,2) UNSIGNED, S2_V BIGINT UNSIGNED, S2_P Decimal(8,2) UNSIGNED, S3_V BIGINT UNSIGNED, S3_P Decimal(8,2) UNSIGNED, S4_V BIGINT UNSIGNED, S4_P Decimal(8,2) UNSIGNED, S5_V BIGINT UNSIGNED, S5_P Decimal(8,2) UNSIGNED, Epoch INT SIGNED, Switch TINYINT SIGNED) DEFAULT CHARSET = utf8'#change Epoch from INT UNSIGNED to INT SIGNED, as later on caclculation needed minus value
'''

Market='sh'

Trading_Table_Insertion_sh='(Code, Open_T, Close_Y, Current, Highest_T, Lowest_T, B, S, V, Amount, B1_V, B1_P, B2_V, B2_P, B3_V, B3_P, B4_V, B4_P, B5_V, B5_P, S1_V, S1_P, S2_V, S2_P, S3_V, S3_P, S4_V, S4_P, S5_V, S5_P, Epoch) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Open_T=VALUES(Open_T), Close_Y=VALUES(Close_Y), Current=VALUES(Current), Highest_T=VALUES(Highest_T), Lowest_T=VALUES(Lowest_T), B=VALUES(B), S=VALUES(S), V=VALUES(V), Amount=VALUES(Amount), B1_V=VALUES(B1_V), B1_P=VALUES(B1_P), B2_V=VALUES(B2_V), B2_P=VALUES(B2_P), B3_V=VALUES(B3_V), B3_P=VALUES(B3_P), B4_V=VALUES(B4_V), B4_P=VALUES(B4_P), B5_V=VALUES(B5_V), B5_P=VALUES(B5_P), S1_V=VALUES(S1_V), S1_P=VALUES(S1_P), S2_V=VALUES(S2_V), S2_P=VALUES(S2_P), S3_V=VALUES(S3_V), S3_P=VALUES(S3_P), S4_V=VALUES(S4_V), S4_P=VALUES(S4_P), S5_V=VALUES(S5_V), S5_P=VALUES(S5_P)'
Trading_Table_Definition_sh='(Code CHAR(8), Open_T Decimal(8,2) UNSIGNED, Close_Y Decimal(8,2) UNSIGNED, Current Decimal(8,2) UNSIGNED, Highest_T Decimal(8,2) UNSIGNED, Lowest_T Decimal(8,2) UNSIGNED, B Decimal(8,2) UNSIGNED, S Decimal(8,2) UNSIGNED, V BIGINT UNSIGNED, Amount BIGINT UNSIGNED, B1_V BIGINT UNSIGNED, B1_P Decimal(8,2) UNSIGNED, B2_V BIGINT UNSIGNED, B2_P Decimal(8,2) UNSIGNED, B3_V BIGINT UNSIGNED, B3_P Decimal(8,2) UNSIGNED, B4_V BIGINT UNSIGNED, B4_P Decimal(8,2) UNSIGNED, B5_V BIGINT UNSIGNED, B5_P Decimal(8,2) UNSIGNED, S1_V BIGINT UNSIGNED, S1_P Decimal(8,2) UNSIGNED, S2_V BIGINT UNSIGNED, S2_P Decimal(8,2) UNSIGNED, S3_V BIGINT UNSIGNED, S3_P Decimal(8,2) UNSIGNED, S4_V BIGINT UNSIGNED, S4_P Decimal(8,2) UNSIGNED, S5_V BIGINT UNSIGNED, S5_P Decimal(8,2) UNSIGNED, Epoch INT UNSIGNED) DEFAULT CHARSET = utf8'


Trading_Table_Standardized_Insertion_sh='(Code, Open_T, Close_Y, Current, Highest_T, Lowest_T, B, S, V, Amount, B1_V, B1_P, B2_V, B2_P, B3_V, B3_P, B4_V, B4_P, B5_V, B5_P, S1_V, S1_P, S2_V, S2_P, S3_V, S3_P, S4_V, S4_P, S5_V, S5_P, Epoch) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Open_T=VALUES(Open_T), Close_Y=VALUES(Close_Y), Current=VALUES(Current), Highest_T=VALUES(Highest_T), Lowest_T=VALUES(Lowest_T), B=VALUES(B), S=VALUES(S), V=VALUES(V), Amount=VALUES(Amount), B1_V=VALUES(B1_V), B1_P=VALUES(B1_P), B2_V=VALUES(B2_V), B2_P=VALUES(B2_P), B3_V=VALUES(B3_V), B3_P=VALUES(B3_P), B4_V=VALUES(B4_V), B4_P=VALUES(B4_P), B5_V=VALUES(B5_V), B5_P=VALUES(B5_P), S1_V=VALUES(S1_V), S1_P=VALUES(S1_P), S2_V=VALUES(S2_V), S2_P=VALUES(S2_P), S3_V=VALUES(S3_V), S3_P=VALUES(S3_P), S4_V=VALUES(S4_V), S4_P=VALUES(S4_P), S5_V=VALUES(S5_V), S5_P=VALUES(S5_P))'
Trading_Table_Standardized_Definition_sh='(Code CHAR(8), Open_T Decimal(8,2) UNSIGNED, Close_Y Decimal(8,2) UNSIGNED, Current Decimal(8,2) UNSIGNED, Highest_T Decimal(8,2) UNSIGNED, Lowest_T Decimal(8,2) UNSIGNED, B Decimal(8,2) UNSIGNED, S Decimal(8,2) UNSIGNED, V BIGINT UNSIGNED, Amount BIGINT UNSIGNED, B1_V BIGINT UNSIGNED, B1_P Decimal(8,2) UNSIGNED, B2_V BIGINT UNSIGNED, B2_P Decimal(8,2) UNSIGNED, B3_V BIGINT UNSIGNED, B3_P Decimal(8,2) UNSIGNED, B4_V BIGINT UNSIGNED, B4_P Decimal(8,2) UNSIGNED, B5_V BIGINT UNSIGNED, B5_P Decimal(8,2) UNSIGNED, S1_V BIGINT UNSIGNED, S1_P Decimal(8,2) UNSIGNED, S2_V BIGINT UNSIGNED, S2_P Decimal(8,2) UNSIGNED, S3_V BIGINT UNSIGNED, S3_P Decimal(8,2) UNSIGNED, S4_V BIGINT UNSIGNED, S4_P Decimal(8,2) UNSIGNED, S5_V BIGINT UNSIGNED, S5_P Decimal(8,2) UNSIGNED, Epoch INT UNSIGNED) DEFAULT CHARSET = utf8'


STK_NameList_Table_Insertion='(Code, Name, Category, Market) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE Code = VALUES(Code), Name = VALUES(Name), Category = VALUES(Category), Market = VALUES(Market)'
STK_NameList_Table_Definition='(Code CHAR(8), Name VARCHAR(20), Category CHAR(5), Market CHAR(2)) DEFAULT CHARSET = utf8'


#Host = 'localhost'#'35.241.78.220'
Host = 'localhost'
#Host = '192.168.1.5'
Port = 3306
User = 'zenbot'
Password = 'password'
Database = 'STK_Test8'
STK_NameList_Table = 'STK_NameList'
Trading_Table = 'Trading_Table'
Trading_Table_CorrectionEpochTime = 'Trading_Table_CorrectionEpochTime'


#####################################################################################################################################################################
#                   Initiation
#####################################################################################################################################################################
def Initiation():
    ################################################################
    #Initiation.....
    print ("\n")
    print("Welcome to Stock Data Extracting Application!")
    time.sleep(ShortDelay)
    print ("\n")
    print ('Please ensure the "daemon_ping.py" has been already running, or at least the "PingTime.txt" is in the working folder')
    time.sleep(MidDelay)
    print ("\n")
    print('###################################################################')
    time.sleep(ShortDelay)
    print('#                                                                 #')
    time.sleep(ShortDelay)
    print('#                 system locale setting: zh_CN.UTF-8              #')
    time.sleep(ShortDelay)
    print('#      Timezone must be set, using "dpkg-reconfigure tzdata"      #')
    time.sleep(ShortDelay)
    print('#                                                                 #')
    time.sleep(ShortDelay)
    print('###################################################################')
    time.sleep(MidDelay)
    print ("\n")
    print('###################################################################')
    time.sleep(ShortDelay)
    print('#                        !!! Warning !!!                          #')
    time.sleep(ShortDelay)
    print('#   1, Do NOT run CPU consumping process such as 7z during script #')
    time.sleep(ShortDelay)
    print('#      running, doing this will trigger the VPS CPU clock locking!#')
    time.sleep(ShortDelay)
    print('#   2, running one 7z process is ok                               #')
    time.sleep(ShortDelay)
    print('#                                                                 #')
    time.sleep(ShortDelay)
    print('###################################################################')
    time.sleep(MidDelay)
    print ("\n")
    print('###################################################################')
    time.sleep(ShortDelay)
    print('#                        !!! Headsup !!!                          #')
    time.sleep(ShortDelay)
    print('#   1, To start a "clean" script, a reboot is needed to preven-   #')
    time.sleep(ShortDelay)
    print('#      t concurrent processes.                                    #')
    time.sleep(ShortDelay)
    print('###################################################################')
    time.sleep(LongDelay)
    print ("\n")
    print('Host=' + Host)


#####################################################################################################################################################################
#                   Play with numbers FUNCTIONS 
#####################################################################################################################################################################

# function-xxxx
################################################################
#PURPOSE  : Generate blocks of numbers
#INPUT    : StartNum, EndNum, BlockSize = how many Granularity for one block, Granularity
#INPUT eg.: Generate_Block(2, 19, 3, 1)
#OUTPUT   :
#REMARK   :  
def Generate_Block(StartNum, EndNum, BlockSize, Granularity):#default Granularity = 1
    BlockQty = math.ceil((EndNum - StartNum) / BlockSize)
    List = []
    i = 1
    j = StartNum
    while i <= BlockQty:
        Block = []
        Ceiling = StartNum + i * BlockSize
        while j < Ceiling and j <= EndNum:
            Block.append(j)
            j = j + Granularity
        List.append(Block)
        i = i + 1
    return List

################################################################
#PURPOSE  : Generate_EpochPair_PastFutureInterval, for tensorflow training
#INPUT    : 
#INPUT eg.: Generate_EpochPair_PastFutureInterval(1635830100, 1635836400, 100, 100)
#OUTPUT   : [[1635830100, 1635831100, 1635831200], [1635831100, 1635832100, 1635832200], [1635832100, 1635833100, 1635833200], [1635833100, 1635834100, 1635834200], [1635834100, 1635835100, 1635835200], [1635835100, 1635836100, 1635836200]]
#REMARK   : 
def Generate_EpochPair_PastFutureInterval(Epoch_Start, Epoch_End, Past_Interval, Future_Interval):
    Epoch_Top_End = Epoch_End - Future_Interval
    Epoch_MovingBottom = Epoch_Start
    EpochPair = []
    while Epoch_MovingBottom + Past_Interval <= Epoch_Top_End:# Epoch_Bottom + Past_Interval = Epoch_Top
        SinglePair = []
        SinglePair.append(Epoch_MovingBottom)
        SinglePair.append(Epoch_MovingBottom + Past_Interval)
        SinglePair.append(Epoch_MovingBottom + Past_Interval + Future_Interval)
        EpochPair.append(SinglePair)#, Epoch_Bottom + Past_Interval, Epoch_Bottom + Past_Interval + Future_Interval)
        Epoch_MovingBottom = Epoch_MovingBottom + Past_Interval
    return EpochPair





#####################################################################################################################################################################
#                   TIME FUNCTIONS 
#####################################################################################################################################################################

# function-xxxx
################################################################
#PURPOSE  : conbine the date info and time info from http://hq.sinajs.cn/list=sh600000
#INPUT    :
#INPUT eg.: DateTime_String('2021-10-20','11:28:04')
#OUTPUT   :
#REMARK   :  
def DateTime_String(Date_Str, Time_Str):
    DateTime_String = Date_Str + ' ' + Time_Str
    return DateTime_String


# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: DataTimeZoned_DateTime_To_EpochTimeStamp('2021-10-20 11:28:04', 28800)
#OUTPUT   : 
#REMARK   : 
def DataTimeZoned_DateTime_To_EpochTimeStamp(DataTimeZoned_DateTime_Str, Data_TimeZone):# DateTime_Format = '%Y-%m-%d %H:%M:%S', epoch e.g.1605422476
    Local_TimeAltZone = time.altzone# AltZone meaning TIME + AltZone = UTC
    DateTime_Format = '%Y-%m-%d %H:%M:%S'
    DataTimeZoned_Time_Object = time.strptime(DataTimeZoned_DateTime_Str, DateTime_Format)#due to it is input from TimeZoned_DateTime_Str which includes TimeZone# 
    LocalTimeZoned_DataTimeZoned_EpochTimeStamp = time.mktime(DataTimeZoned_Time_Object)
    DataTimeZoned_EpochTimeStamp = LocalTimeZoned_DataTimeZoned_EpochTimeStamp - Local_TimeAltZone
    EpochTimeStamp = int(DataTimeZoned_EpochTimeStamp - Data_TimeZone)
    return EpochTimeStamp




# Generate_EpochTime_Interval
################################################################
#PURPOSE  : Generate a list of epoch time, from Starttime to end time with interval
#INPUT    : 
#INPUT eg.: Generate_EpochTime_Interval(1635211200, 1635211260, 4)
#INPUT eg.: Generate_EpochTime_Interval(DataTimeZoned_DateTime_To_EpochTimeStamp('2021-10-26 10:24:00', 28800), DataTimeZoned_DateTime_To_EpochTimeStamp('2021-10-26 10:34:00', 28800), 30)
#OUTPUT   : list of epoch times
#REMARK   : 
def Generate_EpochTime_Interval(StartTime_Epoch, EndTime_Epoch, Interval):
    List = []
    i = StartTime_Epoch
    while i <= EndTime_Epoch:
        List.append(i)
        i = i + Interval
    return List


# Calculate_Interval_Proximity
################################################################
#PURPOSE  : Calculate_Interval_Proximity
#INPUT    : 
#INPUT eg.: Calculate_Interval_Proximity(1, 2, 4)
#OUTPUT   : 
#REMARK   : 
def Calculate_Interval_Proximity(StartTime_Epoch, Interval, EpochTime4Calculation):
    Times_Interval = int((EpochTime4Calculation - StartTime_Epoch) / Interval)
    Lower_Proximity = ((EpochTime4Calculation - StartTime_Epoch) % Interval ) / Interval
    Upper_Proximity = (EpochTime4Calculation - (StartTime_Epoch + Interval * (Times_Interval + 1))) / Interval
    if abs(Lower_Proximity) < abs(Upper_Proximity):
        return StartTime_Epoch + Interval * Times_Interval, Lower_Proximity
    if abs(Lower_Proximity) > abs(Upper_Proximity):
        return StartTime_Epoch + Interval * (Times_Interval + 1), Upper_Proximity
    else:#abs(Lower_Proximity) == abs(Upper_Proximity), currently temperarily useing lower
        return StartTime_Epoch + Interval * Times_Interval, Lower_Proximity


#####################################################################################################################################################################
#                   WebExtract
#####################################################################################################################################################################





#####################################################################################################################################################################
#                   Validity Check
#####################################################################################################################################################################

################################################################
#PURPOSE:   check if the line is valid with data, in terms of correct date, end with ";, suggestion the data is intact
#INPUT:     line in string datatype
#INPUT eg. :Line_DataIsValid('sh600211="西藏药业,80.720,80.720,79.270,81.860,78.580,79.220,79.270,3548361,283782348.000,2700,79.220,2000,79.200,1700,79.190,3500,79.180,2300,79.160,100,79.270,1300,79.280,900,79.290,200,79.300,900,79.320,2020-11-04,13:10:15,00,";')
#OUTPUT:    True/False
#REMARK:    the last cell is not involved, so has to
def Line_DataIsValid(line):
    CommaQty=line.count(',')
# sh000013="企债指数,0.0000,0.0000,0.0000,0.0000,0.0000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2021-12-06,,00,";
    if CommaQty == 33 and line.endswith('00,\";') and line.find(',,') == -1:
        if str(date.today()) == line.split(",")[30]: # Because there are some stock is with old Dates, which has to be ruled out
            return True
        else:
            return False
        return True
    else:
        return False

StartNumber = 1
EndNumber = 999999

#####################################################################################################################################################################
#                   TEXT PROCESSING
#####################################################################################################################################################################

# function-xxxx
################################################################
#PURPOSE  : Generate the stock ID/Code, e.g. sh600000
#INPUT    : 
#INPUT eg.: Generate_List_StkID(1, 999999, 'sh')
#OUTPUT   : 
#REMARK   : 
def Generate_List_StkID(Start, End, Stk_Prefix):
    List = []
    i = Start
    while i <= End:
        Number = str(i)
        Zeroed_Number = Number.rjust(6, '0')#6 is the number of digits in the sh000001
        Entry = Stk_Prefix + Zeroed_Number
        List.append(Entry)
        i = i + 1
    return List

# function-xxxx
################################################################
#PURPOSE  : Generate_List_URLs and divide by block size
#INPUT    : 
#INPUT eg.: Generate_List_URLs(['sh600000', 'sh600001', 'sh600002'], 'http://hq.sinajs.cn/list=', 1)
#INPUT eg.: Generate_List_URLs(Generate_List_StkID(600001, 600100, 'sh'), 'http://hq.sinajs.cn/list=', 33)
#OUTPUT   : 
#REMARK   : 
def Generate_List_URLs(List_StkID, URL_Prefix, Block_Size):#Normall, URL_Prefix='http://hq.sinajs.cn/list=', Block_Size=800
    Entry_Size = len(List_StkID)
    i = 0
    List_ListURLs = []
    while i < Entry_Size:
        ListURLs = List_StkID[i:i + Block_Size]#split the list by Block_Size
        Comma_Joined_ListURLs = ','.join(ListURLs)#join the StkID with comma
        URL_Prefixed_ListURLs = URL_Prefix + Comma_Joined_ListURLs# add prefix
        List_ListURLs.append(URL_Prefixed_ListURLs)
        i = i + Block_Size
    return List_ListURLs


# function-xxxx
################################################################
#PURPOSE  : Convert a list into a string
#INPUT    : 
#INPUT eg.: List2Text(['aa','bb','cc',3])
#OUTPUT   : 
#REMARK   : 
def List2Text(List):
    Text = ''
    for Element in List:
        Text = Text + ', ' + str(Element)
    return Text[2:len(Text)]

    


#####################################################################################################################################################################
############################################                         GET WEB CONTENT                       ##########################################################
#####################################################################################################################################################################

################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: GetPageContent('http://hq.sinajs.cn/list=sh600100', 1)
#OUTPUT   : 
#REMARK   : 
'''
import requests
import time
headers = {'referer': 'http://finance.sina.com.cn'}
Time_Out = 6
'''
def Requests_Get(URL, Time_Out):
    Start_Time = time.time()
    while True:
        try:
            resource = requests.get(URL, headers = headers, timeout = Time_Out)
            End_Time = time.time()
            TakeTime = str(round(End_Time - Start_Time, 3))
            GrabPage = resource.text.strip()
            break
        except:
            print('ERR in Requests_Get, Getting Web Content Failed, URL is:')
            print(URL)
            time.sleep(1)
            continue
    return GrabPage, TakeTime



# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: GetCurrentStockTime('http://hq.sinajs.cn/list=sh000001')
#OUTPUT   : 
#REMARK   : 
def GetCurrentStockTime(Lead_Stock_sh):# current meaning time from current stock data
    GrabPage = Requests_Get(Lead_Stock_sh, Time_Out)[0]
    cells=GrabPage.split(',')
    Time = cells[31]
    return Time


# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: GetCurrentStockTime('http://hq.sinajs.cn/list=sh000001')
#OUTPUT   : 
#REMARK   : 
def GetCurrentStockEpoch(Lead_Stock_sh):# current meaning time from current stock data
    GrabPage = Requests_Get(Lead_Stock_sh, Time_Out)[0]
    cells=GrabPage.split(',')
    Time = cells[31]
    Date = cells[30]
    Epoch = DataTimeZoned_DateTime_To_EpochTimeStamp(DateTime_String(Date, Time), Data_TimeZone)
    return Epoch



# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: 
#OUTPUT   : 
#REMARK   : 
def GetLines(List_StkID, URL_Prefix, Block_Size):
    List_URLs = Generate_List_URLs(List_StkID, URL_Prefix, Block_Size)
    Lines = []# Lines is the collections of n*Block_Size of lines of GrabPage
    counter = 0
    for URL in List_URLs:
        GrabPage = Requests_Get(URL, Time_Out)[0]
        Lines_In_Page = re.findall(r"sh.*",GrabPage)# reformat the content into lines
        for line in Lines_In_Page:
            if Line_DataIsValid(line):# ensure the line is valid, in terms of date is today, data is intact. etc.
                Lines.append(line)
        counter = counter + 1
        print('Completion Percentage: ' + str(round((counter / len(List_URLs)) * 100, 1)) + '%')
    return Lines


# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: 
#OUTPUT   : 
#REMARK   : if could be consolidated into getlines
def GetLines_SingleBlock(List_URLs, URL_Prefix):#!!!len(List_URLs)<= 840
#    List_URLs = Generate_List_URLs(List_StkID_LessThan_BlockSize, URL_Prefix, len(List_StkID_LessThan_BlockSize))
    Lines = []
    GrabPage = Requests_Get(List_URLs, Time_Out)[0]
    Lines_In_Page = re.findall(r"sh.*",GrabPage)# reformat the content into lines
    for line in Lines_In_Page:
        if Line_DataIsValid(line):# ensure the line is valid, in terms of date is today, data is intact. etc.
            Lines.append(line)
    return Lines




#####################################################################################################################################################################
############################################                         PROCESS FOR MYSQL                       ########################################################
#####################################################################################################################################################################


#################################################################################################################
#PURPOSE:   extract a list from lines to be appended to the fun_extracting4MySQL
#INPUT:     single line of STK status
#OUTPUT:    single line in list data type
#REMARK:    as append could not involve variable, so call a function has to be the method
def CreateMySQLEntry_Line(line):
    cellappend=[]
    cells=line.split(',')   # separate each value in the lines and put them in array cells
    Code = str(cells[0][0:8])
    #Name = str(cells[0][10:len(cells[0])])        #the name will be stored separately
    Open_T = round(float(cells[1]),2)
    Close_Y = round(float(cells[2]),2)
    Current = round(float(cells[3]),2)
    Highest_T = round(float(cells[4]),2)
    Lowest_T = round(float(cells[5]),2)
    B = round(float(cells[6]),2)
    S = round(float(cells[7]),2)
    V = int(float(cells[8]))
    Amount = int(float(cells[9])) # calc the float firstly to avoid the "int('0.00') causing the error of 'invalid literal for int()'
    B1_V = int(float(cells[10]))
    B1_P = round(float(cells[11]),2)
    B2_V = int(float(cells[12]))
    B2_P = round(float(cells[13]),2)
    B3_V = int(float(cells[14]))
    B3_P = round(float(cells[15]),2)
    B4_V = int(float(cells[16]))
    B4_P = round(float(cells[17]),2)
    B5_V = int(float(cells[18]))
    B5_P = round(float(cells[19]),2)
    S1_V = int(float(cells[20]))
    S1_P = round(float(cells[21]),2)
    S2_V = int(float(cells[22]))
    S2_P = round(float(cells[23]),2)
    S3_V = int(float(cells[24]))
    S3_P = round(float(cells[25]),2)
    S4_V = int(float(cells[26]))
    S4_P = round(float(cells[27]),2)
    S5_V = int(float(cells[28]))
    S5_P = round(float(cells[29]),2)
    Date = cells[30]
    Time = cells[31]
    Epoch = DataTimeZoned_DateTime_To_EpochTimeStamp(DateTime_String(Date, Time), Data_TimeZone)
#appending
    cellappend.append(Code)
    #cellappend.append(Name)        #the name will be stored separately
    cellappend.append(Open_T)
    cellappend.append(Close_Y)
    cellappend.append(Current)
    cellappend.append(Highest_T)
    cellappend.append(Lowest_T)
    cellappend.append(B)
    cellappend.append(S)
    cellappend.append(V)
    cellappend.append(Amount)
    cellappend.append(B1_V)
    cellappend.append(B1_P)
    cellappend.append(B2_V)
    cellappend.append(B2_P)
    cellappend.append(B3_V)
    cellappend.append(B3_P)
    cellappend.append(B4_V)
    cellappend.append(B4_P)
    cellappend.append(B5_V)
    cellappend.append(B5_P)
    cellappend.append(S1_V)
    cellappend.append(S1_P)
    cellappend.append(S2_V)
    cellappend.append(S2_P)
    cellappend.append(S3_V)
    cellappend.append(S3_P)
    cellappend.append(S4_V)
    cellappend.append(S4_P)
    cellappend.append(S5_V)
    cellappend.append(S5_P)
    cellappend.append(Epoch)
    return cellappend



#################################################################################################################
#PURPOSE:   append the lines to make the argument for the executemany
#INPUT:     lines
#OUTPUT:    data type appended lists(in list)
#REMARK:    
def AppendEntries4MySQL_Line(Lines):
    AppendEntries4MySQL=[]
    for Line in Lines:   # go through every line in the data. e.g. sh000012="xxxxxx,150.963,150.948,151.032,151.037,150.961,0,0,485766,497691869,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2015-09-02,15:04:08,00";
        AppendEntries4MySQL.append(CreateMySQLEntry_Line(Line))
    return AppendEntries4MySQL
#var hq_str_sh220228="";
#var hq_str_sh600228="昌九生化,9.00,9.09,9.42,9.58,9.00,9.41,9.42,4071062,37925267,3100,9.41,900,9.40,2400,9.38,2400,9.37,11100,9.36,49000,9.42,500,9.45,1000,9.46,40600,9.47,5000,9.49,2015-09-17,14:20:30,00";
    


#################################################################################################################
#PURPOSE:   extract a list from 
#INPUT:     single line of 
#OUTPUT:    single line in list data type
#REMARK:    as append could not involve variable, so call a function has to be the method
def CreateMySQLEntry_STK_NameList(Line):
    cellappend=[]
    cells=Line.split(',')   # separate each value in the lines and put them in array cells
    Code = str(cells[0][0:8])
    B1_V = int(float(cells[10]))
    B1_P = round(float(cells[11]),2)
    B2_V = int(float(cells[12]))
    B2_P = round(float(cells[13]),2)
    B3_V = int(float(cells[14]))
    B3_P = round(float(cells[15]),2)
    B4_V = int(float(cells[16]))
    B4_P = round(float(cells[17]),2)
    B5_V = int(float(cells[18]))
    B5_P = round(float(cells[19]),2)
    S1_V = int(float(cells[20]))
    S1_P = round(float(cells[21]),2)
    S2_V = int(float(cells[22]))
    S2_P = round(float(cells[23]),2)
    S3_V = int(float(cells[24]))
    S3_P = round(float(cells[25]),2)
    S4_V = int(float(cells[26]))
    S4_P = round(float(cells[27]),2)
    S5_V = int(float(cells[28]))
    S5_P = round(float(cells[29]),2)
#    if B1_V != 0 and B1_P != 0 and B2_V != 0 and B2_P != 0 and B3_V != 0 and B3_P != 0 and B4_V != 0 and B4_P != 0 and B5_V != 0 and B5_P != 0 and S1_V != 0 and S1_P != 0 and S2_V != 0 and S2_P != 0 and S3_V != 0 and S3_P != 0 and S4_V != 0 and S4_P != 0 and S5_V != 0 and S5_P != 0:
    if B1_V + B2_V + B3_V + B4_V + B5_V != 0 or S1_V + S2_V + S3_V + S4_V + S5_V != 0:
        Category = 'Stock'
        print(Category)
    else:
        Category = 'Index'
        print(Category)
    Name = str(cells[0][10:len(cells[0])])
    Market = Code[0:2]#only support sh and sz, as 2 digits
#appending
    cellappend.append(Code)
    cellappend.append(Name)
    cellappend.append(Category)
    cellappend.append(Market)
    return cellappend

#################################################################################################################
#PURPOSE:   append the lines to make the argument for the executemany
#INPUT:     lines
#OUTPUT:    data type appended lists(in list)
#REMARK:    
def AppendEntries4MySQL_STK_NameList(Lines):
    AppendEntries4MySQL=[]
    for Line in Lines:   # go through every line in the data. e.g. sh000012="xxxxxx,150.963,150.948,151.032,151.037,150.961,0,0,485766,497691869,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2015-09-02,15:04:08,00";
        AppendEntries4MySQL.append(CreateMySQLEntry_STK_NameList(Line))
    return AppendEntries4MySQL
#var hq_str_sh220228="";
#var hq_str_sh600228="昌九生化,9.00,9.09,9.42,9.58,9.00,9.41,9.42,4071062,37925267,3100,9.41,900,9.40,2400,9.38,2400,9.37,11100,9.36,49000,9.42,500,9.45,1000,9.46,40600,9.47,5000,9.49,2015-09-17,14:20:30,00";




#################################################################################################################
#PURPOSE:   read from lines and find the date in the valid line
#INPUT:     lines
#OUTPUT:    date
#REMARK:
def FindDate(Lines):
    for Line in Lines: # below codes are to find the date in a valid data entry
        if Line_DataIsValid(Line):
            FindDate=Line.split(",")[30]
            break
    return FindDate



#################################################################################################################
#PURPOSE:   read from lines and find the time in the valid line
#INPUT:     lines
#OUTPUT:    time
#REMARK:
def FindTime(Lines):
    for Line in Lines: # below codes are to find the date in a valid data entry
        if Line_DataIsValid(Line):
            FindTime=Line.split(",")[31]
            break
    return FindTime

####################################################################################################################################################################################################################
####################################################################################################################################################################################################################
##########################################################################                         Initiate MairaDB of STK Table                 ###################################################################
####################################################################################################################################################################################################################
####################################################################################################################################################################################################################



####################################################################################################################################################################################################################
####################################################################################################################################################################################################################
##########################################################################                              MariaDB                                  ###################################################################
####################################################################################################################################################################################################################
####################################################################################################################################################################################################################



#####################################################################################################################################################################
############################################                    FUNCTIONS - QUERY                            ########################################################
#####################################################################################################################################################################



######################################################################################################################################
############################################        QUERY - Database Level         ###################################################
######################################################################################################################################

#################################################################################################################
#PURPOSE:   	Get the database names from MySQL
#INPUT:     	datebase name in string
#OUTPUT:    	Set: 
#DEPENDENCIES	
#REMARK:    	
def ShowDatabases():
    while True:
        try:
            databases=set()
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SHOW DATABASES'
            cur.execute(statement)
            results = cur.fetchall()
            conn.close()
            for result in results:
                databases.add(result[0])
            return databases
        except:
            continue


######################################################################################################################################
############################################        QUERY - Table Level         ######################################################
######################################################################################################################################

#################################################################################################################
#PURPOSE:   Get the table name by omitting the '-'
#INPUT:     market name in string
#OUTPUT:    xxyyyymmdd, xx is the market name
#REMARK:    
def GetTableName():
    elements = ''
    for element in str(date.today()).split('-'):
        elements = elements + element
    TableName = market + elements
    return TableName


#################################################################################################################
#PURPOSE:   Get the tables from specific database
#INPUT:     datebase name in string
#OUTPUT:    tables
#REMARK:    
def ShowTables(database):
    while True:
        try:
            Tables = set()
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SHOW TABLES'
            cur.execute(statement)
            results = cur.fetchall()
            conn.close()
            for result in results:
                Tables.add(result[0])
            return Tables
        except:
            continue




######################################################################################################################################
############################################        QUERY - Entry Level         ######################################################
######################################################################################################################################
def ReadSQLQuery_df(statement, Database):
    while True:
        try:
            URL_Object = "mysql://" + User + ":" + Password + "@" + Host + ":" + str(Port) +  "/" + Database
            engine = sqlalchemy.create_engine(URL_Object)
            df = pd.read_sql_query(statement, engine)
            if len(df)>=1:
                return df
            else:
                return False
        except:
            print('ERR in ReadSQLQuery_df. ' + 'statement=' + statement)
            time.sleep(3)
            continue


####################################################################################################
#PURPOSE:   check if exist from mysql, matching Fielddata=xxx
#INPUT:	    
#OUTPUT:     
#REMARK:    
def QueryEntry(Database, TableName, Field, FieldMatchingValue):
    statement='SELECT * FROM '+ TableName + ' WHERE ' + Field + '=' + '\'' + str(FieldMatchingValue) + '\''
    df = ReadSQLQuery_df(statement, Database)
    return df



####################################################################################################
#PURPOSE:   check if exist from mysql, matching Fielddata=xxx
#INPUT:	    
#OUTPUT:     
#REMARK:    
#e.g.       QueryEntryFields(Database, Trading_Table_CorrectionEpochTime, 'Code', 'sh600009', 'Code,Current')
def QueryEntryFieldsbk(Database, TableName, Field, FieldMatchingValue, Fields):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='SELECT ' + Fields + ' FROM '+ TableName + ' WHERE ' + Field + '=' + '\'' + str(FieldMatchingValue) + '\''
            df = pd.read_sql(statement, con=conn)
            conn.close()
            if len(df)>=1:
                return df
            else:
                return False
        except:
            continue


def QueryEntryFields(Database, TableName, Field, FieldMatchingValue, Fields):
    statement='SELECT ' + Fields + ' FROM '+ TableName + ' WHERE ' + Field + '=' + '\'' + str(FieldMatchingValue) + '\''
    df = ReadSQLQuery_df(statement, Database)
    return df




####################################################################################################
#PURPOSE:   check if exist from mysql, matching Fielddata=xxx
#INPUT:	    
#OUTPUT:     
#REMARK:    
def QueryEntry_List(Database, TableName, Field, FieldMatchingValue):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='SELECT * FROM '+ TableName + ' WHERE ' + Field + '=' + '\'' + str(FieldMatchingValue) + '\''
            cur.execute(statement)
            results = cur.fetchall()
            conn.close()
            if len(results)>=1:
                return [list(result) for result in results]
            else:
                return False
        except:
            continue



# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: QueryRowNum('STK_Test8', 'Trading_Table')
#INPUT eg.: QueryRowNum('STK_Test8', 'STK_NameList')
#OUTPUT   : 
#REMARK   : 
def QueryRowNum(Database, TableName):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='SELECT COUNT(*) FROM '+ TableName
            cur.execute(statement)
            results = cur.fetchall()
            conn.close()
            QueryRowNum = results[0][0]
            if QueryRowNum == 0:
                return False
            else:
                return QueryRowNum
        except:
            continue



# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: QueryGetRowsByLimit(Database, Trading_Table, 2, 2)
#OUTPUT   : 
#REMARK   : 
def QueryGetRowsByLimit(Database, TableName, LimitStart, LimitQty, OutputDatatype):#Datatype define whether to output Dataframe or List
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='SELECT * FROM '+ TableName + ' LIMIT ' + str(LimitStart) + ',' + str(LimitQty)
            if OutputDatatype == 'Dataframe':
                df = pd.read_sql(statement, con=conn)
                conn.close()
                if len(df)>=1:
                    return df
                else:
                    return False
            elif OutputDatatype == 'List':
                cur.execute(statement)
                results = cur.fetchall()
                conn.close()
                if len(results)>=1:
                    return [list(result) for result in results]
                else:
                    return False
        except:
            time.sleep(3)
            continue




####################################################################################################
#PURPOSE:   check if exist from mysql, matching Fielddata=xxx
#INPUT:	    
#OUTPUT:     
#REMARK:    
#e.g.       QueryEntryFieldsByMultipleFilter(Database, Trading_Table_CorrectionEpochTime, {'Code':'sh600009','Epoch':1635835980}, ['Code', 'Current'], 'Dataframe')
def QueryEntryFieldsByMultipleFilter(Database, TableName, Criteria_Dict, QueryFields_List, OutputDatatype):
#   generate the WhereContent string from Criteria_Dict
    Criteria = list(Criteria_Dict.items())
    print(Criteria)
    WhereContent = ''
    for Criterion in Criteria:
        Field = ''
        Field = str(Criterion[0]) + '=' + '\'' + str(Criterion[1]) + '\'' 
        WhereContent = WhereContent + Field + ' AND '
    WhereContent = WhereContent[0:len(WhereContent)-len(' AND ')]
#   generate the SelectFields string from QueryFields_List
    SelectFields = ''
    for SelectField in QueryFields_List:
        SelectFields = SelectFields + SelectField + ' , '
    SelectFields = SelectFields[0:len(SelectFields)-len(' , ')]
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='SELECT ' + SelectFields + ' FROM '+ TableName + ' WHERE ' + WhereContent
            if OutputDatatype == 'Dataframe':
                df = pd.read_sql(statement, con=conn)
                conn.close()
                if len(df)>=1:
                    return df
                else:
                    return False
            elif OutputDatatype == 'List':
                cur.execute(statement)
                results = cur.fetchall()
                conn.close()
                if len(results)>=1:
                    return [list(result) for result in results]
                else:
                    return False
        except:
            time.sleep(3)
            continue





####################################################################################################
#PURPOSE:   check if exist from mysql, matching Fielddata=xxx
#INPUT:	    
#OUTPUT:     
#REMARK:    
def QueryEntry_STD_Threshhold_Lower(Database, TableName, STD_Threshhold_Lower, Field_Criteria_STD, Field_Lookup):
    STD_Threshhold_Lower = str(STD_Threshhold_Lower)
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SELECT ' + Field_Lookup + ' FROM ' + TableName + ' GROUP BY ' + Field_Lookup + ' HAVING STDDEV(' + Field_Criteria_STD + ') > ' + STD_Threshhold_Lower
            df = pd.read_sql(statement, con=conn)
            conn.close()
            if len(df)>=1:
                return df
            else:
                return False
        except:
            continue

####################################################################################################
#PURPOSE:   check if exist from mysql, matching Fielddata=xxx
#INPUT:	    
#OUTPUT:     
#REMARK:    
def QueryEntry_STD_Range(Database, TableName, STD_Threshhold_Lower, STD_Threshhold_Higher, Field_Criteria_STD, Field_Lookup):
    STD_Threshhold_Lower = str(STD_Threshhold_Lower)
    STD_Threshhold_Higher = str(STD_Threshhold_Higher)
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SELECT ' + Field_Lookup + ' FROM ' + TableName + ' GROUP BY ' + Field_Lookup + ' HAVING STDDEV(' + Field_Criteria_STD + ') >= ' + STD_Threshhold_Lower + ' AND STDDEV(' + Field_Criteria_STD + ') <= ' + STD_Threshhold_Higher
            print(statement)
            df = pd.read_sql(statement, con=conn)
            conn.close()
            if len(df)>=1:
                return df
            else:
                return False
        except:
            continue



#SELECT Code FROM Trading_Table_CorrectionEpochTime GROUP BY Code HAVING STDDEV(Current) >= 0.2 AND STDDEV(Current) <= 99


################################################################
#PURPOSE  : Select based on a list of Fields, base on RangeField (e.g. Epoch)
#INPUT    : 
#INPUT eg.: QueryEntry_By_FieldRange(Database, TableName, ['Code','Current'], 'Epoch', DataTimeZoned_DateTime_To_EpochTimeStamp('2021-11-02 13:30:0', 28800), DataTimeZoned_DateTime_To_EpochTimeStamp('2021-11-02 13:50:0', 28800), 'Dataframe')
#OUTPUT   : 
#REMARK   : 
def QueryEntry_By_FieldRange(Database, TableName, Query_List, RangeField, RangeField_Bottom, RangeField_Top, OutputDatatype):#Datatype define whether to output Dataframe or List
    Fields = List2Text(Query_List)
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SELECT ' + Fields + ' FROM ' + TableName + ' WHERE '+ RangeField + ' >= ' + str(RangeField_Bottom) + ' AND ' +  RangeField + ' <= ' + str(RangeField_Top)
            print(statement)
            if OutputDatatype == 'Dataframe':
                df = pd.read_sql(statement, con=conn)
                conn.close()
                if len(df)>=1:
                    return df
                else:
                    return False
            elif OutputDatatype == 'List':
                cur.execute(statement)
                results = cur.fetchall()
                conn.close()
                if len(results)>=1:
                    return [list(result) for result in results]
                else:
                    return False
        except:
            time.sleep(3)
            print('exception')
            continue


################################################################
#PURPOSE  : Select based on a list of Fields, base on RangeField, and Code (e.g. Epoch)
#INPUT    : 
#INPUT eg.: QueryEntry_By_FieldRange(Database, TableName, ['Code','Current'], 'Epoch', DataTimeZoned_DateTime_To_EpochTimeStamp('2021-11-02 13:30:0', 28800), DataTimeZoned_DateTime_To_EpochTimeStamp('2021-11-02 13:50:0', 28800), 'sh600000', 'Dataframe')
#OUTPUT   : 
#REMARK   : 
def QueryEntry_By_FieldRange_SingleCodeBK(Database, TableName, Query_List, RangeField, RangeField_Bottom, RangeField_Top, SingleCode, OutputDatatype):#Datatype define whether to output Dataframe or List
    Fields = List2Text(Query_List)
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SELECT ' + Fields + ' FROM ' + TableName + ' WHERE '+ ' Code = ' + '\'' + SingleCode + '\'' + ' AND ' + RangeField + ' >= ' + str(RangeField_Bottom) + ' AND ' +  RangeField + ' <= ' + str(RangeField_Top)
            print(statement)
            if OutputDatatype == 'Dataframe':
                df = pd.read_sql(statement, con=conn)
                conn.close()
                if len(df)>=1:
                    return df
                else:
                    return False
            elif OutputDatatype == 'List':
                cur.execute(statement)
                results = cur.fetchall()
                conn.close()
                if len(results)>=1:
                    return [list(result) for result in results]
                else:
                    return False
        except:
            time.sleep(3)
            print('exception')
            continue


def QueryEntry2List(Database, TableName, statement):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            cur.execute(statement)
            results = cur.fetchall()
            conn.close()
            if len(results)>=1:
                return [list(result) for result in results]
            else:
                return False
        except:
            time.sleep(3)
            print('exception')
            continue



def QueryEntry_By_FieldRange_SingleCode(Database, TableName, Query_List, RangeField, RangeField_Bottom, RangeField_Top, SingleCode, OutputDatatype):#Datatype define whether to output Dataframe or List
    Fields = List2Text(Query_List)
    statement = 'SELECT ' + Fields + ' FROM ' + TableName + ' WHERE '+ ' Code = ' + '\'' + SingleCode + '\'' + ' AND ' + RangeField + ' >= ' + str(RangeField_Bottom) + ' AND ' +  RangeField + ' <= ' + str(RangeField_Top)
    print(statement)
    if OutputDatatype == 'Dataframe':
        Data_df = ReadSQLQuery_df(statement, Database)
        return Data_df
    elif OutputDatatype == 'List':
        Data_List = QueryEntry2List(Database, TableName, statement)
        return Data_List




def drawline(Database, TableName, Query_List, Code, StartTime, EndTime):# DateTime_Format = '%Y-%m-%d %H:%M:%S', epoch e.g.1605422476
    StartTime = DataTimeZoned_DateTime_To_EpochTimeStamp(str(StartTime), Data_TimeZone)
    EndTime = DataTimeZoned_DateTime_To_EpochTimeStamp(str(EndTime), Data_TimeZone)# DateTime_Format = '%Y-%m-%d %H:%M:%S', epoch e.g.1605422476
    df = QueryEntry_By_FieldRange_SingleCode(Database, TableName, Query_List, "Epoch", StartTime, EndTime, Code, 'Dataframe')
    fig=plt.figure()
    ax1=fig.add_subplot(2,1,1)
    ax2=fig.add_subplot(2,1,2)
    df.plot(kind='line', grid=True, figsize=(16,12), legend=True, ax=ax1)# https://pandas.pydata.org/pandas-docs/version/0.23.4/generated/pandas.DataFrame.plot.html
    df.plot(kind='kde', grid=True, figsize=(16,12), legend=True, ax=ax2)# https://pandas.pydata.org/pandas-docs/version/0.23.4/generated/pandas.DataFrame.plot.html
    # line, kde
    plt.show()
    print(aa)

ccc=drawline(Database, "Trading_Table", ["B1_V"], 'sh600570', '2023-01-04 13:20:00', '2023-01-07 14:20:00')
print(ccc)

####################################################################################################
#PURPOSE:   Read the last entry from MySQL
#INPUT:     Strings: Database, TableName
#OUTPUT:    Pandas.DataFrame:
#REMARK:    use df.iloc[0] to extract a line, use list(df) to get column names
# ReadLastEntryFromMySQL(Database, TableName).['Calc_Index'][0] to find the last Calc_Index number.
def ReadLastEntryFromMySQL(Database, TableName):
    statement = SelectFromComplier(ShowColumns(Database,TableName)) + TableName + ' LIMIT 1'
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            df = pd.read_sql(statement, con=conn)
            conn.close()
            if len(df)==0:
                return False
            elif len(df)!=0:
                ListFormat=df.values.tolist()[0]
                return ListFormat
        except my.OperationalError as error:
            print(error)
            print('retry in 3 seconds')
            time.sleep(3)
            continue
        except:
            print('ERR in ReadLastEntryFromMySQL. ' + 'Database=' + Database + '; Table=' + TableName)
            time.sleep(3)
            continue



####################################################################################################
#PURPOSE:   	Get the tables from specific Database
#INPUT:     	datebase name in string
#OUTPUT:    	Set: 
#DEPENDENCIES	
#REMARK:    	
def ShowTables(Database):
    while True:
        try:
            tables=set()
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SHOW TABLES'
            cur.execute(statement)
            results = cur.fetchall()
            conn.close()
            for result in results:
                tables.add(result[0])
            return tables
        except:
            continue


####################################################################################################
#PURPOSE:   	Get the tables from specific Database
#INPUT:     	datebase name in string
#OUTPUT:    	Set: 
#DEPENDENCIES	
#REMARK:    	
def TableIsEmpty(Database,TableName):
    if ReadLastEntryFromMySQL(Database, TableName)==False:
        return True
    else:
        return False



####################################################################################################
#PURPOSE:   Complile the "SELECT xxx-1, xxx-2, xxx-n from " Part in statement 
#INPUT:     Tuple: a list of xxx
#OUTPUT:    String: "SELECT xxx-1, xxx-2, xxx-n from "
#REMARK:    
def SelectFromComplier(StringInTuple):
    append=''
    for String in StringInTuple:
    	append=append+String+','
    append=append.rstrip(',')
    append='SELECT '+append+' from '
    return append


####################################################################################################
#PURPOSE:   	ShowColumns
#INPUT:     	datebase name in string
#OUTPUT:    	list
#DEPENDENCIES	
#REMARK:    	
def ShowColumns(Database,TableName):
    while True:
        try:
            columns=[]
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement = 'SHOW COLUMNS from ' + TableName;
            cur.execute(statement)
            results = cur.fetchall()
            conn.close()
            for result in results:
                columns.append(result[0])
            return columns
        except:
            continue

####################################################################################################
#PURPOSE:   Get the Epoch time that is closest to the TimePoints
#INPUT:     
#OUTPUT:    
#REMARK:    
# SQL QUERY = SELECT Epoch FROM(SELECT MIN(Diff), Epoch FROM (SELECT ABS(Epoch - 1609311615) AS Diff, Epoch FROM Trading_Table WHERE Code = 'sh155148') AS Diff_Table) AS Result_Table LIMIT 1;
def GetDiffTime(Database, TableName, EpochTime, Code):#EpochTime is int
    EpochTime = str(EpochTime)
    statement = 'SELECT Epoch FROM(SELECT MIN(Diff), Epoch FROM (SELECT ABS(Epoch - ' + EpochTime + ') AS Diff, Epoch FROM ' + Trading_Table + ' WHERE Code = \'' + Code + '\') AS Diff_Table) AS Result_Table  LIMIT 1'#!!!!!!!!!!!!!!!!!!!!!!!!!!!!Trading_Table to be changed to TableName?
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            df = pd.read_sql(statement, con=conn)
            conn.close()
            if len(df)==0:
                return False
            elif len(df)!=0:
                ListFormat=df.values.tolist()[0]
                return ListFormat
        except my.OperationalError as error:
            print(error)
            print('retry in 3 seconds')
            time.sleep(3)
            continue
        except:
            print('ERR in GetDiffTime. ' + 'Database=' + Database + '; Table=' + TableName)
            time.sleep(3)
            continue

#Epoch=1609305550

####################################################################################################
#PURPOSE:   
#INPUT:     
#OUTPUT:    
#REMARK:    
#SELECT Epoch FROM ((SELECT ABS(Epoch - 1609307040) AS Diff, Epoch FROM Trading_Table WHERE Code = 'sh603236') AS Diff_Table) WHERE Diff = (SELECT MIN(Diff) FROM (SELECT ABS(Epoch - 1609307040) AS Diff, Epoch FROM Trading_Table WHERE Code = 'sh603236') AS Diff_Table) LIMIT 1
#sh603236,EpochTime_Segmented:1609307040,EpochTime:1609307040
#INSERT INTO Trading_Table_CorrectionEpochTime SELECT * FROM Trading_Table WHERE (Code, Epoch) = ('sh603236', 1609307040)
#UPDATE Trading_Table_CorrectionEpochTime SET Epoch = 1609307040 WHERE (Code, Epoch) = ('sh603236', 1609307040)
def CorrectionEpochTime(Database, TableName_Source, TableName_Destination, EpochTime_Segmented, Code):#EpochTime_Segmented is int
    EpochTime_Segmented = str(EpochTime_Segmented)
    Statement_Closest_EpochTime = 'SELECT Epoch FROM ((SELECT ABS(Epoch - ' + EpochTime_Segmented + ') AS Diff, Epoch FROM ' + TableName_Source + ' WHERE Code = \'' + Code + '\') AS Diff_Table) WHERE Diff = (SELECT MIN(Diff) FROM (SELECT ABS(Epoch - ' + EpochTime_Segmented + ') AS Diff, Epoch FROM ' + TableName_Source + ' WHERE Code = \'' + Code + '\') AS Diff_Table) LIMIT 1'
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            df = pd.read_sql(Statement_Closest_EpochTime, con=conn)
#            print('---------------------------------------------------------------------------------------')
#            print(Statement_Closest_EpochTime)
#            print(Code + ',' + 'EpochTime_Segmented:' + EpochTime_Segmented + ',' + 'EpochTime:' + str(df.values.tolist()[0][0]))
            if len(df) == 0:
                return False
            elif len(df) != 0:
                Closest_EpochTime = df.values.tolist()[0][0]    
                if Closest_EpochTime != None:
                    Closest_EpochTime = str(df.values.tolist()[0][0])
#                    print(Closest_EpochTime)
#                    print(Closest_EpochTime)
                    Statement_Insetion = 'INSERT INTO ' + TableName_Destination + ' SELECT * FROM ' + TableName_Source + ' WHERE (Code, Epoch) = (' + '\'' + Code + '\'' + ', ' + Closest_EpochTime + ')'
                    Statement_Update = 'UPDATE ' + TableName_Destination + ' SET Epoch = ' + EpochTime_Segmented + ' WHERE (Code, Epoch) = (' + '\'' + Code + '\'' + ', ' + Closest_EpochTime + ')'
                    cur.execute(Statement_Insetion)
                    conn.commit()
#                    print(Statement_Insetion)
#                    print(Statement_Update)
                    cur.execute(Statement_Update)
                    conn.commit()
                    conn.close()
#                    print('---------------------------------------------------------------------------------------')
                    break
                else:
                    print(Code)
                    break
        except MySQLdb.OperationalError as error:
            print(error)
            print(Statement_Insetion)
            print(cur.execute(Statement_Update))
            print('retry in 3 seconds')
            time.sleep(3)
            continue
        except:
            print(EpochTime_Segmented + ' ' + Statement_Closest_EpochTime + ' ' + Code)
            time.sleep(3)
            continue 




#####################################################################################################################################################################
############################################                    FUNCTIONS - CREATION                            #####################################################
#####################################################################################################################################################################


#################################################################################################################
#PURPOSE:   	create database
#INPUT:		
#DEPENDENCIES	
#REMARK:   	the warning could be silented by enabling "filterwarnings('ignore', category = MySQLdb.Warning)"
def CreateDatabase(Database):
    databases = ShowDatabases()
    print(databases)
    if Database not in databases:
        while True:
            try:
                conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, charset = 'utf8')
                cur = conn.cursor()
                statement='CREATE DATABASE ' + Database
                cur.execute(statement)
                print('Database '+ Database + ' has been Created') # !!!!improvement, if there is arelday talbe exist...
                #conn.commit()
                conn.close()
                break
            except:
                continue
    else:
        print('Database '+ Database + ' already exists')


#################################################################################################################
#PURPOSE:   create table
#INPUT:
#OUTPUT:
#REMARK:   the warning could be silented by enabling "filterwarnings('ignore', category = MySQLdb.Warning)"
def CreateTable(Database, TableName, TableDefinition):
    CreateDatabase(Database)
    Tables = ShowTables(Database)
    if TableName not in Tables:
        while True:
            try:
                conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
                cur = conn.cursor()
                statement='CREATE TABLE IF NOT EXISTS ' + TableName + TableDefinition
                cur.execute(statement)
                print('Table '+ TableName + ' has been Created') # !!!!improvement, if there is arelday talbe exist...
                #conn.commit()
                conn.close()
                break
            except:
                print('ERROR, CreateTable: ' + TableName )
                print('statement = '  + chr(39) + statement + chr(39))
                print('Database = '  + chr(39) + Database + chr(39))
                continue
    else:
        print('Table '+ TableName + ' already exists')


#################################################################################################################
#PURPOSE:   drop table
#INPUT:
#OUTPUT: DropTable(Database, 'Trading_Table_CorrectionEpochTime')
#REMARK:   the warning could be silented by enabling "filterwarnings('ignore', category = MySQLdb.Warning)"
def DropTable(Database, TableName):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='DROP TABLE IF EXISTS ' + TableName
            cur.execute(statement)
            print('Table '+ TableName + ' has been dropped') 
            #conn.commit()
            conn.close()
            break
        except:
            print('ERROR, DropTable: ' + TableName )
            print('statement = '  + chr(39) + statement + chr(39))
            print('Database = '  + chr(39) + Database + chr(39))
            continue
        else:
            print('Table '+ TableName + ' is not exists')





####################################################################################################
#PURPOSE:   	CREATE UNIQUE INDEX index_name ON table_name(index_column_1,index_column_2,...);
#INPUT:		
#DEPENDENCIES	
#REMARK:   	CREATE UNIQUE INDEX Calc_Index ON Auto_CalcResult(Calc_Index);
#REMARK:   	from FUN_Database import CreateUniqueIndex
def CreateUniqueIndex(Database, TableName, UniqueIndex):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='CREATE UNIQUE INDEX ' + UniqueIndex + ' ON ' + TableName + '(' + UniqueIndex + ')'
#            print(statement)
            cur.execute(statement)#conn.commit()
            print('Unique Index ' + UniqueIndex + ' on ' + TableName + ' has been created.')
            conn.close()
            break
        except:
            print('ERROR in creating, please check ' + UniqueIndex + ' on ' + TableName + ' if has been already created')
            break



####################################################################################################
#PURPOSE:   	CREATE UNIQUE INDEX index_name ON table_name(index_column_1,index_column_2,...);
#INPUT:		
#DEPENDENCIES	
#REMARK:   	CREATE UNIQUE INDEX Calc_Index ON Auto_CalcResult(Calc_Index);
#REMARK:   	from FUN_Database import CreateUniqueIndex
def Create_Composite_Indexes(Database, TableName, UniqueIndexes, Composite_Indexes):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='CREATE UNIQUE INDEX ' + Composite_Indexes + ' ON ' + TableName + '(' + UniqueIndexes + ')'
#            print(statement)
            cur.execute(statement)#conn.commit()
            print('Composite Indexes ' + Composite_Indexes + ' on ' + TableName + '(' + UniqueIndexes + ')' + ' has been created.')
            conn.close()
            break
        except:
            print('ERROR in Create_Composite_Indexes, please check ' + UniqueIndexes + ' on ' + TableName + ' if has been already created')
            break


#####################################################################################################################################################################
############################################                    FUNCTIONS - SAVING                            #######################################################
#####################################################################################################################################################################


####################################################################################################
#PURPOSE:   save the data to MySQL database
#INPUT:	    in list[Tuples(),Tuples(),Tuples()] format, e.g.=> data = [('Jane', 12),('Joe', 13),('John', 14)] or [['Jane', 12],['Joe', 13],['John', 14]]
#OUTPUT:    a file stores the string data
#REMARK:    locale settings to be prudent
def SaveData2MySQL(Database, TableName, TableInsertion, InsertedData):
    while True:
        try:
            conn = MySQLdb.connect(host = Host, port = Port, user = User, passwd = Password, db = Database, charset = 'utf8')
            cur = conn.cursor()
            statement='INSERT INTO '+ TableName + TableInsertion
            cur.executemany(statement, InsertedData)
            conn.commit()
            conn.close()
            return True
#            break
        except MySQLdb.OperationalError as error:
            print(error)
            print('retry in 3 seconds')
            time.sleep(3)
            continue
        except:
#            print('ERR in SaveData2MySQL, could be duplicated key')
            print('statement = ' + statement)
#            print('InsertedData = ')
            print(InsertedData)
#            break
            return False


# function-xxxx
################################################################
#PURPOSE  : To Create a new table(Trading_Table_CorrectionEpochTime) with standardized time intervals
#INPUT    : StartTime_Epoch, EndTime_Epoch, Interval, Trading_Table
#INPUT eg.: Create_Trading_Table_SameInterval(1635215074, 1635218989, 100)
#OUTPUT   : Trading_Table_SameInterval
#REMARK   : 
def Create_Trading_Table_SameInterval(EpochList, CodeList):
    DropTable(Database, Trading_Table_CorrectionEpochTime)
    CreateTable(Database, Trading_Table_CorrectionEpochTime, Trading_Table_Standardized_Definition_sh)
    for Code in CodeList:
        print(Code)
        Start_Time = time.time()
        CodeTradingData = np.array(QueryEntry_List(Database, Trading_Table, 'Code', Code))# guessing the query time would be quite long, optimization mighte be needed.
        End_Time = time.time()
        TakeTime = str(round(End_Time - Start_Time, 3))
        print(TakeTime)
        CodeTradingData_Epoch = CodeTradingData[:, [30]]
        CodeTradingData_EpochStandardized = []
        for Epoch in EpochList:
            Diff_CodeTradingData_Epoch = np.absolute(CodeTradingData_Epoch - Epoch)
            index = Diff_CodeTradingData_Epoch.argmin()# find the index of minimum element from the array
            Closest_SingleCodeTradingData = CodeTradingData[index:index+1]# find the single trading entry that is closest to the epoch
            Closest_SingleCodeTradingData_T = Closest_SingleCodeTradingData.T# 
            Closest_SingleCodeTradingData_T[30:31] = Epoch
            Closest_SingleCodeTradingData_List = Closest_SingleCodeTradingData_T.T.tolist()[0]#to avoid the error [[[]],[[]]] (  need [[],[]]     )
            CodeTradingData_EpochStandardized.append(Closest_SingleCodeTradingData_List)
        SaveData2MySQL(Database, Trading_Table_CorrectionEpochTime, Trading_Table_Insertion_sh, CodeTradingData_EpochStandardized)


33333333333333333333333333333333333

def DrawingSingleCode(Database, Table, TableName, Code, StartTime, EndTime):
    QueryEntry(Database, TableName, Field, FieldMatchingValue)



####################################################################################################################################################################################################################
####################################################################################################################################################################################################################
##########################################################################                              MAIN - SUB                               ###################################################################
####################################################################################################################################################################################################################
####################################################################################################################################################################################################################




def Initilization_Database():
    CreateDatabase(Database)
# create the STK_NameList_Table
    CreateTable(Database, STK_NameList_Table, STK_NameList_Table_Definition)
    UniqueIndex = 'Code'
    CreateUniqueIndex(Database, STK_NameList_Table, UniqueIndex)
#'''    if TableIsEmpty(Database,STK_NameList_Table):
#        print(STK_NameList_Table + 'is empty. Will start update process')
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# NEED TO TRUNCATE TABLE FIRSTLY
#        Update_STK_NameList_Table_LoopOnNoneTradingDay()
# create the Trading_Table
    CreateTable(Database, Trading_Table, Trading_Table_Definition_sh)
    UniqueIndexes = 'Code, Epoch'
    Composite_Indexes = 'Code_Epoch'
    Create_Composite_Indexes(Database, Trading_Table, UniqueIndexes, Composite_Indexes)
# create the Trading_Table_CorrectionEpochTime
    CreateTable(Database, Trading_Table_CorrectionEpochTime, Trading_Table_Definition_sh)
    UniqueIndexes = 'Code, Epoch'
    Composite_Indexes = 'Code_Epoch'
    Create_Composite_Indexes(Database, Trading_Table_CorrectionEpochTime, UniqueIndexes, Composite_Indexes)



def Update_STK_NameList_Table():
    print('Starting, Refreshing the STK_NameList_Table.')
    List_StkID = Generate_List_StkID(StartNumber, EndNumber, Stk_Prefix)
    Lines = GetLines(List_StkID, URL_Prefix, Block_Size)
    InsertedData_STK_NameList = AppendEntries4MySQL_STK_NameList(Lines)
    SaveData2MySQL(Database, STK_NameList_Table, STK_NameList_Table_Insertion, InsertedData_STK_NameList)


def Update_STK_NameList_Table_LoopOnNoneTradingDay():
# if on none trading day, causing the grabbed data date is out of date, then do loop unitll the trading day comes.
    while TableIsEmpty(Database,STK_NameList_Table):
        CurrentEpoch = int(time.time())
        StockDataEpoch = GetCurrentStockEpoch(Lead_Stock_sh)
        if CurrentEpoch - StockDataEpoch <= 60:
            Update_STK_NameList_Table()
        elif CurrentEpoch - StockDataEpoch >= (24-15)*3600:# meaning the date is differnt
            print('CurrentEpoch - StockDataEpoch > 60... Sleep 60 Seconds... (In case none trading day)')
            time.sleep(60)
        else:
            Update_STK_NameList_Table()






def Update_STK_Trading_Table():
    Stock_df = QueryEntry(Database, STK_NameList_Table, 'Category', 'Stock')# Get all stock where category=stock
    StockCode_List = Stock_df['Code'].to_list()# turn all stock code into a list
    List_URLs = Generate_List_URLs(StockCode_List, URL_Prefix, Block_Size)# Divide all codes by Block_Size, Generate a list of URL which is in form of http://hq.sinajs.cn/list=code1,code2....
    Count = 1
    while True:
        print('Collecting Count No.' + str(Count) + ', Stock Quantity = ' + str(len(StockCode_List)))
        a = time.time()
        for URLs in List_URLs:# in turn, extract from API and save to MariaDB
            Lines = GetLines_SingleBlock(URLs, URL_Prefix)
            InsertedData_STK = AppendEntries4MySQL_Line(Lines)
            Save_TrueFalse = SaveData2MySQL(Database, Trading_Table, Trading_Table_Insertion_sh, InsertedData_STK)
        b = time.time()
        print('Collecting Count No.' + str(Count) + ': Getting time = ' + str(round(b-a,2)))
        Count = Count + 1
        time.sleep(sinajs_update_interval)


#Prepare the Trading_Table_CorrectionEpochTime table
def Update_Trading_CorrectionEpochTime_Table():
# The issue is low calculation speed due to single threaded process. The solution could be running the calculation from multiple python scripts simultaneously, with a table hosted in mysql distributing the calculation tasks.
# To clean the data by removing the stock/index of low capturing rate, run with below line in mysql
# DELETE FROM Trading_Table WHERE Code IN (SELECT Code FROM (SELECT Code, COUNT(*) AS Count FROM Trading_Table GROUP BY Code) AS BB WHERE Count <= 1000);
# DELETE FROM STK_NameList WHERE Code NOT IN (SELECT Code FROM Trading_Table GROUP BY Code);
    TableName_Source = 'Trading_Table'
    TableName_Destination = 'Trading_Table_CorrectionEpochTime'
#    Stock_df = QueryEntry_STD_Threshhold_Lower(Database, 'Trading_Table', 0.2, 'Current', 'Code')
    Stock_df = QueryEntry(Database, STK_NameList_Table, 'Category', 'Stock')
#    print(Stock_df)
    Codes_list = Stock_df['Code'].tolist()
#    print(Codes_list)
    Time_Start_str = '2021-01-21 13:02:00'
    Time_End_str = '2021-01-21 15:00:00'
    Time_Start = DataTimeZoned_DateTime_To_EpochTimeStamp(Time_Start_str, Data_TimeZone) #Int
    Time_End = DataTimeZoned_DateTime_To_EpochTimeStamp(Time_End_str, Data_TimeZone) #Int
    Time_Interval = 30# Int, Second
#    code_1 = Codes_list[0]
#    while ts < Time_End
    TimePoints = []
    TimePoint = Time_Start
    while TimePoint <= Time_End:
#    while TimePoint < Time_End:# This is for segmented timeslot for multiprocessing
        TimePoints.append(TimePoint)
        TimePoint = TimePoint + Time_Interval
#    print(TimePoints)
    for TimePoint in TimePoints:
        for Code in Codes_list:
            CorrectionEpochTime(Database, TableName_Source, TableName_Destination, TimePoint, Code)#!!!!!!!!!!!!!!!!!!!!!!!!!





# function-xxxx
################################################################
#PURPOSE  : select codes from codeslist based on the xxxxx (in shxxxxx)
#INPUT    : 
#INPUT eg.: 
#OUTPUT   : 
#REMARK   : 
def CodeSelectByRange(CodeList, Bottom, Top):
    Codes = []
    for Code in CodeList:
        CodeNumber = int(Code[2:len(Code)])
        Header = Code[0:2]
        if CodeNumber >= Bottom and CodeNumber <= Top:
            Code_Reassemble = Header + str(CodeNumber)
            Codes.append(Code_Reassemble)
    return Codes


# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: Create_Trading_Table_CorrectionEpochTime(1635215074, 1635218989, 60)
#OUTPUT   : 
#REMARK   : 
def Create_Trading_Table_CorrectionEpochTime(CodeList, StartTime_Epoch, EndTime_Epoch, Interval):
    EpochList = Generate_EpochTime_Interval(StartTime_Epoch, EndTime_Epoch, Interval)
    Create_Trading_Table_SameInterval(EpochList, CodeList)



# function-xxxx
################################################################
#PURPOSE  : 
#INPUT    : 
#INPUT eg.: Stock_Structure(0.3, 99, 0, 200, 15, 0.5)
#OUTPUT   : 
#REMARK   : 
def Stock_Structure(STD_Threshhold_Lower, STD_Threshhold_Higher, AP_random_state, AP_max_iter, AP_convergence_iter, AP_damping):
    from matplotlib import font_manager
    font_manager.fontManager.addfont('./SIMSUN.ttf')
    matplotlib.rcParams['font.family'] = ['SimSun']
#    matplotlib.rcParams['font.family'] = ['Songti SC']
    Stock_df = QueryEntry_STD_Range(Database, Trading_Table_CorrectionEpochTime, STD_Threshhold_Lower, STD_Threshhold_Higher, 'Current', 'Code')# get data from Trading_Table_CorrectionEpochTime
    Code_List = Stock_df.to_dict('list')['Code']
    Code_Name_List = []
    for Code in Code_List:
        Code_Name_Query = QueryEntry(Database, 'STK_NameList', 'Code', Code)
        if isinstance(Code_Name_Query, pd.DataFrame):
            Code_Name_List.append(Code_Name_Query[['Code', 'Name']].values.tolist()[0])
    symbol_dict = {item[0]:item[1] for item in Code_Name_List}
    symbols, names = np.array(sorted(symbol_dict.items())).T
#    print(names[labels == 2])
    Code_Variation_List = []
    Code_List = [Code[0] for Code in Code_Name_List]#re-create the Code_List based on decreated info
    for Code in Code_List:
        Code_Current_List = QueryEntry(Database, Trading_Table_CorrectionEpochTime, 'Code', Code)['Current']#'sh605500'
        Current_Start = Code_Current_List[0]
        del Code_Current_List[0]
        Variations = []
        for Current in Code_Current_List:
            Variation = Current - Current_Start
            Variations.append(Variation)
            Current_Start = Current
        Code_Variation_List.append(Variations)
        arr = np.array(Code_Variation_List)
    #print(Code_Variation_List)
    # #############################################################################
    # Learn a graphical structure from the correlations
    edge_model = covariance.GraphicalLassoCV()
    # standardize the time series: using correlations rather than covariance
    # is more efficient for structure recovery
    X = arr.copy().T
    X = X / X.std(axis = 0)
    edge_model.fit(X)
    # #############################################################################
    # Cluster using affinity propagation
    #_, labels = cluster.affinity_propagation(edge_model.covariance_, random_state = AP_random_state, max_iter = AP_max_iter, convergence_iter = AP_convergence_iter, damping = AP_damping, preference = 0)
    _, labels = cluster.affinity_propagation(edge_model.covariance_, random_state = AP_random_state)
# S, *, preference=None, convergence_iter=15, max_iter=200, damping=0.5, copy=True, verbose=False, return_n_iter=False, random_state='warn'
    print(names[labels])
    n_labels = labels.max()
    for i in range(n_labels + 1):
        print('Cluster %i: %s' % ((i + 1), ', '.join(names[labels == i])))
        print('Cluster %i: %s' % ((i + 1), ', '.join(symbols[labels == i])))
#        print('Cluster %i: %s' % ((i + 1), ', '.join(names[labels == i] + '(' + symbols[labels == i] + ')')))
    # #############################################################################
    # Find a low-dimension embedding for visualization: find the best position of
    # the nodes (the stocks) on a 2D plane
    # We use a dense eigen_solver to achieve reproducibility (arpack is
    # initiated with random vectors that we don't control). In addition, we
    # use a large number of neighbors to capture the large-scale structure.
    #node_position_model = manifold.LocallyLinearEmbedding(n_components=2, eigen_solver='dense', n_neighbors=6)
    node_position_model = manifold.LocallyLinearEmbedding(n_components=2, eigen_solver='dense', n_neighbors=6)  #n_neighbors 就是 kNN 里的 k，就是在做分类时，我们选取问题点最近的多少个最近邻。
    embedding = node_position_model.fit_transform(X.T).T
    # #############################################################################
    # Visualization
    plt.figure(1, facecolor='w', figsize=(10, 8))
    plt.clf()
    ax = plt.axes([0., 0., 1., 1.])
    plt.axis('off')
    # Display a graph of the partial correlations
    partial_correlations = edge_model.precision_.copy()
    d = 1 / np.sqrt(np.diag(partial_correlations))
    partial_correlations *= d
    partial_correlations *= d[:, np.newaxis]
    non_zero = (np.abs(np.triu(partial_correlations, k=1)) > 0.02)
    # Plot the nodes using the coordinates of our embedding
    plt.scatter(embedding[0], embedding[1], s=50 * d ** 2, c=labels, cmap=plt.cm.nipy_spectral)
    # Plot the edges
    start_idx, end_idx = np.where(non_zero)
    # a sequence of (*line0*, *line1*, *line2*), where::
    #            linen = (x0, y0), (x1, y1), ... (xm, ym)
    segments = [[embedding[:, start], embedding[:, stop]]
                for start, stop in zip(start_idx, end_idx)]
    values = np.abs(partial_correlations[non_zero])
    lc = LineCollection(segments, zorder=0, cmap=plt.cm.hot_r, norm=plt.Normalize(0, .7 * values.max()))# 自带的反转就是在后边加一个 _r
    lc.set_array(values)
    lc.set_linewidths(16 * values)
#    lc.set_linewidths(15 * values)
    ax.add_collection(lc)
    # Add a label to each node. The challenge here is that we want to
    # position the labels to avoid overlap with other labels
    for index, (name, label, (x, y)) in enumerate(zip(names, labels, embedding.T)):
        dx = x - embedding[0]
        dx[index] = 1
        dy = y - embedding[1]
        dy[index] = 1
        this_dx = dx[np.argmin(np.abs(dy))]
        this_dy = dy[np.argmin(np.abs(dx))]
        if this_dx > 0:
            horizontalalignment = 'left'
            x = x + .002
        else:
            horizontalalignment = 'right'
            x = x - .002
        if this_dy > 0:
            verticalalignment = 'bottom'
            y = y + .002
        else:
            verticalalignment = 'top'
            y = y - .002
        plt.text(x, y, name, size=10, horizontalalignment=horizontalalignment, verticalalignment=verticalalignment, bbox=dict(facecolor='w', edgecolor=plt.cm.nipy_spectral(label / float(n_labels)), alpha=.6))
    plt.xlim(embedding[0].min() - .15 * embedding[0].ptp(), embedding[0].max() + .10 * embedding[0].ptp(),)
    plt.ylim(embedding[1].min() - .03 * embedding[1].ptp(), embedding[1].max() + .03 * embedding[1].ptp())
#    ax.set_xlim(x-0.3, x+0.3)
#    ax.set_ylim(y-0.3, y+0.3)
#    plt.savefig('1080.svg', figsize = (190.20, 100.80), format = 'svg')#, figsize = (190.20, 100.80), dpi = 1200
    plt.show()
    n_labels = labels.max()
    for i in range(n_labels + 1):
        Codes = symbols[labels == i]
        Current_List =[]
        for Code in Codes:
            Code_Current = QueryEntry(Database, Trading_Table_CorrectionEpochTime, 'Code', Code)['Current'].tolist()
            Current_List.append(Code_Current)
        arr = np.array(Current_List)
        X = arr.copy().T
        #X = X / X.mean(axis = 0)
        X = (X - X.mean(axis = 0)) / X.mean(axis = 0) # to be displayed in title, 100X * 100 
        df = pd.DataFrame(X, columns = names[labels == i])
        #print(df.cov())
        from tabulate import tabulate
        print(tabulate(df.cov(), showindex=True, headers=df.cov().columns))
        fig=plt.figure()
        ax1=fig.add_subplot(2,1,1)
        ax2=fig.add_subplot(2,1,2)
        df.plot(kind='line', grid=True, figsize=(16,12), legend=True, ax=ax1)# https://pandas.pydata.org/pandas-docs/version/0.23.4/generated/pandas.DataFrame.plot.html
        df.plot(kind='kde', grid=True, figsize=(16,12), legend=True, ax=ax2)# https://pandas.pydata.org/pandas-docs/version/0.23.4/generated/pandas.DataFrame.plot.html
        # line, kde
        plt.show()





####################################################################################################################################################################################################################
####################################################################################################################################################################################################################
##########################################################################                                Main                                   ###################################################################
####################################################################################################################################################################################################################
####################################################################################################################################################################################################################



#####################################################################################################################################################################
############################################                    INITIATION                           ################################################################
#####################################################################################################################################################################

# Initiation and welcome
####################################################################################################
Initiation()

# Initilization_Database
####################################################################################################
Initilization_Database()

# If refreshing the STK_NameList_Table or just start from data extracting
####################################################################################################
print('###################################################################')
time.sleep(ShortDelay)
print('#                    !!! Selected_Option !!!                                     #')
time.sleep(ShortDelay)
print('#   Option-1, Update the Table NameList                                          #')
print('#   Option-2, Update the Table Trading_Table (will no work in none-trading day)  #')
print('#   Option-3, Create Trading_Table_CorrectionEpochTime. MAY TAKE A WHILE. * Select the Bottem/Top time carefully, excluding the non validate data         #')
print('#   Option-4, Stock_Structure                                                    #')
time.sleep(LongDelay)
time.sleep(LongDelay)
time.sleep(LongDelay)
print('###################################################################')
if TableIsEmpty(Database,STK_NameList_Table):
    print('!!! ' + STK_NameList_Table + 'is empty !!!')
#****************************************************************************************
# to be commented for running as service
#****************************************************************************************
import sys, select
print('You have 10 seconds to select, else go to default to collect data~')
i, o, e = select.select( [sys.stdin], [], [], 10 )
if (i):
    Selected_Option = sys.stdin.readline().strip()
    print('Select Option-', Selected_Option)
else:
    Selected_Option = '2'
    print('Select Option-', Selected_Option)
#Selected_Option = '2'# to be uncommented for running as service
#****************************************************************************************
#****************************************************************************************
#****************************************************************************************
#Selected_Option = input("Selected_Option: ")
#Selected_Option = '6'


#####################################################################################################################################################################
############################################                    GET DATA                             ################################################################
#####################################################################################################################################################################

# Option-1, to update the Table NameList
if Selected_Option == '1':
    Update_STK_NameList_Table()
elif Selected_Option == '2':
    if TableIsEmpty(Database,STK_NameList_Table):
        Update_STK_NameList_Table_LoopOnNoneTradingDay()
    Update_STK_Trading_Table()
elif Selected_Option == '3':#Prepare the Trading_Table_CorrectionEpochTime table
    Stock_df = QueryEntry(Database, STK_NameList_Table, 'Category', 'Stock')
    CodeList = Stock_df.to_dict('list')['Code']
    CodeList = CodeSelectByRange(CodeList, 600000, 609999)
    DefaultBottomTime = '2023-01-04 13:20:00'
    DefaultTopTime = '2023-01-03 14:55:00'
    DefaultInterval = 20
    print('use \'SELECT COUNT(*) FROM Trading_Table_CorrectionEpochTime;\' to monitor progress')
    print('DefaultBottomTime = [' + DefaultBottomTime + ']\n' + 'DefaultTopTime    = [' + DefaultTopTime + ']\n' + 'DefaultInterval   = [' + str(DefaultInterval) + ']\n')
    UsingDefault = input('Using default parameters, Type y: ')
    if UsingDefault == 'y':
        Create_Trading_Table_CorrectionEpochTime(CodeList, DataTimeZoned_DateTime_To_EpochTimeStamp(DefaultBottomTime, Data_TimeZone), DataTimeZoned_DateTime_To_EpochTimeStamp(DefaultTopTime, Data_TimeZone), DefaultInterval)
    else:
        BottomTime = DataTimeZoned_DateTime_To_EpochTimeStamp(input('BottomTime [' + DefaultBottomTime + ']: '), Data_TimeZone)
        TopTime = DataTimeZoned_DateTime_To_EpochTimeStamp(input('TopTime [' + DefaultTopTime + ']: '), Data_TimeZone)
        Interval = input('Interval [' + DefaultInterval + ']: ')
        Create_Trading_Table_CorrectionEpochTime(CodeList, BottomTime, TopTime, Interval)
elif Selected_Option == '4':
    UsingDefault = input('Using default parameters, Type y: ')
    if UsingDefault == 'y':
        Stock_Structure(0.1, 1, 0, 100, 15, 0.5)
    else:
        STD_Threshhold_Lower = float(input('STD_Threshhold_Lower: '))
        STD_Threshhold_Higher = float(input('STD_Threshhold_Higher: '))
        AP_random_state = int(input('AP_random_state [default:0]: '))
        AP_max_iter = int(input('AP_max_iter [default:200]: '))
        AP_convergence_iter = int(input('AP_convergence_iter [default:15]: '))
        AP_damping = float(input('AP_damping [default:0.5]: '))
        print(STD_Threshhold_Lower, STD_Threshhold_Higher, AP_random_state, AP_max_iter, AP_convergence_iter, AP_damping)
        Stock_Structure(STD_Threshhold_Lower, STD_Threshhold_Higher, AP_random_state, AP_max_iter, AP_convergence_iter, AP_damping)
elif Selected_Option == '5':
    UsingDefault = input('Using default parameters, Type y: ')
    if UsingDefault == 'y':
        Stock_Structure(0.1, 1, 0, 100, 15, 0.5)
    else:
        STD_Threshhold_Lower = float(input('STD_Threshhold_Lower: '))
        STD_Threshhold_Higher = float(input('STD_Threshhold_Higher: '))
        AP_random_state = int(input('AP_random_state [default:0]: '))
        AP_max_iter = int(input('AP_max_iter [default:200]: '))
        AP_convergence_iter = int(input('AP_convergence_iter [default:15]: '))
        AP_damping = float(input('AP_damping [default:0.5]: '))
        print(STD_Threshhold_Lower, STD_Threshhold_Higher, AP_random_state, AP_max_iter, AP_convergence_iter, AP_damping)
        Stock_Structure(STD_Threshhold_Lower, STD_Threshhold_Higher, AP_random_state, AP_max_iter, AP_convergence_iter, AP_damping)


