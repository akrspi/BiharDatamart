import psycopg2
import csv
import pandas as pd
import numpy as np
import requests
import json
from dateutil import parser
import datetime

def mapApplicationChannel(s):
     return  s.capitalize()
     
def map_vehicle_status(s):
    if s == 'SCHEDULED':
        return 'Scheduled'
    elif s == 'DISPOSED':
        return 'Disposed'
    elif s == 'WAITING_FOR_DISPOSAL':
        return 'Waiting for disposal'
    else:
        return s
    
def map_propertytype(s):
    return  s.capitalize()

def map_propertySubType(s):
     return (s.replace('_',' ').capitalize())


def map_paymentsource (s):
    return s.capitalize()
def map_paymentmode (s):
    return s.capitalize()

def mapstate(s):
    return 'Odisha'

def mapDistrict(s):
    if s =='Phagwara':
        return 'Jalandhar'
    else:
        return s;

def map_paymentsourceFromMode(s):
    if s=='Online' :
        return 'Online'
    else :
        return 'Counter'

def mapsLocality(s):
    return ''


def connect():
    try:
        conn = psycopg2.connect(database="mgramseva_uat", user="mgramsevauat",password="mGramseva4321", host="magramseva-uat-db.cw1hfdqtf5pw.ap-south-1.rds.amazonaws.com")
        print("Connection established!")
    except Exception as exception:
        print("Exception occurred while connecting to the database")
        print(exception)

    #Last Week
    impactQuery="SELECT 'Last Week' as Period, count(*) as demand_count, sum(taxamount) as demand_totalamount, sum(collectionamount) as Demand_Collected from egbs_demand_v1 demand inner join eg_ws_connection ws on demand.consumercode=ws.connectionno inner join egbs_demanddetail_v1 demanddetail on demand.id=demanddetail.demandid and (demand.createdtime/1000)>=cast(((extract(epoch from now()))-604800) as bigint) where demand.status='ACTIVE' and demand.businessservice='WS';"
    
    # to_char((to_timestamp(taxperiodfrom/1000)::timestamp  at time Zone 'Asia/Kolkata'), 'mm/dd/yyyy HH24:MI:SS') as taxperiodfrom, to_char((to_timestamp(taxperiodto/1000)::timestamp  at time Zone 'Asia/Kolkata'), 'mm/dd/yyyy HH24:MI:SS') as taxperiodto

    #starttime = input('Enter start date (dd-mm-yyyy): ')
    #endtime = input('Enter end date (dd-mm-yyyy): ')
    #fsmquery = fsmquery.replace('{START_TIME}',dateToEpoch(starttime))
    #fsmquery = fsmquery.replace('{END_TIME}',dateToEpoch(endtime))
    
    query = pd.read_sql_query(impactQuery, conn)
    data = pd.DataFrame(query)
    
    
    #Total number of payments   
    noOfPayments="select 'Last Week' as Period,count(*) as total_payments,COALESCE(sum(pd.amountpaid),0) as total_paid_amount from egcl_payment p join egcl_paymentdetail pd on p.id = pd.paymentid where pd.businessservice = 'WS' and (pd.createdtime/1000)>=cast(((extract(epoch from now()))-604800) as bigint)"
    noOfPaymentsQuery=pd.read_sql_query(noOfPayments,conn)
    noOfPaymentsData=pd.DataFrame(noOfPaymentsQuery)
    
    #Adding the number of consumers   
    noOfConsumers="select 'Last Week' as Period, count(*), count(distinct tenantid) as tenantids from eg_ws_connection where (createdtime/1000)>=cast(((extract(epoch from now()))-604800) as bigint)"
    noOfConsumersQuery=pd.read_sql_query(noOfConsumers,conn)
    noOfConsumersData=pd.DataFrame(noOfConsumersQuery)

    
    data.columns=['Period', 'Demand Count','Total Demand Amount', 'Demand Collected']
    
    noOfPaymentsData.columns=['Period', 'Total No. of Payments', 'Total Payment Amount Collected']
    noOfConsumersData.columns=['Period', 'Total No. of consumers', 'Total Tenants used']
    
    
    impactdata = pd.DataFrame()
    
    impactdata=pd.merge(data,noOfPaymentsData,left_on='Period',right_on='Period',how='left')
    impactdata=pd.merge(impactdata,noOfConsumersData,left_on='Period',right_on='Period',how='left')
    
    #print("Data 1\n",impactdata)
    
    #####################################
    #Last Second Week
    impactQueryWeek2="SELECT 'Last Second Week' as Period, count(*) as demand_count, sum(taxamount) as demand_totalamount, sum(collectionamount) as Demand_Collected from egbs_demand_v1 demand inner join eg_ws_connection ws on demand.consumercode=ws.connectionno inner join egbs_demanddetail_v1 demanddetail on demand.id=demanddetail.demandid and ((demand.createdtime)/1000)>=cast(((extract(epoch from now()))-604800*2) as bigint) and ((demand.createdtime)/1000)<cast(((extract(epoch from now()))-604800*1) as bigint) where demand.status='ACTIVE' and demand.businessservice='WS';"
        
    queryWeek2 = pd.read_sql_query(impactQueryWeek2, conn)
    dataWeek2 = pd.DataFrame(queryWeek2)
    
    
    #Total number of payments   
    noOfPaymentsWeek2="select 'Last Second Week' as Period,count(*) as total_payments,COALESCE(sum(pd.amountpaid),0) as total_paid_amount from egcl_payment p join egcl_paymentdetail pd on p.id = pd.paymentid where pd.businessservice = 'WS' and (pd.createdtime/1000)>=cast(((extract(epoch from now()))-604800*2) as bigint) and (pd.createdtime/1000)<cast(((extract(epoch from now()))-604800*1) as bigint)"
    noOfPaymentsQueryWeek2=pd.read_sql_query(noOfPaymentsWeek2,conn)
    noOfPaymentsDataWeek2=pd.DataFrame(noOfPaymentsQueryWeek2)
    
    #Adding the number of consumers   
    noOfConsumersWeek2="select 'Last Second Week' as Period, count(*), count(distinct tenantid) as tenantids from eg_ws_connection where (createdtime/1000)>=cast(((extract(epoch from now()))-604800*2) as bigint) and (createdtime/1000)<cast(((extract(epoch from now()))-604800*1) as bigint)"
    noOfConsumersQueryWeek2=pd.read_sql_query(noOfConsumersWeek2,conn)
    noOfConsumersDataWeek2=pd.DataFrame(noOfConsumersQueryWeek2)

    
    dataWeek2.columns=['Period', 'Demand Count','Total Demand Amount', 'Demand Collected']
    
    
    noOfPaymentsDataWeek2.columns=['Period', 'Total No. of Payments', 'Total Payment Amount Collected']
    noOfConsumersDataWeek2.columns=['Period', 'Total No. of consumers', 'Total Tenants used']
    
    
    impactdataWeek2 = pd.DataFrame()
    
    impactdataWeek2=pd.merge(dataWeek2,noOfPaymentsDataWeek2,left_on='Period',right_on='Period',how='left')
    impactdataWeek2=pd.merge(impactdataWeek2,noOfConsumersDataWeek2,left_on='Period',right_on='Period',how='left')
    
    
    #impactdata = impactdata.append(impactdataWeek2, ignore_index = True)
    #print("Data 2\n",impactdataWeek2)
    #print("Data 2\n",impactdata)
    
    #####################################
    #Last Third Week
    impactQueryWeek3="SELECT 'Last Third Week' as Period, count(*) as demand_count, sum(taxamount) as demand_totalamount, sum(collectionamount) as Demand_Collected from egbs_demand_v1 demand inner join eg_ws_connection ws on demand.consumercode=ws.connectionno inner join egbs_demanddetail_v1 demanddetail on demand.id=demanddetail.demandid and ((demand.createdtime)/1000)>=cast(((extract(epoch from now()))-604800*3) as bigint) and (demand.createdtime/1000)<cast(((extract(epoch from now()))-604800*2) as bigint) where demand.status='ACTIVE' and demand.businessservice='WS';"
    
    
    queryWeek3 = pd.read_sql_query(impactQueryWeek3, conn)
    dataWeek3 = pd.DataFrame(queryWeek3)
    
    
    #Total number of payments   
    noOfPaymentsWeek3="select 'Last Third Week' as Period,count(*) as total_payments,COALESCE(sum(pd.amountpaid),0) as total_paid_amount from egcl_payment p join egcl_paymentdetail pd on p.id = pd.paymentid where pd.businessservice = 'WS' and (pd.createdtime/1000)>=cast(((extract(epoch from now()))-604800*3) as bigint) and (pd.createdtime/1000)<cast(((extract(epoch from now()))-604800*2) as bigint)"
    noOfPaymentsQueryWeek3=pd.read_sql_query(noOfPaymentsWeek3,conn)
    noOfPaymentsDataWeek3=pd.DataFrame(noOfPaymentsQueryWeek3)
    
    #Adding the number of consumers   
    noOfConsumersWeek3="select 'Last Third Week' as Period, count(*), count(distinct tenantid) as tenantids from eg_ws_connection where (createdtime/1000)>=cast(((extract(epoch from now()))-604800*3) as bigint) and (createdtime/1000)<cast(((extract(epoch from now()))-604800*2) as bigint)"
    noOfConsumersQueryWeek3=pd.read_sql_query(noOfConsumersWeek3,conn)
    noOfConsumersDataWeek3=pd.DataFrame(noOfConsumersQueryWeek3)

    
    dataWeek3.columns=['Period', 'Demand Count','Total Demand Amount', 'Demand Collected']
    
    noOfPaymentsDataWeek3.columns=['Period', 'Total No. of Payments', 'Total Payment Amount Collected']
    noOfConsumersDataWeek3.columns=['Period', 'Total No. of consumers', 'Total Tenants used']
    
    
    impactdataWeek3 = pd.DataFrame()
    
    impactdataWeek3=pd.merge(dataWeek3,noOfPaymentsDataWeek3,left_on='Period',right_on='Period',how='left')
    impactdataWeek3=pd.merge(impactdataWeek3,noOfConsumersDataWeek3,left_on='Period',right_on='Period',how='left')
    
    
    #impactdata = impactdata.append(impactdataWeek3, ignore_index = True)
    
    #print("Data 3\n",impactdataWeek3)
    #print("Data 3\n",impactdata)
    
    #####################################
    #Last Fourth Week
    impactQueryWeek4="SELECT 'Last Fourth Week' as Period, count(*) as demand_count, sum(taxamount) as demand_totalamount, sum(collectionamount) as Demand_Collected from egbs_demand_v1 demand inner join eg_ws_connection ws on demand.consumercode=ws.connectionno inner join egbs_demanddetail_v1 demanddetail on demand.id=demanddetail.demandid and ((demand.createdtime)/1000)>=cast(((extract(epoch from now()))-604800*4) as bigint) and (demand.createdtime/1000)<cast(((extract(epoch from now()))-604800*3) as bigint) where demand.status='ACTIVE' and demand.businessservice='WS';"
    
    queryWeek4 = pd.read_sql_query(impactQueryWeek4, conn)
    dataWeek4 = pd.DataFrame(queryWeek4)
    
    
    #Total number of payments   
    noOfPaymentsWeek4="select 'Last Fourth Week' as Period,count(*) as total_payments,COALESCE(sum(pd.amountpaid),0) as total_paid_amount from egcl_payment p join egcl_paymentdetail pd on p.id = pd.paymentid where pd.businessservice = 'WS' and (pd.createdtime/1000)>=cast(((extract(epoch from now()))-604800*4) as bigint) and (pd.createdtime/1000)<cast(((extract(epoch from now()))-604800*3) as bigint)"
    noOfPaymentsQueryWeek4=pd.read_sql_query(noOfPaymentsWeek4,conn)
    noOfPaymentsDataWeek4=pd.DataFrame(noOfPaymentsQueryWeek4)
    
    #Adding the number of consumers   
    noOfConsumersWeek4="select 'Last Fourth Week' as Period, count(*), count(distinct tenantid) as tenantids from eg_ws_connection where (createdtime/1000)>=cast(((extract(epoch from now()))-604800*4) as bigint) and (createdtime/1000)<cast(((extract(epoch from now()))-604800*3) as bigint)"
    noOfConsumersQueryWeek4=pd.read_sql_query(noOfConsumersWeek4,conn)
    noOfConsumersDataWeek4=pd.DataFrame(noOfConsumersQueryWeek4)

    
    dataWeek4.columns=['Period', 'Demand Count','Total Demand Amount', 'Demand Collected']
    
    #, 'From Date', 'To Date'
    
    noOfPaymentsDataWeek4.columns=['Period', 'Total No. of Payments', 'Total Payment Amount Collected']
    noOfConsumersDataWeek4.columns=['Period', 'Total No. of consumers', 'Total Tenants used']
    
    
    impactdataWeek4 = pd.DataFrame()
    
    impactdataWeek4=pd.merge(dataWeek4,noOfPaymentsDataWeek4,left_on='Period',right_on='Period',how='left')
    impactdataWeek4=pd.merge(impactdataWeek4,noOfConsumersDataWeek4,left_on='Period',right_on='Period',how='left')
    
    
    #impactdata = impactdata.append(impactdataWeek4, ignore_index = True)
    
    #print("Data 3\n",impactdataWeek4)
    #print("Data 3\n",impactdata)
    
    impactdataWeek4 = impactdataWeek4.append(impactdataWeek3, ignore_index = True)
    impactdataWeek4 = impactdataWeek4.append(impactdataWeek2, ignore_index = True)
    impactdataWeek4 = impactdataWeek4.append(impactdata, ignore_index = True)
    
    
    
    
    #####################################
    #Monthly data
    impactMonthlyQuery="SELECT 'Total Till Date' as Period, count(*) as demand_count, sum(taxamount) as demand_totalamount, sum(collectionamount) as Demand_Collected from egbs_demand_v1 demand inner join eg_ws_connection ws on demand.consumercode=ws.connectionno inner join egbs_demanddetail_v1 demanddetail on demand.id=demanddetail.demandid and (demand.createdtime/1000)>=cast(((extract(epoch from now()))-604800*4) as bigint) where demand.status='ACTIVE' and demand.businessservice='WS';"
    
    query = pd.read_sql_query(impactMonthlyQuery, conn)
    dataMonth = pd.DataFrame(query)
    
    
    #Total number of payments   
    noOfPaymentsMonthly="select 'Total Till Date' as Period, count(*) as total_payments,COALESCE(sum(pd.amountpaid),0) as total_paid_amount from egcl_payment p join egcl_paymentdetail pd on p.id = pd.paymentid where pd.businessservice = 'WS' and (pd.createdtime/1000)>=cast(((extract(epoch from now()))-(604800*4)) as bigint)"
    noOfPaymentsQueryMonth=pd.read_sql_query(noOfPaymentsMonthly,conn)
    noOfPaymentsDataMonth=pd.DataFrame(noOfPaymentsQueryMonth)
    #print(noOfPaymentsDataMonth)
    
    #Adding the number of consumers   
    noOfConsumersMonth="select 'Total Till Date' as Period, count(*), count(distinct tenantid) as tenantids from eg_ws_connection where (createdtime/1000)>=cast(((extract(epoch from now()))-(604800*4)) as bigint)"
    noOfConsumersQueryMonth=pd.read_sql_query(noOfConsumersMonth,conn)
    noOfConsumersDataMonth=pd.DataFrame(noOfConsumersQueryMonth)
    #print(noOfConsumersDataMonth)
    
    dataMonth.columns=['Period', 'Demand Count', 'Total Demand Amount', 'Demand Collected']
    
    noOfPaymentsDataMonth.columns=['Period', 'Total No. of Payments', 'Total Payment Amount Collected']
    noOfConsumersDataMonth.columns=['Period', 'Total No. of consumers', 'Total Tenants used']
    
    
    impactdataMonth = pd.DataFrame()
    
    impactdataMonth=pd.merge(dataMonth,noOfPaymentsDataMonth,left_on='Period',right_on='Period',how='left')
    impactdataMonth=pd.merge(impactdataMonth,noOfConsumersDataMonth,left_on='Period',right_on='Period',how='left')
    #print(impactdataMonth)
    
    #impactdata = impactdata.append(impactdataMonth, ignore_index = True)
    impactdata = impactdataWeek4.append(impactdataMonth, ignore_index = True)
    
    ############################################
    print(impactdataWeek4)
    impactdata=impactdata.fillna('N/A')
    impactdata['Total No. of Payments']=impactdata['Total No. of Payments'].replace('N/A', '0')
    impactdata['Total Payment Amount Collected']=impactdata['Total Payment Amount Collected'].replace('N/A', '0')
    impactdata['Total No. of consumers']=impactdata['Total No. of consumers'].replace('N/A', '0')
    impactdata['Total Tenants used']=impactdata['Total Tenants used'].replace('N/A', '0')   
    impactdata['Total Payment Amount Collected']=impactdata['Total Payment Amount Collected'].apply(pd.to_numeric, errors='ignore')
   
    impactdata['Total Demand Amount']=impactdata['Total Demand Amount'].apply(pd.to_numeric, errors='ignore')
    
    impactdata['Demand Count']=impactdata['Demand Count'].apply(pd.to_numeric)
    impactdata['Total No. of Payments']=impactdata['Total No. of Payments'].apply(pd.to_numeric)
    impactdata['Total No. of consumers']=impactdata['Total No. of consumers'].apply(pd.to_numeric)
    impactdata['Total Tenants used']=impactdata['Total Tenants used'].apply(pd.to_numeric)
    #impactdata['tenantid']=impactdata['tenantid'].str.replace("pb.", "",1).str.title()
    #impactdata=impactdata.set_index('Period').transpose()
    impactdata=impactdata.transpose()
    
    #Removing the index row from the transposed data
    #impactdata.columns = impactdata.iloc[0]
    #impactdata = impactdata[1:]
    
    #print(impactdata)
    global uniquetenant
    #uniquetenant = impactdata['tenantid'].unique()
    global accesstoken
    accesstoken = accessToken()
    global localitydict
    localitydict={}
    
    impactdata.fillna('', inplace=True)
    #print(impactdata)
    fileName = "impact_datamart_periodic-report_" + datetime.datetime.now().strftime("%d%m%Y") + ".csv"
    fileLocation = "/datamart/BiharDatamart/periodic-report/" + fileName
    impactdata.to_csv(fileLocation,header = None)
    print("Datamart exported. Please copy it using kubectl cp command to your required location.")
    conn.close()
    print("Connection closed!")


def accessToken():
    query = {'username':'8989898989','password':'eGov@123','userType':'EMPLOYEE',"scope":"read","grant_type":"password"}
    query['tenantId']='pb'
    response = requests.post("https://www.peyjalbihar.org/user/oauth/token", data=query, headers={
        "Connection":"keep-alive","content-type":"application/x-www-form-urlencoded", "origin":"https://www.peyjalbihar.org/","Authorization": "Basic ZWdvdi11c2VyLWNsaWVudDo="})
    print(response)
    jsondata = response.json()
    return jsondata.get('access_token')



def enrichLocality(tenantid,locality):
    if tenantid in localitydict:
        if localitydict[tenantid]=='':
            return ''
        elif locality in localitydict[tenantid]:
            return localitydict[tenantid][locality]
        else:
            return ''
    else:
        return ''
        
            
def dateToEpoch(dateString):
     return str(parser.parse(dateString).timestamp() * 1000)

if __name__ == '__main__':
   connect()


