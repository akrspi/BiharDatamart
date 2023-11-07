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
         

def map_propertytype(s):
    return  s.capitalize()

def map_propertySubType(s):
     return (s.replace('_',' ').capitalize())



def mapstate(s):
    return 'Odisha'


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

    impactQuery="SELECT demand.tenantid, count(distinct demand.id) as demand_count, sum(taxamount) as demand_totalamount, sum(collectionamount) as Demand_Collected from egbs_demand_v1 demand inner join eg_ws_connection ws on demand.consumercode=ws.connectionno inner join egbs_demanddetail_v1 demanddetail on demand.id=demanddetail.demandid where demand.status='ACTIVE' and demand.businessservice='WS'  and demand.tenantid not in('br.testing') group by demand.tenantid order by tenantid;"
    
    # to_char((to_timestamp(taxperiodfrom/1000)::timestamp  at time Zone 'Asia/Kolkata'), 'mm/dd/yyyy HH24:MI:SS') as taxperiodfrom, to_char((to_timestamp(taxperiodto/1000)::timestamp  at time Zone 'Asia/Kolkata'), 'mm/dd/yyyy HH24:MI:SS') as taxperiodto

    #starttime = input('Enter start date (dd-mm-yyyy): ')
    #endtime = input('Enter end date (dd-mm-yyyy): ')
    #fsmquery = fsmquery.replace('{START_TIME}',dateToEpoch(starttime))
    #fsmquery = fsmquery.replace('{END_TIME}',dateToEpoch(endtime))
    
    query = pd.read_sql_query(impactQuery, conn)
    data = pd.DataFrame(query)
    
    
    #Adding the number of consumers   
    noOfConsumers="select tenantid,count(*) from eg_ws_connection where status='Active' and tenantid not in('br.testing') group by tenantid"
    noOfConsumersQuery=pd.read_sql_query(noOfConsumers,conn)
    noOfConsumersData=pd.DataFrame(noOfConsumersQuery)

    #Total number of payments   
    noOfPayments="select p.tenantid as tenantid, count(*) as total_payments,sum(pd.amountpaid) as total_paid_amount from egcl_payment p join egcl_paymentdetail pd on p.id = pd.paymentid where pd.businessservice = 'WS' and p.tenantid not in('br.testing') group by p.tenantid"
    noOfPaymentsQuery=pd.read_sql_query(noOfPayments,conn)
    noOfPaymentsData=pd.DataFrame(noOfPaymentsQuery)
    
    
    
    data.columns=['tenantid','Demand Count','Total Demand Amount', 'Demand Collected']
    
    #, 'From Date', 'To Date'
    
    noOfPaymentsData.columns=['tenantid','Total No. of Payments', 'Total Payment Amount Collected']
    noOfConsumersData.columns=['tenantid','Total No. of consumers']
    
    
    impactdata = pd.DataFrame()
    
    impactdata=pd.merge(data,noOfPaymentsData,left_on='tenantid',right_on='tenantid',how='left')
    impactdata=pd.merge(impactdata,noOfConsumersData,left_on='tenantid',right_on='tenantid',how='left')
    
    impactdata=impactdata.fillna('N/A')
    impactdata['Total No. of Payments']=impactdata['Total No. of Payments'].replace('N/A', '0')
    impactdata['Total Payment Amount Collected']=impactdata['Total Payment Amount Collected'].replace('N/A', '0')
    impactdata['Total No. of consumers']=impactdata['Total No. of consumers'].replace('N/A', '0')
    #impactdata['Disposed Time']=impactdata['Disposed Time'].replace('N/A', '')
    
    #impactdata['No of Trips Completed']=impactdata['No of Trips Completed'].replace('N/A',0)
   
   
    #impactdata = impactdata.dropna(axis=0, subset=['Application Submitted Time'])
    
    #impactdata['Payment Status'] =impactdata['Payment Status'].map(map_paymentsource)
   
    impactdata['Total Payment Amount Collected']=impactdata['Total Payment Amount Collected'].apply(pd.to_numeric, errors='ignore')
   
    impactdata['Total Demand Amount']=impactdata['Total Demand Amount'].apply(pd.to_numeric, errors='ignore')
    
    impactdata['Demand Count']=impactdata['Demand Count'].apply(pd.to_numeric)
    impactdata['Total No. of Payments']=impactdata['Total No. of Payments'].apply(pd.to_numeric)
    impactdata['Total No. of consumers']=impactdata['Total No. of consumers'].apply(pd.to_numeric)
    impactdata['tenantid']=impactdata['tenantid'].str.replace("br.", "",1).str.title()
    
    global uniquetenant
    uniquetenant = impactdata['tenantid'].unique()
    global accesstoken
    accesstoken = accessToken()
    global localitydict
    localitydict={}
    #storeTenantValues()

    #impactdata['Locality'] = impactdata.apply(lambda x : enrichLocality(x.tenantid,x.Locality), axis=1)
    #impactdata = impactdata.drop(columns=['tenantid'])
    impactdata.fillna('', inplace=True)
    impactdata=impactdata.drop_duplicates(subset = ["tenantid"]).reset_index(drop=True)
    fileName = "impact_datamart_report_" + datetime.datetime.now().strftime("%d%m%Y") + ".csv"
    fileLocation = "/datamart/BiharDatamart/weekly-report/" + fileName
    impactdata.to_csv(fileLocation)
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


