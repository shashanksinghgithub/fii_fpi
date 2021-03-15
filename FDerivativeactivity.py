import pandas as pd 
import numpy as np
import requests 
import re
import os
import datetime
from cassandra.cluster import Cluster
import redis
from dateutil.parser import parse
import time
import logging


cassandra_host = "172.17.9.51"
redis_host = 'localhost'
os.chdir("D:\\Data_dumpers\\FII_PI\\FII_derivative_activity")
master_dir="D:\\Data_dumpers\\Master\\"
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
processed_dir = "D:\\Data_dumpers\\FII_PI\\FII_derivative_activity\\processed\\"

logging.basicConfig(filename='test.log',
                        level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s")


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def cassandra_configs_cluster():
    f = open(master_dir+"config.txt",'r').readlines()
    f = [ str.strip(config.split("cassandra,")[-1].split("=")[-1]) for config in f if config.startswith("cassandra")]  
          
    from cassandra.auth import PlainTextAuthProvider

    auth_provider= PlainTextAuthProvider(username=f[1],password=f[2])
    cluster = Cluster([f[0]], auth_provider=auth_provider)
    
    return cluster


#cluster = Cluster([cassandra_host])
cluster = cassandra_configs_cluster()
logging.info('Cassandra Cluster connected...')
# connect to your keyspace and create a session using which u can execute cql commands 
session = cluster.connect('rohit')
logging.info('Using rohit keyspace')


def dateparse(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date


# read holiday master
holiday_master = pd.read_csv(master_dir+'Holidays_2019.txt', delimiter=',',
                                 date_parser=dateparse, parse_dates={'date':[0]})    
holiday_master['date'] = holiday_master.apply(lambda row: row['date'].date(), axis=1) 
        
def process_run_check(d):
    '''Func to check if the process should run on current day or not'''
   
    # check if working day or not 
    if len(holiday_master[holiday_master['date']==d])==0:
        print "working day wait file is getting downloaded"
        return 1
    
    elif len(holiday_master[holiday_master['date']==d])==1:
        logging.info('Holiday: skip for current date :{} '.format(d))
        print ('Holiday: skip for current date :{} '.format(d))        
        return -1

def previous_working_day(d):
    d = d - datetime.timedelta(days=1)
    while  True:
            if d in holiday_master["date"].values:
                d = d - datetime.timedelta(days=1)   
            else:
                return d


def FDerivativeactivity(nd):
    
    d=datetime.datetime.now().date() - datetime.timedelta(days=nd)
    
    if process_run_check(d)== -1:
        return -1
    d1=datetime.datetime.strftime(d,'%d-%b-%Y')    
    
    while True:
        
        print 'Sleep for 5 min'
        time.sleep(300)
            
        try:
            print 'https://archives.nseindia.com/content/fo/fii_stats_{}.xls'.format(d1)
            filedata = requests.get('https://archives.nseindia.com/content/fo/fii_stats_{}.xls'.format(d1), headers=headers)  
        except Exception as e:
            print e
            continue
        
        
        if filedata.status_code==200:
            print "success"
            
            output=open("test.xls","wb")
            output.write(filedata.content)
            output.close()
            print "status",filedata.status_code
                
            df=pd.read_excel("test.xls")
            df.drop(df.index[6:],inplace=True)
            d=str(df.columns[0].split('FOR')[1])
            df.drop(df.index[0],inplace=True)
            session.execute("CREATE TABLE IF NOT EXISTS FDerivativeactivity(reportingdate DATE,idx_fut_buy_contracts FLOAT,idx_fut_buy_crore FLOAT,idx_fut_sell_contracts FLOAT,idx_fut_sell_crore FLOAT,idx_fut_openint_contracts FLOAT,idx_fut_openint_crore FLOAT,idx_opt_buy_contracts FLOAT,idx_opt_buy_crore FLOAT,idx_opt_sell_contracts FLOAT,idx_opt_sell_crore FLOAT,idx_opt_openint_contracts FLOAT,idx_opt_openint_crore FLOAT,stk_fut_buy_contracts FLOAT,stk_fut_buy_crore FLOAT,stk_fut_sell_contracts FLOAT,stk_fut_sell_crore FLOAT,stk_fut_openint_contracts FLOAT,stk_fut_openint_crore FLOAT,stk_opt_buy_contracts FLOAT,stk_opt_buy_crore FLOAT,stk_opt_sell_contracts FLOAT,stk_opt_sell_crore FLOAT,stk_opt_openint_contracts FLOAT,stk_opt_openint_crore FLOAT,PRIMARY KEY(reportingdate))")
            reportingdate=d 
            idx_fut_buy_contracts=df['Unnamed: 1'][2]
            idx_fut_buy_crore=df['Unnamed: 2'][2]
            idx_fut_sell_contracts=df['Unnamed: 3'][2]
            idx_fut_sell_crore=df['Unnamed: 4'][2]
            idx_fut_openint_contracts=df['Unnamed: 5'][2]
            idx_fut_openint_crore=df['Unnamed: 6'][2]
            
            idx_opt_buy_contracts=df['Unnamed: 1'][3]
            idx_opt_buy_crore=df['Unnamed: 2'][3]
            idx_opt_sell_contracts=df['Unnamed: 3'][3]
            idx_opt_sell_crore=df['Unnamed: 4'][3]
            idx_opt_openint_contracts=df['Unnamed: 5'][3]
            idx_opt_openint_crore=df['Unnamed: 6'][3]
                
            stk_fut_buy_contracts=df['Unnamed: 1'][4]
            stk_fut_buy_crore=df['Unnamed: 2'][4]
            stk_fut_sell_contracts=df['Unnamed: 3'][4]
            stk_fut_sell_crore=df['Unnamed: 4'][4]
            stk_fut_openint_contracts=df['Unnamed: 5'][4]
            stk_fut_openint_crore=df['Unnamed: 6'][4]
            
            stk_opt_buy_contracts=df['Unnamed: 1'][5]
            stk_opt_buy_crore=df['Unnamed: 2'][5]
            stk_opt_sell_contracts=df['Unnamed: 3'][5]
            stk_opt_sell_crore=df['Unnamed: 4'][5]
            stk_opt_openint_contracts=df['Unnamed: 5'][5]
            stk_opt_openint_crore=df['Unnamed: 6'][5]
                
            
            df1=[reportingdate,idx_fut_buy_contracts,idx_fut_buy_crore,idx_fut_sell_contracts,idx_fut_sell_crore,idx_fut_openint_contracts,idx_fut_openint_crore,idx_opt_buy_contracts,idx_opt_buy_crore,idx_opt_sell_contracts,idx_opt_sell_crore,idx_opt_openint_contracts,idx_opt_openint_crore,stk_fut_buy_contracts,stk_fut_buy_crore,stk_fut_sell_contracts,stk_fut_sell_crore,stk_fut_openint_contracts,stk_fut_openint_crore,stk_opt_buy_contracts,stk_opt_buy_crore,stk_opt_sell_contracts,stk_opt_sell_crore,stk_opt_openint_contracts,stk_opt_openint_crore]
            final=pd.DataFrame(df1).T
                    
    
            final.columns=['reportingdate','idx_fut_buy_contracts','idx_fut_buy_crore','idx_fut_sell_contracts','idx_fut_sell_crore','idx_fut_openint_contracts','idx_fut_openint_crore','idx_opt_buy_contracts','idx_opt_buy_crore','idx_opt_sell_contracts','idx_opt_sell_crore','idx_opt_openint_contracts','idx_opt_openint_crore','stk_fut_buy_contracts','stk_fut_buy_crore','stk_fut_sell_contracts','stk_fut_sell_crore','stk_fut_openint_contracts','stk_fut_openint_crore','stk_opt_buy_contracts','stk_opt_buy_crore','stk_opt_sell_contracts','stk_opt_sell_crore','stk_opt_openint_contracts','stk_opt_openint_crore']
                
            final['reportingdate']=pd.to_datetime(final['reportingdate'])
                
            final.to_csv('FDerivativeactivity.csv',index=False)
            final.to_csv(processed_dir+'FDerivativeactivity_{}.csv'.format(d),index=False)
            os.system('FDerivativeactivity.bat')
            file = open("output.txt","w") 
            file.write("<html><head></head><body>")
            file.write('<P><b><font face="Times New Roman" size={}>----Data is successfully stored for FDerivativeActivity in cassandra-----</font></b></p>'.format(3.1))
            file.write('</tbody></table></body></html>')
            file.close()
            cluster.shutdown()  #shutdown open cassandra instance 
            os.remove('test.xls')
            os.remove('FDerivativeActivity.csv')
            logging.info('Parameters read from redis for FDerivativeActivity_flag')
            r = redis.Redis(host=redis_host, port=6379) 
            r.set('FDerivativeActivity_flag',1)
            r.set('FDerivativeActivity_flag1',1)
                
            break
                   



start_time = time.time()
if __name__ == '__main__':
    FDerivativeactivity(nd=0)      # set (nd = timedelta) days here    
end_time = time.time()


logging.info('Time taken to process :'.format(end_time - start_time))
print "Execution time: {0} Seconds.... ".format(end_time - start_time)