import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import os
from lxml import html
import datetime
import time
from cassandra.cluster import Cluster
from dateutil.parser import parse
import logging
from selenium import webdriver
import redis
#if chrome driver is not downloaded then do this
#from webdrivermanager import ChromeDriverManager
#gdd = ChromeDriverManager()
#print gdd.download_and_install()


#from selenium.webdriver.common.keys import Keys
#cassandra_host = "localhost"
#chrome_path = "D:\\Data_dumpers\\FII PI Actual numbers\\chromedriver.exe"
#chrome_path = "C:\\Users\\krishna\\AppData\\Local\\salabs_\\WebDriverManager\\bin\\chromedriver.exe"

cassandra_host = "172.17.9.51"
#cassandra_host = "localhost" 
server = '172.17.9.149'; port = 25
redis_host = 'localhost'
os.chdir("D:\\Data_dumpers\\FII_PI\\FII_Actual_Numbers")
processed_files = "D:\\Data_dumpers\\FII_PI\\FII_Actual_Numbers\\Processed_files\\"
master_dir = "D:\\Data_dumpers\\Master\\"

logging.basicConfig(filename='test.log',
                        level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s")



def dateparse(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date

# read holiday master
holiday_master = pd.read_csv(master_dir+'Holidays_2019.txt', delimiter=',',
                                date_parser=dateparse, parse_dates={'date':[0]})   
    
#holiday_master = pd.read_csv('C:\\Users\\devanshm\\Desktop\\devansh\\sebiscrapping\\Holidays_2019.txt', delimiter=',',
#                                 date_parser=dateparse, parse_dates={'date':[0]})  

holiday_master['date'] = holiday_master.apply(lambda row: row['date'].date(), axis=1) 
        
def process_run_check(d):
    '''Func to check if the process should run on current day or not'''
    # check if working day or not 
    if len(holiday_master[holiday_master['date']==d])==0:
        return 1
    
    elif len(holiday_master[holiday_master['date']==d])==1:
        logging.info('Holiday: skip for current date :{} '.format(d))
        return -1

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def cassandra_configs_cluster():
    f = open(master_dir+"config.txt",'r').readlines()
    f = [ str.strip(config.split("cassandra,")[-1].split("=")[-1]) for config in f if config.startswith("cassandra")]  
          
    from cassandra.auth import PlainTextAuthProvider

    auth_provider= PlainTextAuthProvider(username=f[1],password=f[2])
    cluster = Cluster([f[0]], auth_provider=auth_provider)
    
    return cluster

cluster = cassandra_configs_cluster()

#cluster = Cluster([cassandra_host])
logging.info('Cassandra Cluster connected...')
# connect to your keyspace and create a session using which u can execute cql commands 
#session = cluster.connect('rohit')
session = cluster.connect('rohit')
logging.info('Using rohit keyspace')

session.row_factory = pandas_factory
session.default_fetch_size = None

def FIIActualNumbers(nd):
    d=datetime.datetime.now().date()-datetime.timedelta(days=nd)
    print (d)
    if process_run_check(d)== -1:
        return -1
    #FIIActualNumbers_flag=int(FIIActualNumbers_flag)
    
    
    while True:
        
        logging.info('Parameters read from redis for FIIActualNumbers_flag')
        url = "https://www.fpi.nsdl.co.in/web/Reports/Latest.aspx"
        driver = webdriver.Chrome('chromedriver.exe')
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        print(soup.prettify())
        #soup = BeautifulSoup(res.content,'lxml')
    #    tree = html.parse("Latest (Daily Trends in FPI_FII Investments).html")
    #    raw_html=html.tostring(tree)
    #    soup = BeautifulSoup(raw_html, 'html.parser')
    #    numbers = soup.find_all("table", {"class": "tbls01"},text=True)#number
        table=soup.find_all('tr')
        td=soup.find_all('td')
        td_val=[]
        for i in range(len(td)):
            #print td[i].text
            td_val.append(td[i].text)
            
        th=soup.find_all('th')
        th_val=[]
        for i in range(len(th)):
            #print th[i].text
            th_val.append(th[i].text)
      
        # CREATE A TABLE; dump bhavcopies to this table
        session.execute("CREATE TABLE IF NOT EXISTS usd_inr_daily(reportingdate DATE,conversiontoinr FLOAT,PRIMARY KEY(reportingdate))")
        reporting_date=[td_val[0]]
        conversion_inr=[td_val[7]]
        f1_df=[reporting_date,conversion_inr]
        f1_df1=pd.DataFrame(f1_df).T
        f1_df1.rename(columns={0:'reportingdate',1:'conversiontoinr'},inplace=True)
        f1_df1['reportingdate'] =  pd.to_datetime(f1_df1['reportingdate'])
        f1_df1['reportingdate'] = f1_df1['reportingdate'].dt.date
        f1_df1['conversiontoinr']=f1_df1['conversiontoinr'].str.replace('Rs.',"").astype('float')
        if f1_df1["reportingdate"].values[0]==d:
            f1_df1.to_csv("FActualNumbers1.csv",index=False)
            f1_df1.to_csv(processed_files+"FActualNumbers1_{}.csv".format(d),index=False)
            os.system("FActualNumbers1.bat")
    
            session.execute("CREATE TABLE IF NOT EXISTS equity_debt_fpi(reportingdate DATE,equity_s_e_gp FLOAT,equity_s_e_gs FLOAT,equity_s_e_net_crore FLOAT,equity_s_e_net_million FLOAT,equity_p_m_gp FLOAT,equity_p_m_gs FLOAT,equity_p_m_net_crore FLOAT,equity_p_m_net_million FLOAT,debt_s_e_gp FLOAT,debt_s_e_gs FLOAT,debt_s_e_net_crore FLOAT,debt_s_e_net_million FLOAT,debt_p_m_gp FLOAT,debt_p_m_gs FLOAT,debt_p_m_net_crore FLOAT,debt_p_m_net_million FLOAT,hybrid_s_e_gp FLOAT,hybrid_s_e_gs FLOAT,hybrid_s_e_net_crore FLOAT,hybrid_s_e_net_million FLOAT,hybrid_p_m_gp FLOAT,hybrid_p_m_gs FLOAT,hybrid_p_m_net_crore FLOAT,hybrid_p_m_net_million FLOAT,PRIMARY KEY(reportingdate))")
            
            equity_s_e_gp=[td_val[3]]
            equity_s_e_gs=[td_val[4]]
            equity_s_e_net_crore=[re.sub('[(,)]+','-',td_val[5],1).strip(")")]
            equity_s_e_net_million=[re.sub('[(,)]+','-',td_val[6],1).strip(")")]
    
            equity_p_m_gp=[td_val[9]]
            equity_p_m_gs=[td_val[10]]
            equity_p_m_net_crore=[re.sub('[(,)]+','-',td_val[11],1).strip(")")]
            equity_p_m_net_million=[re.sub('[(,)]+','-',td_val[12],1).strip(")")]
            
            debt_s_e_gp=[td_val[20]]
            debt_s_e_gs=[td_val[21]]
            debt_s_e_net_crore=[re.sub('[(,)]+','-',td_val[22],1).strip(")")]
            debt_s_e_net_million=[re.sub('[(,)]+','-',td_val[23],1).strip(")")]
    
            debt_p_m_gp=[td_val[25]]
            debt_p_m_gs=[td_val[26]]
            debt_p_m_net_crore=[re.sub('[(,)]+','-',td_val[27],1).strip(")")]
            debt_p_m_net_million=[re.sub('[(,)]+','-',td_val[28],1).strip(")")]
    
            hybrid_s_e_gp=[td_val[36]]
            hybrid_s_e_gs=[td_val[37]]
            hybrid_s_e_net_crore=[re.sub('[(,)]+','-',td_val[38],1).strip(")")]
            hybrid_s_e_net_million=[re.sub('[(,)]+','-',td_val[39],1).strip(")")]
    
            hybrid_p_m_gp=[td_val[41]]
            hybrid_p_m_gs=[td_val[42]]
            hybrid_p_m_net_crore=[re.sub('[(,)]+','-',td_val[43],1).strip(")")]
            hybrid_p_m_net_million=[re.sub('[(,)]+','-',td_val[44],1).strip(")")]
    
            f2_df=[reporting_date,equity_s_e_gp ,equity_s_e_gs ,equity_s_e_net_crore ,equity_s_e_net_million ,equity_p_m_gp ,equity_p_m_gs ,equity_p_m_net_crore ,equity_p_m_net_million ,debt_s_e_gp ,debt_s_e_gs ,debt_s_e_net_crore ,debt_s_e_net_million ,debt_p_m_gp ,debt_p_m_gs ,debt_p_m_net_crore ,debt_p_m_net_million ,hybrid_s_e_gp ,hybrid_s_e_gs ,hybrid_s_e_net_crore ,hybrid_s_e_net_million ,hybrid_p_m_gp ,hybrid_p_m_gs ,hybrid_p_m_net_crore ,hybrid_p_m_net_million]
            f2_df1=pd.DataFrame(f2_df).T
            f2_df1.columns=['reportingdate','equity_s_e_gp','equity_s_e_gs','equity_s_e_net_crore','equity_s_e_net_million','equity_p_m_gp','equity_p_m_gs','equity_p_m_net_crore','equity_p_m_net_million','debt_s_e_gp','debt_s_e_gs','debt_s_e_net_crore','debt_s_e_net_million','debt_p_m_gp','debt_p_m_gs','debt_p_m_net_crore','debt_p_m_net_million','hybrid_s_e_gp','hybrid_s_e_gs','hybrid_s_e_net_crore','hybrid_s_e_net_million','hybrid_p_m_gp','hybrid_p_m_gs','hybrid_p_m_net_crore','hybrid_p_m_net_million']
            f2_df1['reportingdate'] =  pd.to_datetime(f2_df1['reportingdate'])
            f2_df1['reportingdate'] = f2_df1['reportingdate'].dt.date
            f2_df1.to_csv("FActualNumbers2.csv",index=False)
            f2_df1.to_csv(processed_files+"FActualNumbers2_{}.csv".format(d),index=False)
            os.system("FActualNumbers2.bat")
    
            session.execute("CREATE TABLE IF NOT EXISTS fno_fpi(reportingdate DATE,idx_fut_buy_contracts FLOAT,idx_fut_buy_crore FLOAT,idx_fut_sell_contracts FLOAT,idx_fut_sell_crore FLOAT,idx_fut_openint_contracts FLOAT,idx_fut_openint_crore FLOAT,idx_opt_buy_contracts FLOAT,idx_opt_buy_crore FLOAT,idx_opt_sell_contracts FLOAT,idx_opt_sell_crore FLOAT,idx_opt_openint_contracts FLOAT,idx_opt_openint_crore FLOAT,stk_fut_buy_contracts FLOAT,stk_fut_sell_contracts FLOAT,stk_fut_buy_crore FLOAT,stk_fut_sell_crore FLOAT,stk_fut_openint_contracts FLOAT,stk_fut_openint_crore FLOAT,stk_opt_buy_contracts FLOAT,stk_opt_buy_crore FLOAT,stk_opt_sell_contracts FLOAT,stk_opt_sell_crore FLOAT,stk_opt_openint_contracts FLOAT,stk_opt_openint_crore FLOAT,ir_fut_buy_contracts FLOAT,ir_fut_buy_crore FLOAT,ir_fut_sell_contracts FLOAT,ir_fut_sell_crore FLOAT,ir_fut_openint_contracts FLOAT,ir_fut_openint_crore FLOAT,PRIMARY KEY(reportingdate))")
            
            idx_fut_buy_contracts=[td_val[58]]
            idx_fut_buy_crore=[td_val[59]]
            idx_fut_sell_contracts=[td_val[60]]
            idx_fut_sell_crore=[td_val[61]]
            idx_fut_openint_contracts=[td_val[62]]
            idx_fut_openint_crore=[td_val[63]]
                
            idx_opt_buy_contracts=[td_val[65]]
            idx_opt_buy_crore=[td_val[66]]
            idx_opt_sell_contracts=[td_val[67]]
            idx_opt_sell_crore=[td_val[68]]
            idx_opt_openint_contracts=[td_val[69]]
            idx_opt_openint_crore=[td_val[70]]
    
            stk_fut_buy_contracts=[td_val[72]]
            stk_fut_sell_contracts=[td_val[73]]
            stk_fut_buy_crore=[td_val[74]]
            stk_fut_sell_crore=[td_val[75]]
            stk_fut_openint_contracts=[td_val[76]]
            stk_fut_openint_crore=[td_val[77]]
    
            stk_opt_buy_contracts=[td_val[79]]
            stk_opt_buy_crore=[td_val[80]]
            stk_opt_sell_contracts=[td_val[81]]
            stk_opt_sell_crore=[td_val[82]]
            stk_opt_openint_contracts=[td_val[83]]
            stk_opt_openint_crore=[td_val[84]]
            
            ir_fut_buy_contracts=[td_val[86]]
            ir_fut_buy_crore=[td_val[87]]
            ir_fut_sell_contracts=[td_val[88]]
            ir_fut_sell_crore=[td_val[89]]
            ir_fut_openint_contracts=[td_val[90]]
            ir_fut_openint_crore=[td_val[91]]
    
            f3_df=[reporting_date,idx_fut_buy_contracts ,idx_fut_buy_crore ,idx_fut_sell_contracts ,idx_fut_sell_crore ,idx_fut_openint_contracts ,idx_fut_openint_crore ,idx_opt_buy_contracts ,idx_opt_buy_crore ,idx_opt_sell_contracts ,idx_opt_sell_crore ,idx_opt_openint_contracts ,idx_opt_openint_crore ,stk_fut_buy_contracts ,stk_fut_sell_contracts ,stk_fut_buy_crore ,stk_fut_sell_crore ,stk_fut_openint_contracts ,stk_fut_openint_crore ,stk_opt_buy_contracts ,stk_opt_buy_crore ,stk_opt_sell_contracts ,stk_opt_sell_crore ,stk_opt_openint_contracts ,stk_opt_openint_crore ,ir_fut_buy_contracts ,ir_fut_buy_crore ,ir_fut_sell_contracts ,ir_fut_sell_crore ,ir_fut_openint_contracts ,ir_fut_openint_crore]
            f3_df1=pd.DataFrame(f3_df).T
            f3_df1.columns=['reportingdate','idx_fut_buy_contracts','idx_fut_buy_crore','idx_fut_sell_contracts','idx_fut_sell_crore','idx_fut_openint_contracts','idx_fut_openint_crore','idx_opt_buy_contracts','idx_opt_buy_crore','idx_opt_sell_contracts','idx_opt_sell_crore','idx_opt_openint_contracts','idx_opt_openint_crore','stk_fut_buy_contracts','stk_fut_sell_contracts','stk_fut_buy_crore','stk_fut_sell_crore','stk_fut_openint_contracts','stk_fut_openint_crore','stk_opt_buy_contracts','stk_opt_buy_crore','stk_opt_sell_contracts','stk_opt_sell_crore','stk_opt_openint_contracts','stk_opt_openint_crore','ir_fut_buy_contracts','ir_fut_buy_crore','ir_fut_sell_contracts','ir_fut_sell_crore','ir_fut_openint_contracts','ir_fut_openint_crore']
            f3_df1['reportingdate'] =  pd.to_datetime(f3_df1['reportingdate'])
            f3_df1['reportingdate'] = f3_df1['reportingdate'].dt.date
            f3_df1.to_csv("FActualNumbers3.csv",index=False)
            f3_df1.to_csv(processed_files+"FActualNumbers3_{}.csv".format(d),index=False)
            os.system("FActualNumbers3.bat ")
    #        file = open("output.txt","w") 
    #        file.write("<html><head></head><body>")
    #        file.write('<P><b><font face="Times New Roman" size={}>----Data is successfully stored for FIIActualNumbers in cassandra-----</font></b></p>'.format(3.1))
    #        file.write('</tbody></table></body></html>')
    #        file.close()
            cluster.shutdown()  #shutdown open cassandra instance 
            driver.close()  # close chrome driver instance 
            os.remove("FActualNumbers1.csv")
            os.remove("FActualNumbers2.csv")
            os.remove("FActualNumbers3.csv")
            r = redis.Redis(host=redis_host, port=6379) 
            r.set('FIIActualNumbers_flag',1)
            r.set('FIIActualNumbers_flag1',1)
            r.set('Dailytrendsfpi_flag',1)
            break
        else:
            print "Sleep for 4 min"
            time.sleep(250) 
            
    
FIIActualNumbers(nd=0)