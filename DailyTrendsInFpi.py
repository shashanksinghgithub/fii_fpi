import requests 
import pandas as pd
from bs4 import BeautifulSoup
#from tabulate import tabulate
import re,os,datetime,time,logging,sys
from lxml import html
from cassandra.cluster import Cluster
from dateutil.parser import parse
import numpy as np
import redis 

os.chdir("D:\\Data_dumpers\\FII_PI\\DailyTrendsInFPI\\")


cassandra_host = "172.17.9.51"
email_dir = "D:\\Emails\\Output\\"
redis_host = 'localhost'
master_dir = "D:\\Data_dumpers\\Master\\"



def dateparse(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date


# read holiday master
holiday_master = pd.read_csv(master_dir+'Holidays_2019.txt', delimiter=',',
                                date_parser=dateparse, parse_dates={'date':[0]})   
    
holiday_master['date'] = holiday_master.apply(lambda row: row['date'].date(), axis=1) 



    
def dateparse(row):
    '''Func to parse dates while reading ticker files'''
    d = row.split("+")[0]
    d = pd.to_datetime(d, format='%Y-%m-%d %H:%M:%S')
    return d

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def cassandra_configs_cluster():
    f = open(master_dir+"config.txt",'r').readlines()
    f = [ str.strip(config.split("cassandra,")[-1].split("=")[-1]) for config in f if config.startswith("cassandra")]  
          
    from cassandra.auth import PlainTextAuthProvider

    auth_provider= PlainTextAuthProvider(username=f[1],password=f[2])
    cluster = Cluster([f[0]], auth_provider=auth_provider)
    
    return cluster


def process_run_check(d):
    '''Func to check if the process should run on current day or not'''
    # check if working day or not 
    if len(holiday_master[holiday_master['date']==d])==0:
        return 1
    
    elif len(holiday_master[holiday_master['date']==d])==1:
        logging.info('Holiday: skip for current date :{} '.format(d))
        return -1




def fetch_factualnumbers2(todaydate,previousdate):
    
    # create python cluster object to connect to your cassandra cluster (specify ip address of nodes to connect within your cluster)
    #cluster = Cluster([cassandra_host])
    cluster = cassandra_configs_cluster()

    #logging.info('Cassandra Cluster connected...')
    # connect to your keyspace and create a session using which u can execute cql commands 
    session = cluster.connect('rohit')
    #logging.info('Using test_df keyspace')
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    # CREATE A TABLE; dump bhavcopies to this table
    #query=session.execute("select * from test_df.test_bhavcopy limit 40000")
    logging.info("Reading data from cassandra")
    query=session.execute(
            "SELECT * FROM rohit.equity_debt_fpi WHERE reportingdate <= '{}' AND reportingdate >= '{}' ALLOW FILTERING".format(
                    todaydate,previousdate)) 
    result = query._current_rows
    
    return result

def get_subtotal_total(gf):
    
    gf.sort_values(by="reportingdate",inplace=True)
    datelist=list(gf["reportingdate"].unique())
    
    sub_tot=gf.groupby("reportingdate")
    
    
    equity_g_p_subt=[]
    equity_g_s_subt=[]
    equity_n_i_subt=[]
    equity_n_i_m_subt=[]
    
    debt_g_p_subt=[]
    debt_g_s_subt=[]
    debt_n_i_subt=[]
    debt_n_i_m_subt=[]

    hybrid_g_p_subt=[]
    hybrid_g_s_subt=[]
    hybrid_n_i_subt=[]
    hybrid_n_i_m_subt=[]

    g_p_t=[]
    g_s_t=[]
    n_i_t=[]
    n_i_m_t=[]

    for i in range(0,len(datelist)):
        
        df=sub_tot.get_group(datelist[i])
        df.reset_index(drop=True,inplace=True)
        
        
        
        debt_g_p=df["debt_p_m_gp"]+df["debt_s_e_gp"]
        debt_g_s=df["debt_p_m_gs"]+df["debt_s_e_gs"]
        debt_n_i=df["debt_p_m_net_crore"]+df["debt_s_e_net_crore"]
        debt_n_i_m=df["debt_p_m_net_million"]+df["debt_s_e_net_million"]
    
        equity_g_p=df["equity_p_m_gp"]+df["equity_s_e_gp"]
        equity_g_s=df["equity_p_m_gs"]+df["equity_s_e_gs"]
        equity_n_i=df["equity_p_m_net_crore"]+df["equity_s_e_net_crore"]
        equity_n_i_m=df["equity_p_m_net_million"]+df["equity_s_e_net_million"]

        hybrid_g_p=df["hybrid_p_m_gp"]+df["hybrid_s_e_gp"]
        hybrid_g_s=df["hybrid_p_m_gs"]+df["hybrid_s_e_gs"]
        hybrid_n_i=df["hybrid_p_m_net_crore"]+df["hybrid_s_e_net_crore"]
        hybrid_n_i_m=df["hybrid_p_m_net_million"]+df["hybrid_s_e_net_million"]

        p_t=debt_g_p+equity_g_p+hybrid_g_p
        s_t=debt_g_s+equity_g_s+hybrid_g_s
        i_t=debt_n_i+equity_n_i+hybrid_n_i
        m_t=debt_n_i_m+equity_n_i_m+hybrid_n_i_m
    
        debt_g_p_subt.append(debt_g_p)
        debt_g_s_subt.append(debt_g_s)
        debt_n_i_subt.append(debt_n_i)
        debt_n_i_m_subt.append(debt_n_i_m)
    
        equity_g_p_subt.append(equity_g_p)
        equity_g_s_subt.append(equity_g_s)
        equity_n_i_subt.append(equity_n_i)
        equity_n_i_m_subt.append(equity_n_i_m)

        hybrid_g_p_subt.append(hybrid_g_p)
        hybrid_g_s_subt.append(hybrid_g_s)
        hybrid_n_i_subt.append(hybrid_n_i)
        hybrid_n_i_m_subt.append(hybrid_n_i_m)

        g_p_t.append(p_t)
        g_s_t.append(s_t)
        n_i_t.append(i_t)
        n_i_m_t.append(m_t)
    
    date=pd.DataFrame(datelist)
    debt_g_p_subt=pd.DataFrame(debt_g_p_subt)
    debt_g_s_subt=pd.DataFrame(debt_g_s_subt)
    debt_n_i_subt=pd.DataFrame(debt_n_i_subt)
    debt_n_i_m_subt=pd.DataFrame(debt_n_i_m_subt)

    equity_g_p_subt=pd.DataFrame(equity_g_p_subt)
    equity_g_s_subt=pd.DataFrame(equity_g_s_subt)
    equity_n_i_subt=pd.DataFrame(equity_n_i_subt)
    equity_n_i_m_subt=pd.DataFrame(equity_n_i_m_subt)

    hybrid_g_p_subt=pd.DataFrame(hybrid_g_p_subt)
    hybrid_g_s_subt=pd.DataFrame(hybrid_g_s_subt)
    hybrid_n_i_subt=pd.DataFrame(hybrid_n_i_subt)
    hybrid_n_i_m_subt=pd.DataFrame(hybrid_n_i_m_subt)

    g_p_t=pd.DataFrame(g_p_t)
    g_s_t=pd.DataFrame(g_s_t)
    n_i_t=pd.DataFrame(n_i_t)
    n_i_m_t=pd.DataFrame(n_i_m_t)

    final_df=[date,debt_g_p_subt,debt_g_s_subt,debt_n_i_subt,debt_n_i_m_subt,
             equity_g_p_subt,equity_g_s_subt,equity_n_i_subt,equity_n_i_m_subt,
             hybrid_g_p_subt,hybrid_g_s_subt,hybrid_n_i_subt,hybrid_n_i_m_subt,
             g_p_t,g_s_t,n_i_t,n_i_m_t]

    final_result = reduce(lambda left,right: pd.merge(left,right,left_index=True,right_index=True), final_df)
    final_result.columns=["date","debt_g_p_subt","debt_g_s_subt","debt_n_i_subt","debt_n_i_m_subt",
             "equity_g_p_subt","equity_g_s_subt","equity_n_i_subt","equity_n_i_m_subt",
             "hybrid_g_p_subt","hybrid_g_s_subt","hybrid_n_i_subt","hybrid_n_i_m_subt",
             "g_p_t","g_s_t","n_i_t","n_i_m_t"]

    final_result.reset_index(drop=True,inplace=True)
    final_result.rename(columns={"date":"reportingdate"},inplace=True)

    return final_result

def main(nd):
    
    
    d=datetime.datetime.now().date()-datetime.timedelta(days=nd)
    print (d)
    if process_run_check(d)== -1:
        return -1
    
    r = redis.Redis(host=redis_host, port=6379)     
    Dailytrendsfpi_flag = int(r.get("Dailytrendsfpi_flag")) if r.get("Dailytrendsfpi_flag")!=None else 0
    
    while datetime.datetime.now().time() >= datetime.time(17,30) and Dailytrendsfpi_flag!=1 :
        print 'Sleep for 5 min'
        time.sleep(300)
        Dailytrendsfpi_flag = int(r.get("Dailytrendsfpi_flag")) if r.get("Dailytrendsfpi_flag")!=None else 0
        
    

    prev = d - datetime.timedelta(days=30)
    data_e_d_h=fetch_factualnumbers2(d,prev)
    
    cal_data_e_d_h=get_subtotal_total(data_e_d_h)
    
    
    final_df=pd.merge(data_e_d_h,cal_data_e_d_h,on="reportingdate",how='left')
    
    colrename=pd.DataFrame(list(final_df.columns))
    colrename.columns=["rename"]
    colrename["rename"]= colrename["rename"].str.replace('p_m_gp','primary_market&others_Gross_Purchases(Rs.Crore)') 
    colrename["rename"]= colrename["rename"].str.replace('p_m_gs','primary_market&others_Gross_Sales(Rs.Crore)') 
    colrename["rename"]= colrename["rename"].str.replace('p_m_net_crore','primary_market&others_Net_Investment(Rs.Crore)') 
    colrename["rename"]= colrename["rename"].str.replace('p_m_net_million','primary_market&others_Net_Investment_US($)million') 
    colrename["rename"]= colrename["rename"].str.replace('s_e_gp','Stock_Exchange_Gross_Purchases(Rs.Crore)') 
    colrename["rename"]= colrename["rename"].str.replace('s_e_gs','Stock_Exchange_Gross_Sales(Rs.Crore)') 
    colrename["rename"]= colrename["rename"].str.replace('s_e_net_crore','Stock_Exchange_Net_Investment(Rs.Crore)')   
    colrename["rename"]= colrename["rename"].str.replace('s_e_net_million','Stock_Exchange_Net_Investment_US($)million') 
    colrename["rename"]= colrename["rename"].str.replace('g_p_subt','Gross_Purchases(Rs.Crore)_subtotal') 
    colrename["rename"]= colrename["rename"].str.replace('g_s_subt','Gross_Sales(Rs.Crore)_subtotal') 
    colrename["rename"]= colrename["rename"].str.replace('n_i_subt','Net_Investment(Rs.Crore)_subtotal') 
    colrename["rename"]= colrename["rename"].str.replace('n_i_m_subt','Net_Investment_US($)million_subtotal') 
    colrename["rename"]= colrename["rename"].str.replace('g_p_t','Gross_Purchases(Rs.Crore)_total') 
    colrename["rename"]= colrename["rename"].str.replace('g_p_t','Gross_Purchases(Rs.Crore)_total') 
    colrename["rename"]= colrename["rename"].str.replace('g_s_t','Gross_Sales(Rs.Crore)_total') 
    colrename["rename"]= colrename["rename"].str.replace('n_i_t','Net_Investment(Rs.Crore)_total') 
    colrename["rename"]= colrename["rename"].str.replace('n_i_m_t','Net_Investment_US($)million_total') 
    
    final_columns=list(colrename["rename"].unique())
    final_df.columns=final_columns
    
    final_df_excel=final_df[['reportingdate',
                             'debt_primary_market&others_Gross_Purchases(Rs.Crore)',
                             'debt_primary_market&others_Gross_Sales(Rs.Crore)',
                             'debt_primary_market&others_Net_Investment(Rs.Crore)',
                             'debt_primary_market&others_Net_Investment_US($)million',
                             'debt_Stock_Exchange_Gross_Purchases(Rs.Crore)',
                             'debt_Stock_Exchange_Gross_Sales(Rs.Crore)',
                             'debt_Stock_Exchange_Net_Investment(Rs.Crore)',
                             'debt_Stock_Exchange_Net_Investment_US($)million',
                             'debt_Gross_Purchases(Rs.Crore)_subtotal',
                             'debt_Gross_Sales(Rs.Crore)_subtotal',
                             'debt_Net_Investment(Rs.Crore)_subtotal',
                             'debt_Net_Investment_US($)million_subtotal',                  
                             'equity_primary_market&others_Gross_Purchases(Rs.Crore)',
                             'equity_primary_market&others_Gross_Sales(Rs.Crore)',
                             'equity_primary_market&others_Net_Investment(Rs.Crore)',
                             'equity_primary_market&others_Net_Investment_US($)million',
                             'equity_Stock_Exchange_Gross_Purchases(Rs.Crore)',
                             'equity_Stock_Exchange_Gross_Sales(Rs.Crore)',
                             'equity_Stock_Exchange_Net_Investment(Rs.Crore)',
                             'equity_Stock_Exchange_Net_Investment_US($)million',
                             'equity_Gross_Purchases(Rs.Crore)_subtotal',
                             'equity_Gross_Sales(Rs.Crore)_subtotal',
                             'equity_Net_Investment(Rs.Crore)_subtotal',
                             'equity_Net_Investment_US($)million_subtotal',                
                             'hybrid_primary_market&others_Gross_Purchases(Rs.Crore)',
                             'hybrid_primary_market&others_Gross_Sales(Rs.Crore)',
                             'hybrid_primary_market&others_Net_Investment(Rs.Crore)',
                             'hybrid_primary_market&others_Net_Investment_US($)million',
                             'hybrid_Stock_Exchange_Gross_Purchases(Rs.Crore)',
                             'hybrid_Stock_Exchange_Gross_Sales(Rs.Crore)',
                             'hybrid_Stock_Exchange_Net_Investment(Rs.Crore)',
                             'hybrid_Stock_Exchange_Net_Investment_US($)million',
                             'hybrid_Gross_Purchases(Rs.Crore)_subtotal',
                             'hybrid_Gross_Sales(Rs.Crore)_subtotal',
                             'hybrid_Net_Investment(Rs.Crore)_subtotal',
                             'hybrid_Net_Investment_US($)million_subtotal',
                             'Gross_Purchases(Rs.Crore)_total',
                             'Gross_Sales(Rs.Crore)_total',
                             'Net_Investment(Rs.Crore)_total',
                             'Net_Investment_US($)million_total']][:]
    
    final_report=final_df_excel.T
    final_report = final_report.round(2)
    
    final_report.to_excel(email_dir+"DailyTrendsInFPI_{}.xlsx".format(d),header=None)
    r.set("Dailytrendsfpi_flag",0)
       
    
    
    

main(0)