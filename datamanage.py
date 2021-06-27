import tushare as ts
# import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
import pymysql
ts.set_token('808bf3dd5d9ecbad0130ffc842aa3338112ddbb389e91fa967240921')
pro = ts.pro_api()
import time

import datetime
import warnings
warnings.filterwarnings("ignore")



class datamanage():
    #     管理股票的价格数据，从tushare获取数据，如果已经有数据，就不不保存。

    def dbinit(self):
        self.dbconn = pymysql.connect(host='localhost', port=3306, user='root', password='751982leizhen',
                                      database='leizquant')
        self.dbcur = self.dbconn.cursor()
        self.dbcur.execute('''create table if not exists stocksdaily (ts_code varchar(20),trade_date date,openprice float,highprice float,	
                           lowprice float,closeprice float,pre_closeprice float,changeprice float,pct_chg float,vol float,amount float,
                           primary key(ts_code,trade_date))''')
        self.dbcur.execute('''create table if not exists stocklist (ts_code varchar(20) primary key,symbol	varchar(20),
                            name varchar(20),area varchar(20),industry varchar(20),list_date date)''')

    def __init__(self, startdate='20090101', enddate=datetime.datetime.today().strftime('%Y%m%d'), stocks=[]):
        self.startdate = startdate
        self.enddate = enddate
        self.stocks = stocks
        self.dbinit()
        #         获取股票列表
        self.stocklist = pro.stock_basic(exchange='', list_status='L',
                                         fields='ts_code,symbol,name,area,industry,list_date')
        #         time.sleep(2)

        #         将股票列表加入数据库
        for i in range(len(self.stocklist)):
            self.dbcur.execute(
                'insert ignore into stocklist(ts_code,symbol,name,area,industry,list_date) values(%s,%s,%s,%s,%s,%s)'
                , (self.stocklist.at[i, 'ts_code'], self.stocklist.at[i, 'symbol'], self.stocklist.at[i, 'name']
                   , self.stocklist.at[i, 'area'], self.stocklist.at[i, 'industry'], self.stocklist.at[i, 'list_date']))

        self.dbconn.commit()

    def __del__(self):
       self.dbconn.close()
        # pass

    def getstocksdaily(self):
        if len(self.stocks) == 0:
            self.stocks = list(self.stocklist['ts_code'])

        #         获取所有数据
        for stock in self.stocks:
            print('{} ===========>stock ts_code:{} fetching'.format(time.asctime( time.localtime(time.time()) ),stock))
            time.sleep(5)
            wrongtime=0
        
            try:
                stockdaily = pro.daily(ts_code=stock, start_date=self.startdate, end_date=self.enddate)
            except:
                print(stock, ', error')
                time.sleep(5)
                continue
            for i in range(len(stockdaily)):
                self.dbcur.execute('''insert ignore into stocksdaily(ts_code,trade_date,openprice,highprice,lowprice,closeprice,pre_closeprice,changeprice,pct_chg,vol,amount) 
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                                   (stockdaily.at[i, 'ts_code'], stockdaily.at[i, 'trade_date'],
                                    float(stockdaily.at[i, 'open']), float(stockdaily.at[i, 'high']),
                                    float(stockdaily.at[i, 'low']), float(stockdaily.at[i, 'close']),
                                    float(stockdaily.at[i, 'pre_close']), float(stockdaily.at[i, 'change']),
                                    float(stockdaily.at[i, 'pct_chg']), float(stockdaily.at[i, 'vol']),
                                    float(stockdaily.at[i, 'amount'])))
            #         提交数据库
            self.dbconn.commit()
            print('{} ===========>stock ts_code:{} got'.format( time.asctime( time.localtime(time.time()) ) ,stock))

    #     复权因子
    def getadjfactor(self):
        self.dbcur.execute(
            'create table if not exists adj_factor (ts_code varchar(20),trade_date date,adj_factor float,primary key(ts_code,trade_date))')

        if len(self.stocks) == 0:
            self.stocks = list(self.stocklist['ts_code'])

        #         获取所有数据
        for adj in self.stocks:
            print('{} ===========>adj ts_code:{} fetching'.format(time.asctime( time.localtime(time.time()) ),adj))
            time.sleep(5)
            try:
                stockadj = pro.adj_factor(ts_code=adj, start_date=self.startdate, end_date=self.enddate)
            except:
                print(adj, ', error')
                time.sleep(5)
                continue
            
            for i in range(len(stockadj)):
                self.dbcur.execute('''insert ignore into adj_factor(ts_code,trade_date,adj_factor) 
                                    values(%s,%s,%s)''',
                                   (stockadj.at[i, 'ts_code'], stockadj.at[i, 'trade_date'],
                                    float(stockadj.at[i, 'adj_factor'])))
            #         提交数据库
            self.dbconn.commit()
            print('{} ===========>adj ts_code:{} got'.format( time.asctime( time.localtime(time.time()) ) ,adj))

    def getindexdaily(self):
        # 指数代码
        indexlist=['000001.SH','399001.SZ','399006.SZ','000300.SH','000016.SH','000905.SH']
        self.dbcur.execute('''create table if not exists indexdaily (ts_code varchar(20),trade_date date,openprice float,highprice float,	
                           lowprice float,closeprice float,pre_closeprice float,changeprice float,pct_chg float,vol float,amount float,
                           primary key(ts_code,trade_date))''')
        for index  in indexlist:
            print('{} ===========>index ts_code:{} fetching'.format(time.asctime( time.localtime(time.time()) ),index))
            time.sleep(5)
            try:
                indexdaily = ts.pro_bar(ts_code=index, adj='qfq', start_date=self.startdate, end_date=self.enddate,freq='D',asset='I')
            except:
                print(index, ', error')
                time.sleep(5)
                continue
            for i in range(len(indexdaily)):
                self.dbcur.execute('''insert ignore into indexdaily(ts_code,trade_date,openprice,highprice,lowprice,closeprice,pre_closeprice,changeprice,pct_chg,vol,amount) 
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                                   (indexdaily.at[i, 'ts_code'], indexdaily.at[i, 'trade_date'],
                                    float(indexdaily.at[i, 'open']), float(indexdaily.at[i, 'high']),
                                    float(indexdaily.at[i, 'low']), float(indexdaily.at[i, 'close']),
                                    float(indexdaily.at[i, 'pre_close']), float(indexdaily.at[i, 'change']),
                                    float(indexdaily.at[i, 'pct_chg']), float(indexdaily.at[i, 'vol']),
                                    float(indexdaily.at[i, 'amount'])))
            #         提交数据库
            self.dbconn.commit()
            print('{} ===========>index ts_code:{} got'.format( time.asctime( time.localtime(time.time()) ) ,index))

    def getfundsdaily(self):
        # 获取基金数据，基金是按照‘一天‘传参数
        self.dbcur.execute('''create table if not exists fundsdaily (ts_code varchar(20),trade_date date,openprice float,highprice float,	
                           lowprice float,closeprice float,pre_closeprice float,changeprice float,pct_chg float,vol float,amount float,
                           primary key(ts_code,trade_date))''')
        for day in pd.date_range(self.startdate,self.enddate):
            print('{} ===========>date:{} funds fetching'.format(time.asctime( time.localtime(time.time()) ),day.strftime('%Y%m%d')))
            time.sleep(2)
            try:
                fundsdaily = pro.fund_daily(trade_date=day.strftime('%Y%m%d'))
            except:
                print(day.strftime('%Y%m%d'), ', error')
                time.sleep(5)
                continue
            for i in range(len(fundsdaily)):
                self.dbcur.execute('''insert ignore into fundsdaily(ts_code,trade_date,openprice,highprice,lowprice,closeprice,pre_closeprice,changeprice,pct_chg,vol,amount) 
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                                   (fundsdaily.at[i, 'ts_code'], fundsdaily.at[i, 'trade_date'],
                                    float(fundsdaily.at[i, 'open']), float(fundsdaily.at[i, 'high']),
                                    float(fundsdaily.at[i, 'low']), float(fundsdaily.at[i, 'close']),
                                    float(fundsdaily.at[i, 'pre_close']), float(fundsdaily.at[i, 'change']),
                                    float(fundsdaily.at[i, 'pct_chg']), float(fundsdaily.at[i, 'vol']),
                                    float(fundsdaily.at[i, 'amount'])))
            #         提交数据库
            self.dbconn.commit()
            print('{} ===========>date:{} funds got'.format( time.asctime( time.localtime(time.time()) ) ,day.strftime('%Y%m%d')))

if __name__=='__main__':
    dm = datamanage('20181215')
    dm.getstocksdaily()
#    dm.getadjfactor()
