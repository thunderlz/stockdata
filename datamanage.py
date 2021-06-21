import tushare as ts
# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt
import pymysql
ts.set_token('808bf3dd5d9ecbad0130ffc842aa3338112ddbb389e91fa967240921')
pro = ts.pro_api()
import time

import datetime
import warnings
warnings.filterwarnings("ignore")



class datamanage():
    #     管理股票的价格数据，从tushare获取数据，如果已经有数据，就不获取。

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
            print('{} ===========>ts_code:{} fetching'.format(time.asctime( time.localtime(time.time()) ),stock))
            time.sleep(1)
            try:
                stockdaily = pro.daily(ts_code=stock, start_date=self.startdate, end_date=self.enddate)
            except:
                print(stock, ', error')
                break
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
            print('{} ===========>ts_code:{} got'.format( time.asctime( time.localtime(time.time()) ) ,stock))

    #     复权因子
    def getadjfactor(self):
        self.dbcur.execute(
            'create table if not exists adj_factor (ts_code varchar(20),trade_date date,adj_factor float,primary key(ts_code,trade_date))')

        if len(self.stocks) == 0:
            self.stocks = list(self.stocklist['ts_code'])

        #         获取所有数据
        for stock in self.stocks:
            print('{}============>ts_code:{} fetching'.format(time.asctime( time.localtime(time.time()) ),stock))
            time.sleep(1)
            stockdaily = pro.adj_factor(ts_code=stock, start_date=self.startdate, end_date=self.enddate)
            for i in range(len(stockdaily)):
                self.dbcur.execute('''insert ignore into adj_factor(ts_code,trade_date,adj_factor) 
                                    values(%s,%s,%s)''',
                                   (stockdaily.at[i, 'ts_code'], stockdaily.at[i, 'trade_date'],
                                    float(stockdaily.at[i, 'adj_factor'])))
            #         提交数据库
            self.dbconn.commit()

if __name__=='__main__':
    dm = datamanage('20181215')
    dm.getstocksdaily()
#    dm.getadjfactor()
