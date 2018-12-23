import datamanage

if __name__=='__main__':
    dm = datamanage.datamanage((datetime.datetime.today()-datetime.timedelta(5)).strftime('%Y%m%d'))
    dm.getstocksdaily()