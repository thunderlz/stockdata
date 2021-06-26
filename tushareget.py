import datamanage
import datetime

if __name__=='__main__':
    dm = datamanage.datamanage((datetime.datetime.today()-datetime.timedelta(8000)).strftime('%Y%m%d'))
    dm.getstocksdaily()
    dm.getindexdaily()
    dm.getadjfactor()
