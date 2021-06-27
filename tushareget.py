import datamanage
import datetime

if __name__=='__main__':
    dm = datamanage.datamanage(startdate=(datetime.datetime.today()-datetime.timedelta(5)).strftime('%Y%m%d'))
    dm.getstocksdaily()
    dm.getindexdaily()
    dm.getadjfactor()
    dm.getfundsdaily()
