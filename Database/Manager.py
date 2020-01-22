"""
Module response for interaction with database.
"""

from Database.Tables import Company, Share, Interval, Price, Time, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, aliased
from pandas import DataFrame as DataFrame
import pandas as pd
import sys
from datetime import datetime
from typing import Callable
import traceback
from config import LOG_FILE, LOG_FILE_COUNT, LOG_FILE_SIZE, LOG_LEVEL, DATABASE, COMPANY
import logging
import logging.handlers
from sqlalchemy.inspection import inspect

# <editor-fold desc="Add logger">
LOG_FILENAME = LOG_FILE
log = logging.getLogger()
handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=LOG_FILE_SIZE, backupCount=LOG_FILE_COUNT)
log.setLevel(LOG_LEVEL)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-10s: %(message)s - %(module)s'))
log.addHandler(handler)


# </editor-fold>


class Manager:
    """
    Class is manager of database:

    * create session;
    * give convinient method for frequency request:
        - put DataFrame in database (if there was the same record - ignore them);
        - get query from database as DataFrame;
    """

    def __init__(self):
        self.engine = create_engine(DATABASE)
        Base.metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.session = sessionmaker(bind=self.engine)()
        insp = inspect(self.engine)
        self.pr_k = {x: insp.get_primary_keys(x) for x in insp.get_table_names()}
        self.fr_k =dict()
        # <editor-fold desc="Fill foreign key">
        for x in insp.get_table_names():
            d = insp.get_foreign_keys(x)
            if d:
                self.fr_k[x] = []
                for y in d:
                    self.fr_k[x].extend(y['constrained_columns'])
            else:
                self.fr_k[x] = d
        # </editor-fold>
        self.interval = {'1m':'1','10m':'10','1h':'60','1d':'24'}
        self.company = pd.read_csv(COMPANY, sep = ';', encoding='windows-1251', engine='python')[['SECID','SECNAME']]
        self.company.columns = ['ID', 'company']

    def put(self, company, date1, date2, interval):
        """
        Put data 'company' with 'interval' from 'data1' to 'data2' in database

        :param str company: company secid
        :param str interval: interval record (valid value '1m''10m''1h''1d')
        :param str date1: data in format '2015-09-22'
        :param str date2: data in format '2015-09-22'
        :rtype:
        """
        ser = self.company[self.company['ID']==company].iloc[0]
        for data in pd.date_range(datetime.strptime(date1, '%Y-%m-%d'), datetime.strptime(date2, '%Y-%m-%d')):
            data = data.strftime('%Y-%m-%d')
            logging.info('Data "%s  %s  %s" is writing to database.Start',ser['ID'], data, interval)
            ser['interval'] = interval
            df = self._get_all_pages(ser['ID'], data, ser['interval'])
            df = df.apply(lambda x, y=ser: pd.concat([x,y]), axis = 1)
            self._put_df(df)
            logging.info('Data "%s  %s  %s" is writing to database.End',ser['ID'], data, interval)



    def _get_all_pages(self, company, date, interval):
        """
        DataFrame with share

        :param str company: company secid
        :param date: data in format '2015-09-22'
        :param str interval: interval record (valid value '1m''10m''1h''1d')
        :return: DataFrame with data
        :rtype: DataFrame
        """
        count = 0
        df = DataFrame()
        while True:
            dfx = self._get_page(company, date, interval, count * 500)
            if not dfx.empty:
                df = pd.concat([df,dfx])
                count += 1
            else:
                break
        if not df.empty:
            df.index = range(len(df.index))
            df['begin'] = pd.to_datetime(df['begin'])
            df['end'] = pd.to_datetime(df['end'])
        return df

    def _get_page(self, company, date, interval, start):
        """
        * made urn;
        * return page as DataFrame

        :param str company: shortname company
        :param str date: data in format like '2017-12-01'
        :param str interval: interval record (valid value '1m''10m''1h''1d')
        :param start: start record.data return by page with 500 record.
                To get all we need call with start=0, start=500,...
        :return:
        :rtype: DataFrame
        """
        return pd.read_csv('http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/'+
                           company+'/candles.csv?from='+date+'&till='+date+'&interval='+self.interval[interval]+'&start='+str(start),
                           sep=';', encoding='windows-1251', engine='python',
                           skiprows=2)


    def _get_id(self, rec, primary_key=True, foreign_key=False):
        """
        method return recort from data base if it is else None
        :param rec:
        :param primary_key:
        :param foreign_key:
        :return:
        :rtype:
        """
        cls = rec.__class__
        flt = self.session.query(cls)
        # <editor-fold desc="check is it new values">
        if primary_key:
            for name in self.pr_k[rec.__tablename__]:
                flt = flt.filter(getattr(cls, name) == getattr(rec, name))
        if foreign_key:
            for name in self.fr_k[rec.__tablename__]:
                flt = flt.filter(getattr(cls, name) == getattr(rec, name))
        # </editor-fold>
        return flt.first()

    def _put_record(self, recs, primary_key=True, foreign_key=False):
        """
         * put records in database;
         * flash and return list of records point on it from database(if record already in data base return it)

        :param class recs:
        :return: record point on inserted record from data base
        :rtype: Callable
        """
        for i in range(len(recs)):
            id = self._get_id(recs[i], primary_key, foreign_key)
            if id is None:
                self.session.add(recs[i])
            else:
                recs[i]=id
        self.session.flush()
        return recs

    def _put_df(self, data):
        """
        Put DataFrame data in data base (add only new data, if record already in database missed them)

        :param DataFrame data: DataFrame must have columns 'open', 'close', 'high', 'low', 'value',
            'volume', 'begin', 'end', 'description','company'
        :return: if all OK return True
        :rtype: bool
        """
        # <editor-fold desc="Check is 'data' DataFrame">
        if data.empty:
            return True
        if not isinstance(data, DataFrame) or \
                not set(['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ID',
                         'company']).issubset(data.columns):
            logging.error('Error put data in data base. Data is not DataFrame or there are not all data')
            return False
        # </editor-fold>
        try:
            d = data.to_dict('list')
            company = self._put_record([Company(ID = id, company=comp) for id, comp in zip(d['ID'], d['company'])])
            interval = self._put_record([Interval(ID=x) for x in d['interval']])
            start = self._put_record([Time(ID=x) for x in d['begin']])
            end = self._put_record([Time(ID=x) for x in d['end']])
            open = self._put_record([Price(companyID=comp.ID, timeID=t.ID, price=pr)
                                     for comp,t, pr in zip(company,start,d['open'])],
                                    primary_key=False, foreign_key=True)
            close = self._put_record([Price(companyID=comp.ID, timeID=t.ID, price=pr)
                                     for comp,t, pr in zip(company,end,d['close'])],
                                    primary_key=False, foreign_key=True)
            self._put_record([Share(openID=o.ID, closeID=c.ID, intervalID=i.ID, high=h, low=l,
                      volume=v, value=value) for o,c,i, h,l,v, value in zip(open, close, interval,d['high'],
                                                                            d['low'], d['volume'],d['value'] )])
            self.session.commit()
            return True
        except Exception:
            self.session.rollback()
            logging.error('Error put data in data base.' + '\n'.join(traceback.format_exception(sys.exc_info()[0],
               sys.exc_info()[1], sys.exc_info()[2])))
            return False

    def get_company(self):
        """
        Get list company in database

        :return:
        :rtype: DataFrame
        """
        return self._get(fields = [Company.ID.label('SHORTNAMME'), Company.company.label('NAMME')])

    def get_share(self, company, time1, time2, interval):
        """
        get share for company from tim11 to time2 with interval

        :param str company: short name company
        :param datetime time1: start time
        :param datetime time2: end time
        :param str interval: interval
        :return:
        :rtype: DataFrame
        """
        Price1 = aliased(Price)
        return self._get(
            fields=[Price.timeID.label('start'), Price1.timeID.label('end'), Share.low, Share.high,
                    Share.volume, Share.value],
                joins=[(Share, Share.openID==Price.ID),(Price1, Share.closeID==Price1.ID )],
                filters=[(Price.timeID>time1),(Price.timeID<time2),(Price.companyID==company),(Share.intervalID==interval)],
                order = Price.timeID)


    def get_price(self,time, company=None):
        """
        Get price for all company or specific company

        :param datetime time: time
        :param str company:  short name company
        :return:
        :rtype: DataFrame
        """
        if company is None:
            df=list()
            for x in m.get_company()['SHORTNAMME']:
                df.append(self._get(fields = [Price.timeID,Price.price,Price.companyID],
                      filters = [(Price.timeID>=time),(Price.companyID==x)],
                      order=Price.timeID,
                      first=True))
            return pd.concat(df)
        else:
            return self._get(fields=[Price.timeID, Price.price, Price.companyID],
                      filters=[(Price.timeID >= time),(Price.companyID==company)],
                      order=Price.timeID,
                      first=True)

    def _get(self, fields=list(), joins=list(), filters=list(), order = None , first=False):
        """
        Function for query from table

        Example self._get(Share, Share.min>10, Share.max<25)
        Price1 = aliased(Price)
         m.get(fields=[Price.timeID.label('sasa'), Price1.timeID],
                join=[(Share,Share.openID==Price.ID),(Price1, Share.closeID==Price1.ID )],
                filter=[(Time.ID<datetime.datetime(2015,6,4,10,10)),(Time.ID>datetime.datetime(2015,6,4,10,1))],
                order = Price,timeID)

        :param Callable table: class of table
        :param args: fillter expression like Share.min>10, Share.max<25
        :return:
        :rtype: DataFrame
        """
        qw = self.session.query(*fields)
        for x in joins:
            qw = qw.join(x)
        for x in filters:
            qw = qw.filter(x)
        if order is not None:
            qw = qw.order_by(order)
        if first:
            df = DataFrame([qw.first()])
        else:
            df = DataFrame(qw.all())
        return df


if __name__ == '__main__':
    m = Manager()

    # Price1 = aliased(Price)
    # print(m.get(fields=(Price.timeID.label('time'), Price.companyID, Price.price),
    #             filter=(Price.timeID>datetime(2020,1,16,11,0),Price.timeID<datetime(2020,1,16,11,4), Price.companyID=='SBER')))
    # print(m.get(fields=(Price.timeID,Price.companyID,Share.high,Share.low,Share.value,Share.value,Price.price, Price1.price),
    #             filter=(Price.timeID>datetime(2020,1,16,10,55),Price.timeID<datetime(2020,1,16,11,30), Price.companyID=='GAZP', Share.intervalID == '10m'),
    #             join=((Share,Share.openID==Price.ID),(Price1,Share.closeID==Price1.ID))))
    Price1 = aliased(Price)
    # print(m.get(fields=[Share.high,Share.low, Price.timeID,  Price1.timeID, Price.price, Price1.price, Share.intervalID, Price.companyID],
    #             joins=[(Price, Share.openID == Price.ID), (Price1, Share.closeID == Price1.ID)],
    #             filters=[(Price.timeID>datetime(2018,1,5,10,55)),(Price.timeID<datetime(2019,1,5,10,55)),(Price.companyID=='GAZP')],
    #             order = Price.timeID))
    # for x in m.session.query(Price.ID, Price.timeID, Price.companyID).filter(Price.timeID>datetime(2020,1,20,10,55)).all():
    #     print(x)
    # m.connection.execute(setattr())
    # primaryKeyColName = table.primary_key.colum
    # for y in ['1d','1h','10m','1m']:
    #     for x in ['SBER','GAZP']:
    #         m.put(x, '2018-01-01', '2019-10-01', y)
    # print(m.get_price(datetime(2018,10,5,11,2)))
    # print(m.get_company())
    print(m.get_share('SBER', datetime(2018,10,5,11,2),datetime(2018,10,15,11,2),'1d'))

