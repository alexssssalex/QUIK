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

    def _put_record(self, rec, primary_key=True, foreign_key=False):
        """
         * put record in database;
         * flash and return point on it from database(if record already in data base return it)

        :param class rec:
        :return: record point on inserted record from data base
        :rtype: Callable
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
        id = flt.first()
        if id is not None:
            return id
        else:
            self.session.add(rec)
            self.session.flush()
            return rec

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
            for d in data.to_dict('index').values():
                company = self._put_record(Company(ID=d['ID'], company=d['company']))
                interval = self._put_record(Interval(ID=d['interval']))
                start = self._put_record(Time(ID=d['begin']))
                end = self._put_record(Time(ID=d['end']))
                open = self._put_record(Price(companyID=company.ID, timeID=start.ID, price=d['open']), primary_key=False, foreign_key=True)
                close = self._put_record(Price(companyID=company.ID, timeID=end.ID, price=d['close']), primary_key=False, foreign_key=True)
                self._put_record(Share(openID=open.ID, closeID=close.ID, intervalID=interval.ID, high=d['high'], low=d['low'],
                                       volume=d['volume'], value=d['value']))
                self.session.commit()
            return True
        except Exception:
            self.session.rollback()
            logging.error('Error put data in data base.' + '\n'.join(traceback.format_exception(sys.exc_info()[0],
               sys.exc_info()[1], sys.exc_info()[2])))
            return False

    def get(self, fields=list(), join=list(), filter=list()):
        """
        Function for query from table

        Example self._get(Share, Share.min>10, Share.max<25)
        Price1 = aliased(Price)
         m.get(fields=(Price.timeID.label('sasa'), Price1.timeID),
                join=((Share,Share.openID==Price.ID),(Price1, Share.closeID==Price1.ID )),
                filter=(Time.ID<datetime.datetime(2015,6,4,10,10),Time.ID>datetime.datetime(2015,6,4,10,1)))

        :param Callable table: class of table
        :param args: fillter expression like Share.min>10, Share.max<25
        :return:
        :rtype: DataFrame
        """
        Price1 = aliased(Price)
        qw = self.session.query(*fields)
        for x in join:
            qw = qw.join(x)
        for x in filter:
            qw = qw.filter(x)
        df = DataFrame(qw.all())
        return df


if __name__ == '__main__':
    m = Manager()
    # data1 = {
    #     'description': ['ghgg', 'gggg'],
    #     'company': ['SBER', 'SBER'],
    #     'interval': ['1m', '1m'],
    #     'open': [65.65, 66.17],
    #     'close': [66.17, 66.31],
    #     'high': [66.2, 66.48],
    #     'low': [65.57, 66.09],
    #     'volume': [2.58e8, 3.04e8],
    #     'value': [3912130, 4586100],
    #
    #     'begin': [datetime.datetime(2015, 6, 4, 10), datetime.datetime(2015, 6, 4, 10,9)],
    #     'end': [datetime.datetime(2015, 6, 4, 10,10), datetime.datetime(2025, 6, 4, 10,19)],

    # }
    # x = DataFrame(data1)
    # # # # print(x[[True,False]])
    # print(m.put(x))
    # x = Time(id = d
    # print(m.get(Company))
    # print(DataFrame(m.session.query(Time.id, Share.begin_id, Company.company,Price).join(Company).join(Time, Time.id ==Share.begin_id).join(Price, Price.company_id==Company.id).filter(Share.company_id=='SBER').all()))
    d1= aliased(Price)
    qw = m.session.query(Company.ID)
    # qw = qw.join(Price, Share.openID==Price.ID).join(d1,Share.closeID==d1.ID)
    # qw = qw.filter(Price.companyID =='SBER').filter(Interval.ID =='1m')
    #
    # qw = qw.all()
    # Price1 = aliased(Price)
    # print(m.get(fields=(Price.timeID.label('sasa'), Price1.timeID),
    #             join=((Share,Share.openID==Price.ID),(Price1, Share.closeID==Price1.ID )),
    #             filter=(Time.ID<datetime.datetime(2015,6,4,10,10),Time.ID>datetime.datetime(2015,6,4,10,1))))
    #
    # df =m.get_data('SBER','2019-12-02','10m')

    # print(m.company[m.company['ID']=='SBER'])

    #
    # primaryKeyColName = table.primary_key.colum
    for x in ['GAZP','SBER']:
        m.put(x, '2018-01-01', '2020-01-20', '10m')