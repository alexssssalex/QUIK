"""
Module response for interaction with database.
"""

from Database.DataBase import Company, Share, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists
from pandas import DataFrame as DataFrame
import sys
import datetime
from typing import Callable
import traceback
from config import LOG_FILE, LOG_FILE_COUNT, LOG_FILE_SIZE, LOG_LEVEL, DATABASE
import logging
import logging.handlers

LOG_FILENAME = LOG_FILE
log = logging.getLogger()
handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=LOG_FILE_SIZE, backupCount=LOG_FILE_COUNT)
log.setLevel(LOG_LEVEL)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-10s: %(message)s - %(module)s'))
log.addHandler(handler)


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

    def _put(self, table, data, *args):
        """
        Function to put data in table of database. 'data' is all column of this table
        After data we put list of primary keys. Function check if inputs data[*args] not in table and add only new data

        :param Callable table: Class of table
        :param DataFrame data: !!! DataFrame must contain exactly(only) columns of 'table'
        :param str args: sequence 'primary key's of 'table' to check
        :return:
        :rtype: bool
        """
        for d in data.to_dict('index').values():
            ex = exists()
            # <editor-fold desc="make 'ex' to check is it new values">
            for name in args:
                ex = ex.where(getattr(table, name) == d[name])
            # </editor-fold>
            if not self.session.query(ex).scalar():
                self.session.add(table(**d))
        return True

    def put(self, data):
        """
        Put DataFrame data in data base (add only new data, if record already in database missed them)

        :param DataFrame data: DataFrame must have columns 'company', 'interval','datetime','min','max',
                'open','close','volume','description'
        :return: if all OK return True
        :rtype: bool
        """
        # <editor-fold desc="Check is 'data' DataFrame">
        if not isinstance(data, DataFrame):
            logging.error('Error put data in data base. Data is not DataFrame')
            return False
        # </editor-fold>
        try:
            # <editor-fold desc="Make column 'datetime'">
            if 'datetime' not in data:
                data['datetime'] = data.index
            # </editor-fold>
            self._put(Company, data[['company', 'description']], 'company')
            self._put(Share, data[['company', 'interval', 'datetime', 'min', 'max', 'open', 'close', 'volume']],
                      'company', 'interval', 'datetime')
            self.session.commit()
            return True
        except Exception:
            self.session.rollback()
            logging.error('Error put data in data base.' + '\n'.join(traceback.format_exception(sys.exc_info()[0],
                                                                                                sys.exc_info()[1],
                                                                                                sys.exc_info()[2])))
        return False

    def get(self, table, *args):
        """
        Function for qwery from table

        Example self._get(Share, Share.min>10, Share.max<25)

        :param Callable table: class of table
        :param args: fillter expression like Share.min>10, Share.max<25
        :return:
        :rtype: DataFrame
        """
        qw = self.session.query(*[getattr(table, x) for x in [col.key for col in table.__table__.columns]])
        for f in args:
            qw = qw.filter(f)
        df = DataFrame(qw.all())
        return df.set_index('datetime') if 'datetime' in df else df


if __name__ == '__main__':
    m = Manager(DATABASE)
    time = [datetime.datetime(2020, 12, 2, 14), datetime.datetime(2025, 12, 2, 15)]
    data1 = DataFrame(
        {'description': ['', ''], 'datetime': time, 'min': [1, 2], 'max': [10, 11], 'open': [25, 26], 'close': [56, 45],
         'volume': [25, 26], 'company': ['SBER', 'SBER8888jjjjjj25'], 'interval': [41, 45]}).set_index('datetime')
    print(m.get(Share))
