"""
Module to get data from exchanhe. Now implement Moscow Exchange
"""

import requests
import pandas as pd
from pandas import DataFrame


class Exchanhe:
    """
    Class for get data from different Exchange
    """

    def __init__(self):
        self._link = 'iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles.csv?from=2015-04-04&till=2015-04-15&interval=10'
        self.interval={1:1,10:10,60:60,24:60*24}

    def link(self, company, date, interval):
        return pd.read_csv('http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/'+
                           company+'/candles.csv?from='+date+'&till='+date+'&interval='+str(interval),
                           sep=';', encoding='windows-1251', engine='python',
                           skiprows=2)


    def get(self):
        return self.link('SBER','2015-04-06',1)

    def _get(self, link):
        count = 0
        df = DataFrame()
        while True:
            print(count)
            data = pd.read_csv(link + '&start=' + str(count * 100),
                               sep =';',encoding = 'windows-1251', engine = 'python',
                               skiprows=2)
            if data.empty:
                break;
            else:
                df = pd.concat([df, data])
                count += 1
        return df

    def get_company(self):
        return self._get(self.link_company)[['SECID','SHORTNAME']]


if __name__=='__main__':
    e = Exchanhe()
    x = e.get()
    print(x)

