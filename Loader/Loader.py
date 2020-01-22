"""
Module for load data
"""

from config import COMPANY
import pandas as pd

class Loader:
    """
    Class to fill data base
    """

    def __init__(self):
        self.interval = {'1min':1,'10min':2,'1hour':60,'1day':1}
        self.company = pd.read_csv(COMPANY, sep = ';', encoding='windows-1251', engine='python')[['SECID','SECNAME']]

    def _get_company(self):
        return df

if __name__ == '__main__':
    l = Loader()
    print(l.fill_company())