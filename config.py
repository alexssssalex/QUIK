"""
FILE CONFIG
"""

import logging

"""
SET UP LOGGER
"""

#: level debug
LOG_LEVEL = logging.DEBUG

#: log file name
LOG_FILE = 'log/log.log'

#: max size of log file (bytes)
LOG_FILE_SIZE = 1024*512

#: max count of logs files
LOG_FILE_COUNT = 2

#: database
DATABASE = 'sqlite:///data/database.db'

#: file with company name
COMPANY = 'data/company.csv'

