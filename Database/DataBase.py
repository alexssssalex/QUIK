"""

Module is description of database

"""

from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKeyConstraint
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Company(Base):
    """
    Company description.
    """
    __tablename__ = 'company'
    company = Column(String(), primary_key=True, unique=True)
    description = Column(String(), default='')


class Share(Base):
    """
    Shares.
    """
    __tablename__ = 'share'
    __table_args__ = (ForeignKeyConstraint(('company',), ['company.company']),)

    interval = Column(Integer(), primary_key=True)
    datetime = Column(DateTime(), primary_key=True)
    company = Column(String(), primary_key=True)
    open = Column(Float())
    close = Column(Float())
    min = Column(Float())
    max = Column(Float())
    volume = Column(Float())
    company_ref = relationship('Company', backref=backref('share'))


": set of data are needs to fill DataFrame to put in database (except 'time' - it will be axis of dataframe)"
INPUTS = {'tag', 'description', 'interval', 'open', 'close', 'min', 'max', 'volume'}
