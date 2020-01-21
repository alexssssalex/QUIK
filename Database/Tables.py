"""

Module is description of database

"""

from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKeyConstraint, UniqueConstraint,ForeignKey
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Company(Base):
    """
    Company description.
    """
    __tablename__ = 'company'

    ID = Column(String(), primary_key=True, unique=True)
    company = Column(String(), default='')


class Interval(Base):
    """
    Company description.
    """
    __tablename__ = 'interval'

    ID = Column(String(), primary_key=True, unique=True)


class Time(Base):
    """
    Company description.
    """
    __tablename__ = 'time'
    ID = Column(DateTime(), primary_key=True, unique=True)


class Price(Base):
    """
    Company description.
    """
    __tablename__ = 'price'
    # __table_args__ = (ForeignKeyConstraint(('company_id',), ['company.id']),
    #                   ForeignKeyConstraint(('time_id',), ['time.id']))

    ID = Column(Integer(), primary_key=True)
    companyID = Column(String(), ForeignKey('company.ID'))
    timeID = Column(DateTime(), ForeignKey('time.ID'))
    price = Column(Float(), nullable=False)


class Share(Base):
    """
    Shares.
    """
    __tablename__ = 'share'
    __table_args__ = (ForeignKeyConstraint(('openID',), ['price.ID']),
                      ForeignKeyConstraint(('closeID',), ['price.ID']),
                      ForeignKeyConstraint(('intervalID',), ['interval.ID']))

    openID = Column(Integer(), primary_key=True)
    closeID = Column(Integer(), primary_key=True)
    intervalID = Column(String())
    high = Column(Float())
    low = Column(Float())
    volume = Column(Float())
    value = Column(Float())
