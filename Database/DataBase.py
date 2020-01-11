from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, Boolean,DateTime,Float,ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship

import datetime

"""
Module is description structure of database

"""


Base = declarative_base()


class Company(Base):
    """
    Table description company
    """
    __tablename__ = 'company'
    id = Column(Integer(), primary_key=True)
    tag = Column(String(5), nullable=False, unique=True)
    description = Column(String(50), default='')

class Share(Base):
    """
    Table shares
    """
    __tablename__ = 'share'
    __table_args__ = (ForeignKeyConstraint(['company_id'], ['company.id']),)

    interval = Column(Integer(), primary_key=True)
    datetime = Column(DateTime(), primary_key=True)
    company_id = Column(Integer(), primary_key=True)
    open = Column(Float())
    close = Column(Float())
    min = Column(Float())
    max = Column(Float())
    volume = Column(Float())
    company = relationship('Company', backref = backref('share'))

engine = create_engine('sqlite:///test4.db')
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()
x = Share(datetime = datetime.datetime.now(),
          interval = 15,
          open =  100.,
          close = 125.,
          min = 10.,
          max = 25.,
          volume= 112)
#
# for i in range(10):
#     x = Company(tag='SBER{0}'.format(i), description=str(i))
#     session.add(x)
# comp = session.query(Company).filter(Company.tag == 'SBER7').one()
# x.company = comp
# session.add(x)
# session.commit()
# comp = session.query(Share).join(Company).filter(Company.tag == 'SBER5')
comp_id = session.query(Company.id).filter(Company.tag == 'SBER5').scalar()
comp = session.query(Share).filter(Share.company_id == comp_id)
# x.company= comp
# print(x.company)
# session.delete(comp)
comp.delete()
session.commit()
# print(comp)
# for x in comp:
#     print(x.interval, x.company_id)
