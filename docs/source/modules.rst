Description
###########
Market robot
Remarks:

* all exchange data by DataFrame with index axis Timestamp
* config.py - file with config constant
* DataBase - package work with data base;

Notes
#####

This is first version for test database. Next level implement class which
give ability simulate Exchange:

* class can add user;
* user can ask current time;
* use can ask history only before time;
* user can buy and sold share;

Use cases
#########

.. figure:: ../../uml/images/UseCases.png


Database structure
##################

.. figure:: ../../uml/images/DataBase.png

    Structure of database

class structure
##################
.. figure:: ../../uml/images/Classes.png


Packages and files
##################

CONFIG
======

.. automodule:: config
    :members:


DataBase
========
Package respond for create and communication with database.

* exchange with external application by DataFrame;
* implement method for usual task
* manage reliability database;
* implement method for usual task:
    * get data from database(as DataFrame);
    * put data (DataFrame) from DataFrame

data
----
Folder with data

+------------+-------------------------+
|company.csv | file with company names |
+------------+-------------------------+
|database.db     | data base               |
+------------+-------------------------+


DataBase
--------

.. automodule:: Database.Tables
   :members:

Manager
--------

.. automodule:: Database.Manager
   :members:

Loader
--------

.. automodule:: Loader.Loader
   :members: