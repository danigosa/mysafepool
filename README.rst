=====
MySafePool
=====

An extension of mysql pooling with SQLAlchemy and PyMYSQL

Requirements
-----------

- Django 1.6
- Pymysql
- SQLAlchemy

Install
------------

$ pip install git+https://github.com/danigosa/mysafepool

On settings add "mysqlpool" as database backend.

Add variables like this::

    SQLALCHEMY_POOL_OPTIONS = {
    'max_overflow': 5,
    'pool_size': 5,
    'recycle': 60
    }
    MYSQL_CONNECT_DEFAULT_RETRIALS = 2
