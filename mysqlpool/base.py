from django.db import utils
import sys

__author__ = 'danigosa'

"""
MySQL database backend for Django. Modified to support pooling with SQLAlchemy directly from DATABASES settings

Requires MySQLdb: http://sourceforge.net/projects/mysql-python
Requires SQLAlchemy: http://www.sqlalchemy.org/
"""

try:
    import MySQLdb
except ImportError, e:
    try:
        import pymysql as Database
    except ImportError, e:
        pass
try:
    from _mysql import OperationalError
except ImportError:
    from pymysql import OperationalError

import time
from sqlalchemy import exc
from sqlalchemy import event
import sqlalchemy.pool as pool
from sqlalchemy.pool import Pool
import logging
from django.conf import settings

log = logging.getLogger()

##### CONNECTION POOLING ######
POOL_SETTINGS = getattr(settings, 'SQLALCHEMY_POOL_OPTIONS', {})


class hashabledict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


class hashablelist(list):
    def __hash__(self):
        return hash(tuple(sorted(self)))


def mysql_connection_retry(fn):
    """
    Default settings retrials
    """
    def db_op_wrapper(*args, **kwargs):
        retries = settings.MYSQL_CONNECT_DEFAULT_RETRIALS
        for i in xrange(retries):
            try:
                return fn(*args, **kwargs)
            except OperationalError:
                log.exception('AUTORRECONNECTING MySQL Server')
                time.sleep(pow(2, i))

        raise Exception("MySQL: No luck even after %d retries" % retries)

    return db_op_wrapper


class ManagerProxy(object):
    """
    Manage connections through the pool
    """
    def __init__(self, manager):
        self.manager = manager

    def __getattr__(self, key):
        return getattr(self.manager, key)

    @mysql_connection_retry
    def connect(self, *args, **kwargs):
        if 'conv' in kwargs:
            conv = kwargs['conv']
            if isinstance(conv, dict):
                items = []
                for k, v in conv.items():
                    if isinstance(v, list):
                        v = hashablelist(v)
                    items.append((k, v))
                kwargs['conv'] = hashabledict(items)
        if 'ssl' in kwargs:
            ssl = kwargs['ssl']
            if isinstance(ssl, dict):
                items = []
                for k, v in ssl.items():
                    if isinstance(v, list):
                        v = hashablelist(v)
                    items.append((k, v))
                kwargs['ssl'] = hashabledict(items)
        return self.manager.connect(*args, **kwargs)


@event.listens_for(Pool, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    """
    Pessimistic Connection Freshness Approach
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
    except:
        # optional - dispose the whole pool
        # instead of invalidating one at a time
        # connection_proxy._pool.dispose()

        # raise DisconnectionError - pool will try
        # connecting again up to three times before raising.
        log.info("Unfreshed connection detected for the Pool! Reconnecting...")
        raise exc.DisconnectionError()
    cursor.close()

# Init backend with proxied Database
pool_initialized = False
log.debug('Init Managed Databased...')

db_pool = ManagerProxy(pool.manage(Database, **POOL_SETTINGS))