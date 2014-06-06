__author__ = 'danigosa'

"""
MySQL database backend for Django. Modified to support pooling with SQLAlchemy directly from DATABASES settings

Requires MySQLdb: http://sourceforge.net/projects/mysql-python
Requires SQLAlchemy: http://www.sqlalchemy.org/
"""

import pymysql as Database
from pymysql import OperationalError

import time
from sqlalchemy import exc
from sqlalchemy import event
import sqlalchemy.pool as pool
from sqlalchemy.pool import Pool
import logging
from django.conf import settings
from django.utils import importlib
import hashlib
from django.db import utils
import sys


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

backend_module = importlib.import_module('pymysql')

def serialize(**kwargs):
    # We need to figure out what database connection goes where
    # so we'll hash the args.
    keys = sorted(kwargs.keys())
    out = [repr(k) + repr(kwargs[k])
           for k in keys if isinstance(kwargs[k], (str, int, bool))]
    return hashlib.md5(''.join(out)).hexdigest()


class DatabaseCreation(backend_module.DatabaseCreation):
    # The creation flips around between databases in a way that the pool
    # doesn't like. After the db is created, reset the pool.
    def _create_test_db(self, *args):
        result = super(DatabaseCreation, self)._create_test_db(*args)
        db_pool.close()
        return result


class SafeCursorWrapper(backend_module.CursorWrapper):
    """
    Override Cursor Execution to prevent Connection Errors
    Errors with retrial: 1205, 2006, 2013
    """

    codes_for_connectionerror = ('2006', '2013')

    def execute(self, query, args=None):
        try:
            return self.cursor.execute(query, args)
        except backend_module.Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except backend_module.Database.OperationalError, e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e[0] in self.codes_for_connectionerror:
                # Refresh cursor with a new connection and retry
                connection = db_pool.connect(**POOL_SETTINGS)

                connection.encoders[backend_module.SafeUnicode] = connection.encoders[unicode]
                connection.encoders[backend_module.SafeString] = connection.encoders[str]

                backend_module.connection_created.send(sender=self.__class__, connection=self)
                self.cursor = connection.cursor()
                return self.cursor.execute(query, args)

            elif e[0] in self.codes_for_integrityerror:
                raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]
        except backend_module.Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

    def executemany(self, query, args):
        try:
            return self.cursor.executemany(query, args)
        except backend_module.Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except backend_module.Database.OperationalError, e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e[0] in self.codes_for_connectionerror:
                # Refresh cursor with a new connection and retry
                connection = db_pool.connect(**POOL_SETTINGS)

                connection.encoders[backend_module.SafeUnicode] = connection.encoders[unicode]
                connection.encoders[backend_module.SafeString] = connection.encoders[str]

                backend_module.connection_created.send(sender=self.__class__, connection=self)
                self.cursor = connection.cursor()
                return self.cursor.executemany(query, args)

            elif e[0] in self.codes_for_integrityerror:
                raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]
        except backend_module.Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]


class DatabaseWrapper(backend_module.DatabaseWrapper):
    # Unfortunately we have to override the whole cursor function
    # so that Django will pick up our managed Database class.
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.creation = DatabaseCreation(self)

    def _serialize(self, settings_dict=None):
        if settings_dict is None:
            settings_dict = self.settings_dict

        kwargs = {
            'conv': backend_module.django_conversions,
            'charset': 'utf8',
            'use_unicode': True,
        }

        if settings_dict['USER']:
            kwargs['user'] = settings_dict['USER']
        if settings_dict['NAME']:
            kwargs['db'] = settings_dict['NAME']
        if settings_dict['PASSWORD']:
            kwargs['passwd'] = settings_dict['PASSWORD']
        if settings_dict['HOST'].startswith('/'):
            kwargs['unix_socket'] = settings_dict['HOST']
        elif settings_dict['HOST']:
            kwargs['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            kwargs['port'] = int(settings_dict['PORT'])
        # We need the number of potentially affected rows after an
        # "UPDATE", not the number of changed rows.
        kwargs['client_flag'] = backend_module.CLIENT.FOUND_ROWS
        kwargs.update(settings_dict['OPTIONS'])
        # SQL Alchemy can't serialize the dict that's in OPTIONS, so
        # we'll do some serialization ourselves. You can avoid this
        # step specifying sa_pool_key in the DB settings.
        kwargs['sa_pool_key'] = serialize(**kwargs)
        return kwargs

    def _rollback(self):
        # Connection has been lost, lets put it out from the pool
        # Close the connection and return to the pool with a new one
        # Close performs a DPApi rollback() on the connection
        # Avoid using MySQLdb _rollback() on BaseDatabaseWrapper
        if self.connection:
            self.connection.close()

    def _is_valid_connection(self):
        # If you don't want django to check that the connection is valid,
        # then set DATABASE_POOL_CHECK to False.
        if getattr(settings, 'MYSQL_DJANGO_CONNECTION_CHECK', True):
            return self._valid_connection()
        # Force connection refreshing in each cursor created
        return False

    def _cursor(self):
        if not self._is_valid_connection():
            _settings = self._serialize()
            self.connection = db_pool.connect(**_settings)

            self.connection.encoders[backend_module.SafeUnicode] =\
                    self.connection.encoders[unicode]
            self.connection.encoders[backend_module.SafeString] =\
                    self.connection.encoders[str]

            backend_module.connection_created.send(sender=self.__class__,
                                                   connection=self)

        cursor = SafeCursorWrapper(self.connection.cursor())
        return cursor