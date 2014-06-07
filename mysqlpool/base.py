import UserDict

__author__ = 'danigosa'

"""
MySQL database backend for Django in green mode.
Modified to support pooling with SQLAlchemy directly from DATABASES settings (with SSL support)

Requires Django >= 1.6
Requires PyMYSQL: https://github.com/PyMySQL/PyMySQL
Requires SQLAlchemy: http://www.sqlalchemy.org/
"""

# Monkey Patch MysqlDb driver with pymysql
try:
    import pymysql

    pymysql.install_as_MySQLdb()
except ImportError:
    pass

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
import os
from django.utils.safestring import SafeBytes, SafeText
from django.utils import six


log = logging.getLogger()

# Pool settings
POOL_SETTINGS = getattr(settings, 'SQLALCHEMY_POOL_OPTIONS', {})

# Global variable to hold the actual connection pool.
MYSQLPOOL = None


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


def _hash_kwargs(kwargs):
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


def serialize(**kwargs):
    """
    We need to figure out what database connection goes where
    so we'll hash the args.
    :param kwargs:
    :return: md5 encoded args
    """
    keys = sorted(kwargs.keys())
    out = [repr(k) + repr(kwargs[k])
           for k in keys if isinstance(kwargs[k], (str, int, bool))]
    return hashlib.md5(''.join(out)).hexdigest()


def _get_pool():
    """
    Creates one and only one pool using the configured settings
    """
    global MYSQLPOOL
    if MYSQLPOOL is None:
        MYSQLPOOL = ManagerProxy(pool.manage(Database, **POOL_SETTINGS))
        setattr(MYSQLPOOL, '_pid', os.getpid())
    if getattr(MYSQLPOOL, '_pid', None) != os.getpid():
        pool.clear_managers()
    return MYSQLPOOL


def _connect(**kwargs):
    """
    Obtains a database connection from the connection pool
    """
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
    return _get_pool().connect(**kwargs)

django_backend_module = importlib.import_module('django.db.backends.mysql.base')


class DatabaseCreation(django_backend_module.DatabaseCreation):
    """
    The creation flips around between databases in a way that the pool
    doesn't like. After the db is created, reset the pool.
    """

    def _create_test_db(self, *args):
        result = super(DatabaseCreation, self)._create_test_db(*args)
        _get_pool().close()
        return result


class SafeCursorWrapper(django_backend_module.CursorWrapper):
    """
    Override Cursor Execution to prevent Connection Errors
    Errors with retrial: 1205, 2006, 2013
    """

    codes_for_connectionerror = ('1205', '2006', '2013')

    def execute(self, query, args=None):
        try:
            return self.cursor.execute(query, args)
        except django_backend_module.Database.IntegrityError, e:
            if e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            raise
        except django_backend_module.Database.OperationalError, e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e[0] in self.codes_for_connectionerror:
                # Refresh cursor with a new connection and retry
                connection = _get_pool().connect(**POOL_SETTINGS)

                connection.encoders[SafeText] = connection.encoders[six.text_type]
                connection.encoders[SafeBytes] = connection.encoders[bytes]

                django_backend_module.connection_created.send(sender=self.__class__, connection=self)
                self.cursor = connection.cursor()
                return self.cursor.execute(query, args)

            elif e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            raise
        except django_backend_module.Database.DatabaseError, e:
            if e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            raise

    def executemany(self, query, args):
        try:
            return self.cursor.executemany(query, args)
        except django_backend_module.Database.IntegrityError, e:
            if e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            raise
        except django_backend_module.Database.OperationalError, e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e[0] in self.codes_for_connectionerror:
                # Refresh cursor with a new connection and retry
                connection = _get_pool().connect(**POOL_SETTINGS)

                connection.encoders[SafeText] = connection.encoders[six.text_type]
                connection.encoders[SafeBytes] = connection.encoders[bytes]

                django_backend_module.connection_created.send(sender=self.__class__, connection=self)
                self.cursor = connection.cursor()
                return self.cursor.executemany(query, args)

            elif e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            raise
        except django_backend_module.Database.DatabaseError, e:
            if e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
            raise


class DatabaseWrapper(django_backend_module.DatabaseWrapper):
    # Unfortunately we have to override the whole cursor function
    # so that Django will pick up our managed Database class.
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.creation = DatabaseCreation(self)

    def _serialize(self, settings_dict=None):
        if settings_dict is None:
            settings_dict = self.settings_dict

        kwargs = {
            'conv': django_backend_module.django_conversions,
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
        kwargs['client_flag'] = django_backend_module.CLIENT.FOUND_ROWS
        kwargs.update(settings_dict['OPTIONS'])
        # SQL Alchemy can't serialize the dict that's in OPTIONS, so
        # we'll do some serialization ourselves. You can avoid this
        # step specifying sa_pool_key in the DB settings.
        kwargs['sa_pool_key'] = serialize(**kwargs)
        return kwargs

    def ensure_connection(self):
        """
        Guarantees that a connection to the database is established.
        """
        if self.connection is None:
            _settings = self._serialize()
            self.connection = _get_pool().connect(**_settings)

            self.connection.encoders[SafeText] = \
                self.connection.encoders[six.text_type]
            self.connection.encoders[SafeBytes] = \
                self.connection.encoders[bytes]

            django_backend_module.connection_created.send(sender=self.__class__,
                                                          connection=self)

    def _rollback(self):
        # Connection has been lost, lets put it out from the pool
        # Close the connection and return to the pool with a new one
        # Close performs a DPApi rollback() on the connection
        # Avoid using MySQLdb _rollback() on BaseDatabaseWrapper
        if self.connection:
            self.connection.close()

    def create_cursor(self):
        return SafeCursorWrapper(self.connection.cursor())

    def _cursor(self):
        self.ensure_connection()
        with self.wrap_database_errors:
            return self.create_cursor()
