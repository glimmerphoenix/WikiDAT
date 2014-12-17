# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:27:02 2014

@author: jfelipe
"""
import MySQLdb
import warnings
import wikidat.retrieval.db.base_schema as bs


class MySQLDB(object):
    """
    Models connections to MySQL database (convenience methods)

    TODO: To be substitued by SQLAlchemy, if appropriate
    """

    def __init__(self, db=None, host='localhost', port=3306,
                 user=None, passwd=None):
        """
        Intilialize new MySQL DB connection object
        """
        self.db = db
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.con = None  # Connection to DB
        self.cursor = None  # Cursor to DB

    def connect(self):
        """
        Establish a new connection to MySQL DB and initialize new cursor
        """
        if self.db is None:
            self.con = MySQLdb.Connect(host=self.host, port=self.port,
                                       user=self.user, passwd=self.passwd,
                                       charset="utf8", use_unicode=True)
        else:
            # print "Connected to database: " + self.db
            self.con = MySQLdb.Connect(host=self.host, port=self.port,
                                       user=self.user, passwd=self.passwd,
                                       db=self.db, charset="utf8",
                                       use_unicode=True, local_infile=True)
        self.cursor = self.con.cursor()

    def close(self):
        """
        Close existing connection to MySQL DB
        """
        if(self.con is not None):
            self.con.close()
            self.con = None
            self.cursor = None

    def __repr__(self):
        return """wikidat.dbutils.MySQLDB object
            to database %r in host %r at port %r""" % (
            self.db, self.host, self.port)

    def db_exists(self, db):
        """
        Check if DB already exists or not
        """
        params = {'dbname': db}
        return self.execute_query(bs.check_database.format(**params))

    def create_database(self, db):
        """
        Create schema in local database
        """
        # TODO: Parameterize engine with common configuration file
        # using ArgParse
        params = {'dbname': db}
        self.send_query(bs.drop_database.format(**params))
        self.send_query(bs.create_database.format(**params))

    def create_schema_revhist(self, engine='ARIA'):
        """
        Create schema in local database for tables related to rev-history dumps
        """
        # TODO: Parameterize engine with common configuration file
        params = {'engine': engine}
        self.send_query(bs.drop_page)
        self.send_query(bs.create_page.format(**params))
        self.send_query(bs.drop_revision)
        self.send_query(bs.create_revision.format(**params))
        self.send_query(bs.drop_revision_hash)
        self.send_query(bs.create_revision_hash.format(**params))
        self.send_query(bs.drop_namespaces)
        self.send_query(bs.create_namespaces.format(**params))
        self.send_query(bs.drop_user)
        self.send_query(bs.create_user.format(**params))
        self.send_query(bs.drop_revision_IP)
        self.send_query(bs.create_revision_IP.format(**params))
        self.send_query(bs.drop_revision_user_zero)
        self.send_query(bs.create_revision_user_zero.format(**params))
        self.send_query(bs.drop_IP_country)
        self.send_query(bs.create_IP_country.format(**params))

    def create_schema_logitem(self, engine='ARIA'):
        """
        Create schema in local database for tables related with logging dump
        """
        # TODO: Parameterize engine with common configuration file
        params = {'engine': engine}
        self.send_query(bs.drop_logging)
        self.send_query(bs.create_logging.format(**params))
        self.send_query(bs.drop_block)
        self.send_query(bs.create_block.format(**params))
        self.send_query(bs.drop_user_new)
        self.send_query(bs.create_user_new.format(**params))
        self.send_query(bs.drop_user_level)
        self.send_query(bs.create_user_level.format(**params))

    def create_pks_revhist(self):
        """
        Create primary keys for baselines database tables in revision
        history dumps
        """
        print "Creating primary key for table page..."
        self.send_query(bs.pk_page)
        print "Creating primary key for table revision..."
        self.send_query(bs.pk_revision)
        print "Creating primary key for table namespaces..."
        self.send_query(bs.pk_namespaces)
        print "Creating primary key for table user and others..."
        print
        self.send_query(bs.pk_user)
        self.send_query(bs.pk_user_new)
        self.send_query(bs.pk_user_level)
        self.send_query(bs.pk_revision_IP)
        self.send_query(bs.pk_IP_country)

    def create_pks_logitem(self):
        """
        Create primary keys for baselines database tables in logging dump
        """
        print "Creating primary key for table logging and block..."
        print
        self.send_query(bs.pk_logging)
        # PKs for additional tables used in data processing
        self.send_query(bs.pk_block)

    def insert_namespaces(self, nsdict):
        """
        Insert namespace info (from RevHist or RevMeta dumps)
        """
        self.cursor.executemany(bs.insert_namespaces, nsdict.items())

    def send_query(self, query):
        """
        Send query to DB. Attempt 'ntimes' consecutive times before giving up
        query: query to be sent to DB
        """
        # TODO: Handle errors properly with logger library
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            warnings.simplefilter('ignore', MySQLdb.Warning)
            try:
                self.cursor.execute(query)
                # self.con.commit()
            except (Exception), e:
                # TODO: This is potentially dangerous, we should
                # capture and log DB exceptions adequately using
                # Python logger
                print "Exception in send_query method: ", e
                print query

    def insert_many(self, query_template, values):
        """
        Send multiple statements to DB. Typically used in bulk data inserts.
        """
        # TODO: Handle errors properly with logger library
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            warnings.simplefilter('ignore', MySQLdb.Warning)
            try:
                self.cursor.executemany(query_template, values)
                # self.con.commit()
            except (Exception), e:
                # TODO: This is potentially dangerous, we should
                # capture and log DB exceptions adequately using
                # Python logger
                print "Exception in send_query method: ", e

    def execute_query(self, query):
        """
        Send query to DB, fetch all returned values
        cursor: DB connection cursor
        query: query sent to DB
        """
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            # TODO: Handle errors properly with logger
            warnings.simplefilter('ignore', MySQLdb.Warning)
            try:
                nres = self.cursor.execute(query)
                results = self.cursor.fetchall()
                if nres == 0:
                    return None
                else:
                    return results
            except (Exception):
                raise
