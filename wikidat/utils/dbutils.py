# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:27:02 2014

@author: jfelipe
"""
import MySQLdb
import warnings


class MySQLDB(object):
    """
    Models connections to MySQL database (convenience methods)

    TODO: To be substitued by SQLAlchemy, if appropriate
    """

    def __init__(self, db='testwiki', host='localhost', port=3306,
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
        self.con = MySQLdb.Connect(host=self.host, port=self.port,
                                   user=self.user, passwd=self.passwd,
                                   db=self.db, charset="utf8",
                                   use_unicode=True)
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

    def send_query(self, query):
        """
        Send query to DB. Attempt 'ntimes' consecutive times before giving up
        query: query to be sent to DB
        """
        # TODO: Handle errors properly with logger library
        #chances = 0
        #while chances < ntimes:
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            warnings.simplefilter('ignore', MySQLdb.Warning)
            try:
                self.cursor.execute(query)
                self.db.commit()
            except (Exception), e:
                # TODO: This is potentially dangerous, we should
                # capture and log DB exceptions adequately using
                # Python logger
                pass

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
