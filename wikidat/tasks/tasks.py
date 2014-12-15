# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 18:00:49 2014

Implementation of common ETL (Extraction, Transformation and Loading)
processes with Wikipedia data:

    - Page-stub (metadata only)
    - Page-meta-history dumps (complete dumps with revision history text)
    - Logging (MediaWiki activity logs, administrative actions)
    - [Future types for links, categories, etc.]

@author: jfelipe
"""

from wikidat.retrieval.etl import PageRevisionETL, LoggingETL
from wikidat.retrieval.revision import users_file_to_db
import download
from wikidat.utils.dbutils import MySQLDB
import multiprocessing as mp
import os
import sys
import glob


class Task(object):
    """
    Abstract class defining common interface for all tasks
    """

    def __init__(self, lang, db_user, db_passw, db_name, db_engine,
                 date=None, host='localhost', port=3306):
        """
        Builder method of class RevisionHistoryRetrieval.
        Arguments:
            - language: code of the Wikipedia language to be processed
            - date: publication date of target dump files collection
        """
        self.lang = lang
        self.date = date
        self.down = None
        self.etl = None
        self.host = host
        self.port = port
        self.db_user = db_user
        self.db_passw = db_passw
        self.db_name = db_name
        self.db_engine = db_engine

    def DB_exists(self):
        db_check = MySQLDB(host=self.host, port=self.port, user=self.db_user,
                           passwd=self.db_passw)
        db_check.connect()
        db_exists = db_check.db_exists(self.db_name)
        db_check.close()
        return db_exists

class RevHistoryTask(Task):
    """
    A complete, multiprocessing parser of full revision history dump files
    """

    def __init__(self, host, port, db_name, db_user, db_passw, db_engine,
                 lang='scowiki', date=None, etl_lines=1):
        """
        Builder method of class RevisionHistoryTask.
        Arguments:
            - language: code of the Wikipedia language to be processed
            - date: publication date of target dump files collection
        """
        super(RevHistoryTask, self).__init__(lang=lang, date=date, host=host,
                                             port=port, db_name=db_name,
                                             db_user=db_user,
                                             db_passw=db_passw,
                                             db_engine=db_engine)
        self.etl_lines = etl_lines
        self.etl_list = []

    def create_DB(self, complete=False):
        if complete:
            db_create = MySQLDB(host=self.host, port=self.port,
                                user=self.db_user, passwd=self.db_passw)
            db_create.connect()
            db_create.create_database(self.db_name)
            db_create.close()
        db_schema = MySQLDB(host=self.host, port=self.port, user=self.db_user,
                            passwd=self.db_passw, db=self.db_name)
        db_schema.connect()
        db_schema.create_schema_revhist(engine=self.db_engine)
        db_schema.close()

    # TODO: include args detect_FA, detect_FLIST, detect_GA
    # and implement flow control in process_revision
    def execute(self, page_fan, rev_fan, page_cache_size, rev_cache_size,
                mirror, download_files, base_ports, control_ports,
                dumps_dir=None):
        """
        Run data retrieval and loading actions.
        Arguments:
            - page_fan = Number of workers to fan out page elements parsing
            - rev_fan = Number of workers to fan out rev elements parsing
            - db_user = User name to connect to local database
            - db_passw = Password for database user
            - mirror = Base URL of site hosting XML dumps
        """
        if download_files:
            # TODO: Use proper logging module to track execution progress
            # Choose corresponding file downloader and etl wrapper
            print "Downloading new dump files from %s, for language %s" % (
                  mirror, self.lang)
            self.down = download.RevHistDownloader(mirror,
                                                   self.lang, dumps_dir)
            # Donwload latest set of dump files
            self.paths, self.date = self.down.download(self.date)
            if not self.paths:
                print "Error: dump files with pages-logging info not found."
                print "Program will exit now."
                sys.exit()

            print "Got files for lang %s, date: %s" % (self.lang, self.date)

            # db_name = self.lang + '_' + self.date.strip('/')

        else:
            # Case of dumps folder provided explicity
            if dumps_dir:
                # Allow specifying relative paths, as well
                abs_dumps_path = os.path.expanduser(dumps_dir)
                dumps_path = os.path.join(abs_dumps_path,
                                          self.lang + '_dumps', self.date)
                # Retrieve path to all available files to feed ETL lines
                if not os.path.exists(dumps_path):
                    print "No dump files will be downloaded and local folder with dump files not found."
                    print "Please, specify a valid path to local folder containing dump files."
                    print "Program will exit now."
                    sys.exit()

                else:
                    # Attempt to find list of .7z or .xml files to be processed
                    self.paths = glob.glob(os.path.join(dumps_path,
                                                        '*pages-meta-hsitory*.7z'))
                    if not self.paths:
                        self.paths = glob.glob(os.path.join(dumps_path,
                                                            '*pages-meta-hsitory*.xml'))
                        if not self.paths:
                            print "Directory %s does not contain any valid dump file." % dumps_path
                            print "Program will exit now."
                            sys.exit()
            # If not provided explicitly, look for default location of
            # dumps directory
            else:
                dumps_dir = os.path.join("data", self.lang + '_dumps',
                                         self.date)
                # Look up dump files in default directory name
                if not os.path.exists(dumps_dir):
                    print "Default directory %s containing dump files not found." % dumps_dir
                    print "Program will exit now."
                    sys.exit()

                else:
                    self.paths = glob.glob(os.path.join(dumps_dir, '*pages-meta-history*.7z'))
                    if not self.paths:
                        self.paths = glob.glob(os.path.join(dumps_dir,
                                                            '*pages-meta-hsitory*.xml'))
                        if not self.paths:
                            print "Directory %s does not contain any valid dump file." % dumps_dir
                            print "Program will exit now."
                            sys.exit()

        print "paths: " + unicode(self.paths)

        # Create database
        # TODO: Empty correspoding tables if DB already exists
        # or let the user select behaviour with config argument
        if self.DB_exists():
            self.create_DB(complete=False)
        else:
            self.create_DB(complete=True)

        # Complete the queue of paths to be processed and STOP flags for
        # each ETL subprocess
        paths_queue = mp.JoinableQueue()
        for path in self.paths:
            paths_queue.put(path)

        for x in range(self.etl_lines):
            paths_queue.put('STOP')

        for x in range(self.etl_lines):
            new_etl = PageRevisionETL(name="ETL:RevHistory-process-%s" % x,
                                      paths_queue=paths_queue, lang=self.lang,
                                      page_fan=page_fan, rev_fan=rev_fan,
                                      page_cache_size=page_cache_size,
                                      rev_cache_size=rev_cache_size,
                                      db_name=self.db_name,
                                      db_user=self.db_user, db_passw=self.db_passw,
                                      base_port=base_ports[x]+(20*x),
                                      control_port=control_ports[x]+(20*x)
                                      )
            self.etl_list.append(new_etl)

        print "ETL:RevHistory process for page and revision history defined OK."
        print "Proceeding with ETL workflows. This may take time..."
        # Extract, process and load information in local DB
        for etl in self.etl_list:
            etl.start()

        # Wait for ETL lines to finish
        for etl in self.etl_list:
            etl.join()

        # Insert user info after all ETL lines have finished
        # to ensure that all metadata are stored in Redis cache
        # disregarding of the execution order
        data_dir = os.path.join(os.getcwd(), os.path.split(self.paths[0])[0])
        db_users = MySQLDB(host=self.host, port=self.port, user=self.db_user,
                           passwd=self.db_passw, db=self.db_name)
        db_users.connect()
        users_file_to_db(con=db_users, lang=self.lang,
                         log_file=os.path.join(data_dir, 'logs', 'users.log'),
                         tmp_dir=os.path.join(data_dir, 'tmp')
                         )
        db_users.close()
        # TODO: logger; ETL step completed, proceeding with data
        # analysis and visualization
        print "ETL:RevHistory process finished for language %s and date %s" % (
              self.lang, self.date)

        # Create primary keys for all tables
        # TODO: This must also be tracked by main logging module
        print "Now creating primary key indexes in database tables."
        print "This may take a while..."
        db_pks = MySQLDB(host='localhost', port=3306, user=self.db_user,
                         passwd=self.db_passw, db=self.db_name)
        db_pks.connect()
        db_pks.create_pks_revhist()
        db_pks.close()


class PagesLoggingTask(Task):
    """
    A complete, multiprocessing parser of page-logging dump files
    """

    def __init__(self, host, port, db_name, db_user, db_passw, db_engine,
                 lang='scowiki', date=None, etl_lines=1):
        """
        Builder method of class RevisionHistoryRetrieval.
        Arguments:
            - language: code of the Wikipedia language to be processed
            - date: publication date of target dump files collection
        """
        super(PagesLoggingTask, self).__init__(lang=lang, date=date, host=host,
                                               port=port, db_name=db_name,
                                               db_user=db_user,
                                               db_passw=db_passw,
                                               db_engine=db_engine)
        self.etl_lines = etl_lines
        self.etl_list = []

    def create_DB(self, complete=False):
        if complete:
            db_create = MySQLDB(host=self.host, port=self.port,
                                user=self.db_user, passwd=self.db_passw)
            db_create.connect()
            db_create.create_database(self.db_name)
            db_create.close()
        db_schema = MySQLDB(host=self.host, port=self.port, user=self.db_user,
                            passwd=self.db_passw, db=self.db_name)
        db_schema.connect()
        db_schema.create_schema_logitem(engine=self.db_engine)
        db_schema.close()

    def execute(self, log_fan, log_cache_size,
                mirror, download_files, base_ports, control_ports,
                dumps_dir=None):
        """
        Run data retrieval and loading actions.
        Arguments:
            - log_fan = Number of workers to fan out logitem elements parsing
            - db_user = User name to connect to local database
            - db_passw = Password for database user
            - mirror = Base URL of site hosting XML dumps
        """
        # ****************************************************
        if download_files:
            # TODO: Use proper logging module to track execution progress
            # Choose corresponding file downloader and etl wrapper
            print """Downloading new logging dump files from %s,
                     for language %s""" % (mirror, self.lang)
            self.down = download.LoggingDownloader(mirror,
                                                   self.lang, dumps_dir)
            # Donwload latest set of dump files
            self.paths, self.date = self.down.download(self.date)
            if not self.paths:
                print "Error: dump files with pages-logging info not found."
                print "Program will exit now."
                sys.exit()

            print "Got files for lang %s, date: %s" % (self.lang, self.date)

        else:
            # Case of dumps folder provided explicity
            if dumps_dir:
                # Allow specifying relative paths, as well
                abs_dumps_path = os.path.expanduser(dumps_dir)
                dumps_path = os.path.join(abs_dumps_path,
                                          self.lang + '_dumps', self.date)
                # Retrieve path to all available files to feed ETL lines
                if not os.path.exists(dumps_path):
                    print "No dump files will be downloaded and local folder with dump files not found."
                    print "Please, specify a valid path to local folder containing dump files."
                    print "Program will exit now."
                    sys.exit()

                else:
                    # Attempt to find list of *page-logging*.gz or
                    # *page-logging*.xml files to be processed
                    self.paths = glob.glob(os.path.join(dumps_path, '*pages-logging*.gz'))
                    if not self.paths:
                        self.paths = glob.glob(os.path.join(dumps_path,
                                                            '*pages-logging*.xml'))
                        if not self.paths:
                            print "Directory %s does not contain any valid dump file." % dumps_path
                            print "Program will exit now."
                            sys.exit()
            # If not provided explicitly, look for default location of
            # dumps directory
            else:
                dumps_dir = os.path.join("data", self.lang + '_dumps',
                                         self.date)
                # Look up dump files in default directory name
                if not os.path.exists(dumps_dir):
                    print "Default directory %s containing dump files not found." % dumps_dir
                    print "Program will exit now."
                    sys.exit()

                else:
                    self.paths = glob.glob(os.path.join(dumps_dir, '*pages-logging*.gz'))
                    if not self.paths:
                        self.paths = glob.glob(os.path.join(dumps_dir,
                                                            '*pages-logging*.xml'))
                        if not self.paths:
                            print "Directory %s does not contain any valid dump file." % dumps_dir
                            print "Program will exit now."
                            sys.exit()

        print "paths: " + unicode(self.paths)

        # Create database if not exists
        # empty logging table otherwise
        if self.DB_exists():
            self.create_DB(complete=False)
        else:
            self.create_DB(complete=True)

        new_etl = LoggingETL(name="ETL:Logging-process-0",
                             path=self.paths, lang=self.lang,
                             log_fan=log_fan,
                             log_cache_size=log_cache_size,
                             db_name=self.db_name,
                             db_user=self.db_user, db_passw=self.db_passw,
                             base_port=base_ports[0]+(30),
                             control_port=control_ports[0]+(30)
                             )
        print "ETL:Logging process for administrative records defined OK."
        print "Proceeding with ETL workflow. This may take time..."
        # Extract, process and load information in local DB
        new_etl.start()
        # Wait for ETL line to finish
        new_etl.join()
        # TODO: logger; ETL step completed, proceeding with data
        # analysis and visualization
        print "ETL:Logging process finished for language %s and date %s" % (
              self.lang, self.date)

        # Create primary keys for all tables
        # TODO: This must also be tracked by official logging module
        print "Now creating primary key indexes in database tables."
        print "This may take a while..."
        db_pks = MySQLDB(host='localhost', port=3306, user=self.db_user,
                         passwd=self.db_passw, db=self.db_name)
        db_pks.connect()
        db_pks.create_pks_logitem()
        db_pks.close()
