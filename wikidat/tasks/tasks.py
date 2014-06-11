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

from wikidat.sources.etl import PageRevisionETL
from download import RevHistDownloader
from wikidat.utils.dbutils import MySQLDB
import multiprocessing as mp
import os
import sys
import glob


class Task(object):
    """
    Abstract class defining common interface for all tasks
    """

    def __init__(self, lang, date=None):
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


class RevisionHistoryTask(Task):
    """
    A complete, multiprocessing parser of full revision history dump files
    """

    def __init__(self, lang='scowiki', date=None, etl_lines=1):
        """
        Builder method of class RevisionHistoryRetrieval.
        Arguments:
            - language: code of the Wikipedia language to be processed
            - date: publication date of target dump files collection
        """
        super(RevisionHistoryTask, self).__init__(lang=lang, date=date)
        self.etl_lines = etl_lines
        self.etl_list = []

    # TODO: include args detect_FA, detect_FLIST, detect_GA
    # and implement flow control in process_revision
    def execute(self, page_fan, rev_fan, host, port,
                db_name, db_user, db_passw, db_engine,
                mirror, download_files,
                base_ports, control_ports,
                dumps_dir=None):
        """
        Run data retrieval and loading actions
        """
        if download_files:
            # TODO: Use proper logging module to track execution progress
            # Choose corresponding file downloader and etl wrapper
            print "Downloading new dump files from %s, for language %s" % (
                  mirror, self.lang)
            self.down = RevHistDownloader(mirror, self.lang)
            # Donwload latest set of dump files
            self.paths, self.date = self.down.download(self.date)
            print "Got files for lang %s, date: %s" % (self.lang, self.date)

            #db_name = self.lang + '_' + self.date.strip('/')

        else:
            # Case of dumps folder provided explicity
            if dumps_dir:
                # Allow specifying relative paths, as well
                dumps_path = os.path.expanduser(dumps_dir)
                # Retrieve path to all available files to feed ETL lines
                if not os.path.exists(dumps_path):
                    print "No dump files will be downloaded and local folder "
                    print "with dump files not found. Please, specify a "
                    print "valid path to local folder containing dump files."
                    print "Program will exit now."
                    sys.exit()

                else:
                    self.paths = glob.glob(dumps_path + '*.7z')
            else:
                dumps_dir = os.path.join(self.lang + '_dumps', self.date)
                # Look up dump files in default directory name
                if not os.path.exists(dumps_dir):
                    print "Default directory %s" % dumps_dir
                    print " containing dump files not found."
                    print "Program will exit now."
                    sys.exit()

                else:
                    self.paths = glob.glob(dumps_dir + '/*.7z')

        print "paths: " + unicode(self.paths)

        # DB SCHEMA PREPARATION
        db_create = MySQLDB(host=host, port=port, user=db_user,
                            passwd=db_passw)
        db_create.connect()
        db_create.create_database(db_name)
        db_create.close()
        db_schema = MySQLDB(host=host, port=port, user=db_user,
                            passwd=db_passw, db=db_name)
        db_schema.connect()
        db_schema.create_schema(engine=)
        db_schema.close()

        # Complete the queue of paths to be processed and STOP flags for
        # each ETL subprocess
        paths_queue = mp.JoinableQueue()
        for path in self.paths:
            paths_queue.put(path)

        for x in range(self.etl_lines):
            paths_queue.put('STOP')

        for x in range(self.etl_lines):
            new_etl = PageRevisionETL(name="ETL process - %s" % x,
                                      paths_queue=paths_queue, lang=self.lang,
                                      page_fan=page_fan, rev_fan=rev_fan,
                                      db_name=db_name,
                                      db_user=db_user, db_passw=db_passw,
                                      base_port=base_ports[x]+(20*x),
                                      control_port=control_ports[x]+(20*x))
            self.etl_list.append(new_etl)
        print "ETL process for page and revision history defined OK."
        print "Proceeding with ETL workflows. This may take time..."
        # Extract, process and load information in local DB
        for etl in self.etl_list:
            etl.start()

        # Wait for ETL lines to finish
        for etl in self.etl_list:
            etl.join()

        # TODO: logger; ETL step completed, proceeding with data
        # analysis and visualization
        print "ETL process finished for language %s and date %s" % (
              self.lang, self.date)

        # Create primary keys for all tables
        # TODO: This must also be tracked by official logging module
        print "Now creating primary key indexes in database tables."
        print "This may take a while..."
        db_pks = MySQLDB(host='localhost', port=3306, user=db_user,
                         passwd=db_passw, db=db_name)
        db_pks.connect()
        db_pks.create_pks()
        db_pks.close()
