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

    def execute(self, page_fan, rev_fan, db_user, db_passw,
                mirror='http://dumps.wikimedia.org/'):
        """
        Run data retrieval and loading actions
        """
        # TODO: Use proper logging module to track execution progress
        # Choose corresponding file downloader and etl wrapper
        print "Downloading new dump files from %s, for language %s" % (
              mirror, self.lang)
        self.down = RevHistDownloader(mirror, self.lang)
        # Donwload latest set of dump files
        self.paths, self.date = self.down.download(self.date)
        print "Downloaded files for lang %s, date: %s" % (self.lang, self.date)

        db_name = self.lang + '_' + self.date.strip('/')

        self.paths = ['/home/jfelipe/Development/spyder/WikiDAT/wikidat/enwiki_dumps/20140502/enwiki-20140502-pages-meta-history4.xml-p000100559p000104998.7z',
                      '/home/jfelipe/Development/spyder/WikiDAT/wikidat/enwiki_dumps/20140502/enwiki-20140502-pages-meta-history1.xml-p000000010p000003263.7z',
                      '/home/jfelipe/Development/spyder/WikiDAT/wikidat/enwiki_dumps/20140502/enwiki-20140502-pages-meta-history1.xml-p000003264p000005405.7z']

        print "paths: " + unicode(self.paths)

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
                                      base_port=20000+(20*x),
                                      control_port=30000+(20*x))
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
