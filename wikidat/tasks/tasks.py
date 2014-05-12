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


class RevisionHistoryTask(object):
    """
    A complete, multiprocessing parser of full revision history dump files
    """

    def __init__(self, lang, date=None):
        """
        Builder method of class RevisionHistoryRetrieval.
        Arguments:
            - language: code of the Wikipedia language to be processed
            - date: publication date of target dump files collection
        """
        super(RevisionHistoryTask, self).__init__(lang=lang, date=date)

    def execute(self, path, page_fan, rev_fan, lang,
                db_name, db_user, db_passw):
        """
        Run data retrieval and loading actions
        """
        # Choose corresponding file downloader and etl wrapper
        self.down = RevHistDownloader(mirror='http://dumps.wikimedia.org',
                                      lang='scowiki')
        self.etl = PageRevisionETL(path=path, page_fan=page_fan,
                                   rev_fan=rev_fan, lang=lang,
                                   db_name=db_name, db_user=db_user,
                                   db_passw=db_passw)

        # Donwload latest set of dump files
        self.down.download()

        # Extract, process and load information in local DB
        self.etl.run()