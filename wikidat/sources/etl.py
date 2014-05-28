# -*- coding: utf-8 -*-
"""
Created on Sun Mar 30 18:09:16 2014

@author: jfelipe
"""
# import multiprocessing as mp
import sys
import time
from processors import Producer, Processor, Consumer
from dump import DumpFile, process_xml
from page import process_pages, store_pages_db
from revision import process_revs, store_revs_db
from wikidat.utils.dbutils import MySQLDB


class ETL(object):
    """
    Abstract class defining common behaviour for all ETL (Extraction,
    Tranformation and Load) workflows with Wikipedia data
    """

    def __init__(self, paths=None, page_fan=1, rev_fan=3, lang=None,
                 db_name=None, db_user=None, db_passw=None):
        """
        Initialize new worfklow
        """
        self.paths = paths
        self.page_fan = page_fan; self.rev_fan = rev_fan; self.lang = lang
        self.db_name = db_name
        self.db_user = db_user
        self.db_passw = db_passw


class PageRevisionETL(ETL):
    """
    Models workflow to import page and revision history data from Wikipedia
    database dump files
    """

    def __init__(self, paths=None, lang=None, page_fan=1, rev_fan=3,
                 db_name=None, db_user=None, db_passw=None):
        """
        Initialize new PageRevision workflow
        """
        super(PageRevisionETL,
              self).__init__(paths=paths, page_fan=page_fan,
                             rev_fan=rev_fan, lang=lang, db_name=db_name,
                             db_user=db_user, db_passw=db_passw)

        # DB SCHEMA PREPARATION
        db_create = MySQLDB(host='localhost', port=3306, user=db_user,
                            passwd=db_passw)
        db_create.connect()
        db_create.create_database(self.db_name)
        db_create.close()
        db_schema = MySQLDB(host='localhost', port=3306, user=db_user,
                            passwd=db_passw, db=self.db_name)
        db_schema.connect()
        db_schema.create_schema()
        db_schema.close()

    def run(self):
        """
        Execute workflow to import revision history data from dump files

        The data loading workflow is composed of a number of processor
        elements, which can be:

            - Producers: raw input data --> input element queue
            - ConsumerProducer: input element queue --> insert db queue
            - Consumer: insert db queue --> database (MySQL/MariaDB)
        """
        start = time.time()
        print "Starting PageRevisionETL workflow at %s" % (
              time.strftime("%Y-%m-%d %H:%M:%S %Z",
                            time.localtime()))

        db_ns = MySQLDB(host='localhost', port=3306, user=self.db_user,
                        passwd=self.db_passw, db=self.db_name)
        db_ns.connect()

        db_pages = MySQLDB(host='localhost', port=3306,
                           user=self.db_user, passwd=self.db_passw,
                           db=self.db_name)
        db_pages.connect()

        db_revs = MySQLDB(host='localhost', port=3306, user=self.db_user,
                          passwd=self.db_passw, db=self.db_name)
        db_revs.connect()

        # DATA EXTRACTION
        for path in self.paths:
            # Start subprocess to extract elements from revision dump file
            dump_file = DumpFile(path)
            xml_reader = Producer(name='xml_reader',
                                  target=process_xml,
                                  kwargs=dict(
                                      dump_file=dump_file),
                                  page_consumers=self.page_fan,
                                  rev_consumers=self.rev_fan,
                                  push_pages_port=5557,
                                  push_revs_port=5558)

            print "Starting data extraction from XML revision history file"
            print "Dump file: " + path
            xml_reader.start()

            # List to keep tracking of page and revision workers
            workers = []
            db_workers_revs = []
            # Create and start page processes
            for worker in range(self.page_fan):
                print "page worker num. ", worker, "started"
                process_page = Processor(name='process_page_' + unicode(self.page_fan),
                                         target=process_pages,
                                         producers=1, consumers=1,
                                         pull_port=5557,
                                         push_port=5559)
                process_page.start()
                workers.append(process_page)

            # Create and start revision processes
            for worker in range(self.rev_fan):
                print "revision worker num. ", worker, "started"

                db_wrev = MySQLDB(host='localhost', port=3306, user=self.db_user,
                                  passwd=self.db_passw, db=self.db_name)
                db_wrev.connect()

                process_revision = Processor(name="".join(['process_revision_',
                                                           unicode(self.rev_fan)]),
                                             target=process_revs,
                                             kwargs=dict(
                                                 con=db_wrev,
                                                 lang=self.lang),
                                             producers=1, consumers=1,
                                             pull_port=5558,
                                             push_port=5560)
                process_revision.start()
                workers.append(process_revision)
                db_workers_revs.append(db_wrev)
    
            page_insert_db = Consumer(name='insert_page',
                                      target=store_pages_db,
                                      kwargs=dict(con=db_pages),
                                      producers=self.page_fan,
                                      pull_port=5559)

            rev_insert_db = Consumer(name='insert_revision',
                                     target=store_revs_db,
                                     kwargs=dict(con=db_revs),
                                     producers=self.rev_fan,
                                     pull_port=5560)

            print "And inserting in DB..."
            page_insert_db.start()
            rev_insert_db.start()

            print "Waiting for all processes to finish..."
            xml_reader.join()
            for w in workers:
                w.join()
            page_insert_db.join()
            rev_insert_db.join()

        end = time.time()
        print "All tasks done in %.4f sec." % ((end-start)/1.)

        db_ns.close()
        db_pages.close()
        db_revs.close()
        for dbcon in db_workers_revs:
            dbcon.close()

        # Finally, create primary keys for all tables
        # TODO: This must also be tracked by official logging module
        print "Now creating primary key indexes in database tables."
        print "This may take a while..."
        db_pks = MySQLDB(host='localhost', port=3306, user=self.db_user,
                         passwd=self.db_passw, db=self.db_name)
        db_pks.connect()
        db_pks.create_pks()
        db_pks.close()


class PageRevisionMetaETL(ETL):
    """
    Implements workflow to extract and store metadata for pages and
    revisions (stub-meta-history.xml files)
    """
    pass


class LoggingETL(ETL):
    """
    Implements workflow to extract and store information from logged
    actions in MediaWiki. For instance, user blocks, page protections,
    new users, flagged revisions reviews, etc.
    """
    pass


class SQLDumpETL(ETL):
    """
    Implements workflow to load native SQL dump files (compressed)
    """
    pass


if __name__ == '__main__':
    path = sys.argv[1]
    page_fan = int(sys.argv[2])
    rev_fan = int(sys.argv[3])
    lang = sys.argv[4]
    db_name = sys.argv[5]
    db_user = sys.argv[6]
    db_passw = sys.argv[7]

    workflow = PageRevisionETL(path, page_fan, rev_fan, lang, db_name,
                               db_user, db_passw)
    workflow.run()
