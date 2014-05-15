# -*- coding: utf-8 -*-
"""
Created on Sun Mar 30 18:09:16 2014

@author: jfelipe
"""
import multiprocessing as mp
import sys
import time
from processors import Producer, Processor, Consumer
from page import Page
from revision import Revision
from dump import DumpFile
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

        # Create waiting queues for elements to be processed
        self.page_queue = mp.JoinableQueue()  # Page elements
        self.revision_queue = mp.JoinableQueue()  # Revision elements
        self.user_queue = mp.JoinableQueue()  # User ids and names
        self.page_insert_queue = mp.JoinableQueue()  # Ext. insert blocks page
        self.rev_insert_queue = mp.JoinableQueue()  # Ext. insert blocks revs

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
        #db_ns.autocommit(True)
        #cursor_ns = db_ns.cursor()
        #log_file_ns = 'error_ns_' + lang + '.log'

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
                                  target=dump_file.extract_elements,
                                  out_page_queue=self.page_queue,
                                  out_rev_queue=self.revision_queue,
                                  page_consumers=self.page_fan,
                                  rev_consumers=self.rev_fan)

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
                                         target=Page().process,
                                         #kwargs=dict(LIVE=False),
                                         input_queue=self.page_queue,
                                         output_queue=self.page_insert_queue,
                                         producers=1, consumers=1)
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
                                             target=Revision().process,
                                             kwargs=dict(
                                                 con=db_wrev,
                                                 lang=self.lang),
                                             input_queue=self.revision_queue,
                                             output_queue=self.rev_insert_queue,
                                             producers=1, consumers=1)
                process_revision.start()
                workers.append(process_revision)
                db_workers_revs.append(db_wrev)
    
            page_insert_db = Consumer(name='insert_page',
                                      target=Page().store_db,
                                      kwargs=dict(con=db_pages),
                                      input_queue=self.page_insert_queue,
                                      producers=self.page_fan)

            rev_insert_db = Consumer(name='insert_revision',
                                     target=Revision().store_db,
                                     kwargs=dict(con=db_revs),
                                     input_queue=self.rev_insert_queue,
                                     producers=self.rev_fan)

            print "And inserting in DB..."
            page_insert_db.start()
            rev_insert_db.start()

            print "Waiting for all processes to finish..."
            xml_reader.join()
            for w in workers:
                w.join()
            page_insert_db.join()
            rev_insert_db.join()

            self.page_queue.join()
            self.revision_queue.join()
            self.page_insert_queue.join()
            self.rev_insert_queue.join()

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
