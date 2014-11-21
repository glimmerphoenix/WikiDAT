# -*- coding: utf-8 -*-
"""
Created on Sun Mar 30 18:09:16 2014

@author: jfelipe
"""
# import multiprocessing as mp
import sys
import os
import time
import multiprocessing as mp
from processors import Producer, Processor, Consumer
from dump import DumpFile, process_xml
from page import process_pages_to_file, store_pages_file_db
from revision import process_revs_to_file, store_revs_file_db
from wikidat.utils.dbutils import MySQLDB


class ETL(mp.Process):
    """
    Abstract class defining common behaviour for all ETL (Extraction,
    Tranformation and Load) workflows with Wikipedia data
    """

    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, paths_queue=None, page_fan=1, rev_fan=3,
                 lang=None, db_name=None, db_user=None, db_passw=None):
        """
        Initialize new worfklow
        """
        super(ETL, self).__init__(name=name)
        self.target = target
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}

        self.paths_queue = paths_queue
        self.page_fan = page_fan; self.rev_fan = rev_fan; self.lang = lang
        self.db_name = db_name
        self.db_user = db_user
        self.db_passw = db_passw


class PageRevisionETL(ETL):
    """
    Models workflow to import page and revision history data from Wikipedia
    database dump files
    """

    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, paths_queue=None, lang=None, page_fan=1,
                 rev_fan=3, page_cache_size=1000000, rev_cache_size=1000000,
                 db_name=None, db_user=None, db_passw=None,
                 base_port=None, control_port=None):
        """
        Initialize new PageRevision workflow
        """
        super(PageRevisionETL,
              self).__init__(group=None, target=None, name=name, args=None,
                             kwargs=None, paths_queue=paths_queue,
                             page_fan=page_fan, rev_fan=rev_fan, lang=lang,
                             db_name=db_name,
                             db_user=db_user, db_passw=db_passw)

        self.page_cache_size = page_cache_size
        self.rev_cache_size = rev_cache_size
        self.base_port = base_port
        self.control_port = control_port

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
        for path in iter(self.paths_queue.get, 'STOP'):
            # Start subprocess to extract elements from revision dump file
            dump_file = DumpFile(path)
            xml_reader = Producer(name='_'.join([self.name, 'xml_reader']),
                                  target=process_xml,
                                  kwargs=dict(
                                      dump_file=dump_file),
                                  page_consumers=self.page_fan,
                                  rev_consumers=self.rev_fan,
                                  push_pages_port=self.base_port,
                                  push_revs_port=self.base_port+1,
                                  control_port=self.control_port)

            print "Starting data extraction from XML revision history file"
            print "Dump file: " + path
            xml_reader.start()

            # List to keep tracking of page and revision workers
            workers = []
            db_workers_revs = []
            # Create and start page processes
            for worker in range(self.page_fan):
                print "page worker num. ", worker, "started"
                process_page = Processor(name='_'.join([self.name,
                                                        'process_page',
                                                        unicode(worker)]),
                                         target=process_pages_to_file,
                                         producers=1, consumers=1,
                                         pull_port=self.base_port,
                                         push_port=self.base_port+2,
                                         control_port=self.control_port)
                process_page.start()
                workers.append(process_page)

            # Create and start revision processes
            for worker in range(self.rev_fan):
                print "revision worker num. ", worker, "started"

                db_wrev = MySQLDB(host='localhost', port=3306,
                                  user=self.db_user,
                                  passwd=self.db_passw, db=self.db_name)
                db_wrev.connect()

                process_revision = Processor(name='_'.join([self.name,
                                                            'process_revision',
                                                            unicode(worker)]),
                                             target=process_revs_to_file,
                                             kwargs=dict(
                                                 con=db_wrev,
                                                 lang=self.lang),
                                             producers=1, consumers=1,
                                             pull_port=self.base_port+1,
                                             push_port=self.base_port+3,
                                             control_port=self.control_port)
                process_revision.start()
                workers.append(process_revision)
                db_workers_revs.append(db_wrev)

            # Create directory for logging files if it does not exist
            log_dir = os.path.join(os.path.split(path)[0], 'logs')
            tmp_dir = os.path.join(os.getcwd(),
                                   os.path.split(path)[0], 'tmp')
            file_name = os.path.split(path)[1]

            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)
            log_file = os.path.join(log_dir, file_name + '.log')

            page_insert_db = Consumer(name='_'.join([self.name,
                                                     'insert_page']),
                                      target=store_pages_file_db,
                                      kwargs=dict(con=db_pages,
                                                  log_file=log_file,
                                                  tmp_dir=tmp_dir,
                                                  file_rows=self.page_cache_size,
                                                  etl_prefix=self.name),
                                      producers=self.page_fan,
                                      pull_port=self.base_port+2)

            rev_insert_db = Consumer(name='_'.join([self.name,
                                                    'insert_revision']),
                                     target=store_revs_file_db,
                                     kwargs=dict(con=db_revs,
                                                 log_file=log_file,
                                                 tmp_dir=tmp_dir,
                                                 file_rows=self.rev_cache_size,
                                                 etl_prefix=self.name),
                                     producers=self.rev_fan,
                                     pull_port=self.base_port+3)

            print "And inserting in DB..."
            page_insert_db.start()
            rev_insert_db.start()

            print "Waiting for all processes to finish..."
            xml_reader.join()
            for w in workers:
                w.join()
            page_insert_db.join()
            rev_insert_db.join()

            # Mark this path as done
            self.paths_queue.task_done()

        # Mark STOP message as processed and finish
        self.paths_queue.task_done()

        end = time.time()
        print "All tasks done in %.4f sec." % ((end-start)/1.)

        db_ns.close()
        db_pages.close()
        db_revs.close()
        for dbcon in db_workers_revs:
            dbcon.close()


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
