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


class RevisionHistoryWorkflow(object):
    """
    Models workflow to import information from Wikipedia
    database dump files
    """

    def __init__(self):
        """
        Initialize new Wikipedia workflow
        """
        pass

    def run(self, path, page_fan, rev_fan, lang, db_name, db_user,
            db_passw):
        """
        Define and run workflow to import data from dump files

        The data loading workflow is composed of a number of processor
        elements, which can be:

            - Producers: raw input data --> input element queue
            - ConsumerProducer: input element queue --> insert db queue
            - Consumer: insert db queue --> database (MySQL)
        """
        # Create waiting queues for elements to be processed
        page_queue = mp.JoinableQueue()
        revision_queue = mp.JoinableQueue()
        logitem_queue = mp.JoinableQueue()
        user_queue = mp.JoinableQueue()

        page_insert_queue = mp.JoinableQueue()
        rev_insert_queue = mp.JoinableQueue()

        # Create and initialize shared users cache
        #mgr = mp.Manager()
        #users_cache = mgr.dict()
        #users_cache['-1'] = 'NA'
        #users_cache['0'] = 'Anonymous'

        #page_fan = 1
        #rev_fan = 1

        start = time.time()
        print "Starting program at %s" % (
            time.strftime("%Y-%m-%d %H:%M:%S %Z",
                          time.localtime()))

        db_ns = MySQLDB(host='localhost', port=3306, user=db_user,
                        passwd=db_passw, db=db_name)
        db_ns.connect()
        #db_ns.autocommit(True)
        #cursor_ns = db_ns.cursor()
        #log_file_ns = 'error_ns_' + lang + '.log'

        db_pages = MySQLDB(host='localhost', port=3306,
                           user=db_user, passwd=db_passw,
                           db=db_name)
        db_pages.connect()

        db_revs = MySQLDB(host='localhost', port=3306, user=db_user,
                          passwd=db_passw, db=db_name)
        db_revs.connect()

        # Start subprocess to extract elements from revision dump file
        dump_file = DumpFile(path)
        xml_reader = Producer(name='xml_reader',
                              target=dump_file.extract_elements,
                              out_page_queue=page_queue,
                              out_rev_queue=revision_queue,
                              page_consumers=page_fan,
                              rev_consumers=rev_fan)

        print "Starting data extraction from XML revision history file"
        print "Dump file: " + path
        xml_reader.start()

        # List to keep tracking of page and revision workers
        workers = []
        db_workers_revs = []
        # Create and start page processes
        for worker in range(page_fan):
            print "page worker num. ", worker, "started"
            process_page = Processor(name='process_page_' + unicode(page_fan),
                                     target=Page().process,
                                     #kwargs=dict(LIVE=False),
                                     input_queue=page_queue,
                                     output_queue=page_insert_queue,
                                     producers=1, consumers=1)
            process_page.start()
            workers.append(process_page)

        # Create and start revision processes
        for worker in range(rev_fan):
            print "revision worker num. ", worker, "started"

            db_wrev = MySQLDB(host='localhost', port=3306, user=db_user,
                              passwd=db_passw, db=db_name)
            db_wrev.connect()

            process_revision = Processor(name='process_revision_' + unicode(rev_fan),
                                         target=Revision().process,
                                         kwargs=dict(
                                             con=db_wrev,
                                             lang=lang),
                                         input_queue=revision_queue,
                                         output_queue=rev_insert_queue,
                                         producers=1, consumers=1)
            process_revision.start()
            workers.append(process_revision)
            db_workers_revs.append(db_wrev)

        page_insert_db = Consumer(name='insert_page',
                                  target=Page().store_db,
                                  kwargs=dict(con=db_pages),
                                  input_queue=page_insert_queue,
                                  producers=page_fan)

        rev_insert_db = Consumer(name='insert_revision',
                                 target=Revision().store_db,
                                 kwargs=dict(con=db_revs),
                                 input_queue=rev_insert_queue,
                                 producers=rev_fan)

        print "And inserting in DB..."
        page_insert_db.start()
        rev_insert_db.start()

        print "Waiting for all processes to finish..."
        xml_reader.join()
        for w in workers:
            w.join()
        page_insert_db.join()
        rev_insert_db.join()

        page_queue.join()
        revision_queue.join()
        page_insert_queue.join()
        rev_insert_queue.join()

        end = time.time()
        print "All tasks done in %.4f sec." % ((end-start)/1.)

        db_ns.close()
        db_pages.close()
        db_revs.close()
        for dbcon in db_workers_revs:
            dbcon.close()

if __name__ == '__main__':
    path = sys.argv[1]
    page_fan = int(sys.argv[2])
    rev_fan = int(sys.argv[3])
    lang = sys.argv[4]
    db_name = sys.argv[5]
    db_user = sys.argv[6]
    db_passw = sys.argv[7]

    workflow = RevisionHistoryWorkflow()
    workflow.run(path, page_fan, rev_fan, lang,
                 db_name, db_user, db_passw)
