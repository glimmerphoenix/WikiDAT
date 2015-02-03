# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:14:09 2014

@author: jfelipe
"""
import time
from .data_item import DataItem
import logging
import csv
import os


class Page(DataItem):
    """
    Models Page elements in Wikipedia database dumps
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor method for Page objects. Must forward params to
        parent class DataItem (mandatory inheritance)

        The following keys must be populated:
        ---------
        * id: Unique numeric identifier for this page
        * ns: Namespace code for this page (0 is main)
        * title: Page title (unicode string)
        * restrictions: List of applicable restrictions to this page
        """
        super(Page, self).__init__(*args, **kwargs)


def process_pages(pages_iter):
    """
    Process an iterator of Page objects and yields unicode tuples to be
    appended to an extended insert for DB storage
    """
    # Build component for extended insert with info about this Page
    for page in pages_iter:
        new_page_insert = "".join(["(", page['id'], ",",
                                   page['ns'], ",'",
                                   page['title'].
                                   replace("\\", "\\\\").
                                   replace("'", "\\'").
                                   replace('"', '\\"'), "',"])
        if 'restrictions' in page:
            new_page_insert = "".join([new_page_insert,
                                       "'", page['restrictions'],
                                       "')"])
        else:
            new_page_insert = "".join([new_page_insert, "'')"])

        yield new_page_insert


def pages_to_file(pages_iter):
    """
    Process an iterator of Page objects and yields unicode tuples to be
    stored in a temp file for later bulk data load in DB.
    """
    for page in pages_iter:
        page_insert = (int(page['id']), int(page['ns']),
                       page['title'],
                       (page['restrictions'] if 'restrictions' in page
                        else u'NULL'),
                       )
        yield page_insert


def pages_file_to_db(pages_iter, con=None, log_file=None,
                     tmp_dir=None, file_rows=1000000, etl_prefix=None):
    """
    Process page insert items received from iterator. Page inserts are stored
    in a temp file, then a bulk data load is triggered in MySQL.
    """
    insert_rows = 0
    total_pages = 0
    logging.basicConfig(filename=log_file, level=logging.DEBUG)

    print("Starting page data loading at %s." % (
        time.strftime("%Y-%m-%d %H:%M:%S %Z",
                      time.localtime())))
    logging.info("Starting page data loading at %s." % (
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))

    insert_pages = """LOAD DATA INFILE '%s' INTO TABLE page
                      FIELDS OPTIONALLY ENCLOSED BY '"'
                      TERMINATED BY '\t' ESCAPED BY '"'
                      LINES TERMINATED BY '\n'"""

    path_file_page = os.path.join(tmp_dir, etl_prefix + '_page.csv')
    # Delete previous versions of tmp files if present
    if os.path.isfile(path_file_page):
        os.remove(path_file_page)

    for page in pages_iter:
        total_pages += 1

        if insert_rows == 0:
            file_page = open(path_file_page, 'w')
            writer = csv.writer(file_page, dialect='excel-tab',
                                lineterminator='\n')
        # Write data to tmp file
        try:
            writer.writerow([s if isinstance(s, str)
                             else str(s) for s in page])
        except Exception as e:
            print(e)
            print(page)

        insert_rows += 1

        # Call MySQL to load data from file and reset rows counter
        if insert_rows == file_rows:
            # Insert in DB
            file_page.close()
            con.send_query(insert_pages % path_file_page)
            insert_rows = 0
            # No need to delete tmp files, as they are empty each time we
            # open them again for writing

    file_page.close()
    con.send_query(insert_pages % path_file_page)
    # Clean tmp files
#    os.remove(path_file_page)

    logging.info("END: %s pages processed %s." % (
                 total_pages,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))
    print("END: %s pages processed %s." % (
        total_pages, time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                   time.localtime())))


def store_pages_db(pages_iter, con=None, log_file=None, size_cache=500):
    """
    Class method, processor to insert Page info in DB

    Arguments:
    ----------
    pages_iter = iterator over Page elements to be stored in DB
    """
    page_insert_rows = 0
    total_pages = 0
    logging.basicConfig(filename=log_file, level=logging.DEBUG)

    print("Starting data loading at %s." % (
        time.strftime("%Y-%m-%d %H:%M:%S %Z",
                      time.localtime())))
    logging.info("Starting data loading at %s." % (
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))

    for new_page_insert in pages_iter:
        # Build extended insert for Page objects
        if page_insert_rows == 0:
            page_insert = "".join(["INSERT INTO page ",
                                   "VALUES", new_page_insert])
            page_insert_rows += 1

        elif page_insert_rows <= size_cache:
            page_insert = "".join([page_insert, ",",
                                   new_page_insert])
            # Update rows counter
            page_insert_rows += 1
        else:
            con.send_query(page_insert)

            page_insert = "".join(["INSERT INTO page ",
                                   "VALUES", new_page_insert])
            # Update rows counter
            page_insert_rows = 1

        total_pages += 1
        #if total_pages % 1000 == 0:
            #print "%s pages processed %s." % (total_pages,
                    #time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                  #time.localtime()) )

    # Send last extended insert for page
    con.send_query(page_insert)
    logging.info("END: %s pages processed %s." % (
                 total_pages,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))
    print("END: %s pages processed %s." % (
        total_pages, time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                   time.localtime())))
