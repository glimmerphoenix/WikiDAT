# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:13:42 2014

@author: jfelipe
"""
import hashlib
import time
from wikidat.utils import maps
from data_item import DataItem
import csv
import os

import logging


class Revision(DataItem):
    """
    Models Revision elements in Wikipedia dump files
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor method for Revision objects. Must forward params to
        parent class DataItem (mandatory inheritance)

        The following keys must be populated:
        ---------
        * id: Unique numeric identifier for this revision
        * rev_user: Numeric identifier of author of this revision (0 is anon)
        * rev_timestamp: Timestamp when this rev was saved in database
        * rev_len: Length in bytes of this revision
        * ... (other params)
        """
        super(Revision, self).__init__(*args, **kwargs)


def process_revs(rev_iter, con=None, lang=None):
    """
    Process iterator of Revision objects extracted from dump files
    :Parameters:
        - rev_iter: iterator of Revision objects
        - lang: identifier of Wikipedia language edition from which this
        element comes from (e.g. frwiki, eswiki, dewiki...)
    """
    # Get tags to identify Featured Articles, Featured Lists and
    # Good Articles
    # lang = 'dewiki'
    # new_user = False

    if ((lang in maps.FA_RE) and (lang in maps.FLIST_RE) and
            (lang in maps.GA_RE)):
        fa_pat = maps.FA_RE[lang]
        flist_pat = maps.FLIST_RE[lang]
        ga_pat = maps.GA_RE[lang]
    else:
        raise RuntimeError('Unsupported language ' + lang)

    for rev in rev_iter:
        contrib_dict = rev['contrib_dict']

        # ### TEXT-RELATED OPERATIONS ###
        # Calculate SHA-256 hash, length of revision text and check
        # for REDIRECT
        # TODO: Inspect why there are pages without text

        # Stores SHA-256 hash of revision text
        text_hash = hashlib.sha256()
        # Default values to 0. These fields will be set below if any of the
        # target patterns is detected
        rev['redirect'] = '0'
        rev['is_fa'] = '0'
        rev['is_flist'] = '0'
        rev['is_ga'] = '0'

        if rev['text'] is not None:
            text = rev['text'].encode('utf-8')
            text_hash.update(text)
            rev['len_text'] = str(len(text))

            # Detect pattern for redirect pages
            if rev['text'][0:9].upper() == '#REDIRECT':
                rev['redirect'] = '1'

            # FA and FList detection
            # Currently 39 languages are supported regarding FA detection
            # We only enter pattern matching for revisions of pages in
            # main namespace
            if rev['ns'] == '0':
                if fa_pat is not None:
                    mfa = fa_pat.search(rev['text'])
                    # Case of standard language, one type of FA template
                    if (mfa is not None and len(mfa.groups()) == 1):
                        rev['is_fa'] = '1'
                    # Case of fawiki or cawiki, 2 types of FA templates
                    # Possible matches: (A, None) or (None, B)
                    if lang == 'fawiki' or lang == 'cawiki':
                        if (mfa is not None and len(mfa.groups()) == 2 and
                                (mfa.groups()[1] is None or
                                 mfa.groups()[0] is None)):
                                    rev['is_fa'] = '1'

                # Check if FLIST is supported in this language, detect if so
                if flist_pat is not None:
                    mflist = flist_pat.search(rev['text'])
                    if mflist is not None and len(mflist.groups()) == 1:
                        rev['is_flist'] = '1'

                # Check if GA is supported in this language, detect if so
                if ga_pat is not None:
                    mga = ga_pat.search(rev['text'])
                    if mga is not None and len(mga.groups()) == 1:
                        rev['is_ga'] = '1'
        # Compute hash for empty text here instead of in default block above
        # This way, we avoid computing the hash twice for revisions with text
        else:
            rev['len_text'] = '0'
            text_hash.update('')

        # SQL query building
        # Build extended inserts for people, revision and revision_hash
        rev_insert = "".join(["(", rev['id'], ",",
                              rev['page_id'], ","])

        rev_hash = "".join(["(", rev['id'], ",",
                            rev['page_id'], ","])

        # Check that revision has a valid contributor
        # and build extended insert for people
        if len(contrib_dict) > 0:
            # Anonymous user
            if 'ip' in contrib_dict:
                # new_user = False
                rev_insert = "".join([rev_insert, "0,"])
                rev_hash = "".join([rev_hash, "0,"])

                # TODO: Upload IP info to people table

            # Registered user
            else:
                rev_insert = "".join([rev_insert,
                                      contrib_dict['id'], ","])
                rev_hash = "".join([rev_hash,
                                    contrib_dict['id'], ","])
                query_user = "".join(["SELECT rev_user, ",
                                      "rev_user_text ",
                                      "FROM people WHERE rev_user = ",
                                      contrib_dict['id']])
                # If this is a new user add info to people table
                known_user = con.execute_query(query_user)
                if known_user is None:
                    # Standard case of new username
                    if contrib_dict['username'] is not None:
                        # Activate flag to insert new user info in DB
                        # new_user = True
                        # Update common users cache
                        user_insert = "".join(["(",
                                               contrib_dict['id'],
                                               ",'",
                                               contrib_dict['username'].
                                               replace("\\", "\\\\").
                                               replace("'", "\\'").
                                               replace('"', '\\"'),
                                               "')"])
                        con.send_query("".join(["INSERT INTO people ",
                                                "VALUES",
                                                user_insert])
                                       )

                    # Handle strange case of new user w/o username
                    else:
                        user_insert = "".join(["(", contrib_dict['id'],
                                               "NULL)"])
                        con.send_query("".join(["INSERT INTO people ",
                                                "VALUES",
                                                user_insert]),
                                       )
                        # new_user = False

                else:
                    # Case of previously unknown user
                    if known_user[0][1] is None and\
                            contrib_dict['username'] is not None:
                        # new_user = True

                        update_login = "".join(["UPDATE people SET ",
                                                "rev_user_text = '",
                                                contrib_dict['username'].
                                                replace("\\", "\\\\").
                                                replace("'", "\\'").
                                                replace('"', '\\"'), "' ",
                                                "WHERE rev_user=",
                                                contrib_dict['username']])
                        con.send_query(update_login)
                    # else:
                        # new_user = False

        # TODO: Inspect why there are revisions without contributor
        # Mark revision as missing contributor
        else:
            # new_user = False
            rev_insert = "".join([rev_insert, "-1, "])
            rev_hash = "".join([rev_hash, "-1, "])

        # rev_timestamp
        rev['timestamp'] = rev['timestamp'].\
            replace('Z', '').replace('T', ' ')
        rev_insert = "".join([rev_insert, "'",
                              rev['timestamp'], "',"])
        # rev_len
        rev_insert = "".join([rev_insert,
                              rev['len_text'], ","])
        # rev_parent_id
        if rev['rev_parent_id'] is not None:
            rev_insert = "".join([rev_insert,
                                  rev['rev_parent_id'], ","])
        else:
            rev_insert = "".join([rev_insert, "NULL,"])

        # rev_redirect
        rev_insert = "".join([rev_insert,
                              rev['redirect'], ","])

        if 'minor' in rev:
            rev_insert = "".join([rev_insert, "0,"])
        else:
            rev_insert = "".join([rev_insert, "1,"])

        # Add is_fa and is_flist fields
        rev_insert = "".join([rev_insert,
                              rev['is_fa'], ",",
                              rev['is_flist'], ",",
                              rev['is_ga'], ","])

        if 'comment' in rev and\
                rev['comment'] is not None:
            rev_insert = "".join([rev_insert, '"',
                                  rev['comment'].
                                  replace("\\", "\\\\").
                                  replace("'", "\\'").
                                  replace('"', '\\"'), '")'])
        else:
            rev_insert = "".join([rev_insert, "'')"])

        # Finish insert for revision_hash
        rev_hash = "".join([rev_hash,
                            "'", text_hash.hexdigest(), "')"])

        yield (rev_insert, rev_hash)

        rev = None
        contrib_dict = None
        text = None
        text_hash = None


def process_revs_to_file(rev_iter, con=None, lang=None):
    """
    Process iterator of Revision objects extracted from dump files
    :Parameters:
        - rev_iter: iterator of Revision objects
        - lang: identifier of Wikipedia language edition from which this
        element comes from (e.g. frwiki, eswiki, dewiki...)
    """
    # Get tags to identify Featured Articles, Featured Lists and
    # Good Articles
    # lang = 'dewiki'
    # new_user = False

    if ((lang in maps.FA_RE) and (lang in maps.FLIST_RE) and
            (lang in maps.GA_RE)):
        fa_pat = maps.FA_RE[lang]
        flist_pat = maps.FLIST_RE[lang]
        ga_pat = maps.GA_RE[lang]
    else:
        raise RuntimeError('Unsupported language ' + lang)

    for rev in rev_iter:
        contrib_dict = rev['contrib_dict']

        # ### TEXT-RELATED OPERATIONS ###
        # Calculate SHA-256 hash, length of revision text and check
        # for REDIRECT
        # TODO: Inspect why there are pages without text

        # Stores SHA-256 hash of revision text
        text_hash = hashlib.sha256()
        # Default values to 0. These fields will be set below if any of the
        # target patterns is detected
        rev['redirect'] = '0'
        rev['is_fa'] = '0'
        rev['is_flist'] = '0'
        rev['is_ga'] = '0'

        if rev['text'] is not None:
            text = rev['text'].encode('utf-8')
            text_hash.update(text)
            rev['len_text'] = str(len(text))

            # Detect pattern for redirect pages
            if rev['text'][0:9].upper() == '#REDIRECT':
                rev['redirect'] = '1'

            # FA and FList detection
            # Currently 39 languages are supported regarding FA detection
            # We only enter pattern matching for revisions of pages in
            # main namespace
            if rev['ns'] == '0':
                if fa_pat is not None:
                    mfa = fa_pat.search(rev['text'])
                    # Case of standard language, one type of FA template
                    if (mfa is not None and len(mfa.groups()) == 1):
                        rev['is_fa'] = '1'
                    # Case of fawiki or cawiki, 2 types of FA templates
                    # Possible matches: (A, None) or (None, B)
                    if lang == 'fawiki' or lang == 'cawiki':
                        if (mfa is not None and len(mfa.groups()) == 2 and
                                (mfa.groups()[1] is None or
                                 mfa.groups()[0] is None)):
                                    rev['is_fa'] = '1'

                # Check if FLIST is supported in this language, detect if so
                if flist_pat is not None:
                    mflist = flist_pat.search(rev['text'])
                    if mflist is not None and len(mflist.groups()) == 1:
                        rev['is_flist'] = '1'

                # Check if GA is supported in this language, detect if so
                if ga_pat is not None:
                    mga = ga_pat.search(rev['text'])
                    if mga is not None and len(mga.groups()) == 1:
                        rev['is_ga'] = '1'
        # Compute hash for empty text here instead of in default block above
        # This way, we avoid computing the hash twice for revisions with text
        else:
            rev['len_text'] = '0'
            text_hash.update('')

        # Default value is missing user
        user = -1
        if len(contrib_dict) > 0:
            if 'ip' in contrib_dict:
                user = 0
                ip = contrib_dict['ip']
            else:
                user = int(contrib_dict['id'])
                ip = u'NULL'

        # Tuple of revision values
        rev_insert = (int(rev['id']), int(rev['page_id']), int(user),
                      rev['timestamp'].replace('Z', '').replace('T', ' '),
                      int(rev['len_text']),
                      (int(rev['rev_parent_id'])
                       if rev['rev_parent_id'] is not None else u'NULL'),
                      int(rev['redirect']),
                      (0 if 'minor' in rev else 1),
                      int(rev['is_fa']), int(rev['is_flist']),
                      int(rev['is_ga']),
                      (rev['comment'] if 'comment' in rev and
                       rev['comment'] is not None else u'NULL'),
                      ip,
                      )

        # Tuple of revision_hash values
        rev_hash = (int(rev['id']), int(rev['page_id']), int(user),
                    text_hash.hexdigest(),
                    )

        yield (rev_insert, rev_hash)

        rev = None
        contrib_dict = None
        text = None
        text_hash = None


def store_revs_file_db(rev_iter, con=None, log_file=None,
                       tmp_dir=None, file_rows=1000000,
                       etl_prefix=None):
    """
    Processor to insert revision info in DB

    This version uses an intermediate temp data file to speed up bulk data
    loading in MySQL/MariaDB, using LOAD DATA INFILE.

    Arguments:
        - rev_iter: Iterator providing tuples (rev_insert, rev_hash_insert)
        - con: Connection to local DB
        - log_file: Log file to track progress of data loading operations
        - tmp_dir: Directory to store temporary data files
        - file_rows: Number of rows to store in each tmp file
    """
    insert_rows = 0
    total_revs = 0

    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    logging.info("Starting parsing process...")

    insert_rev = """LOAD DATA LOCAL INFILE '%s' INTO TABLE revision
                    FIELDS OPTIONALLY ENCLOSED BY '"'
                    TERMINATED BY '\t' ESCAPED BY '"'
                    LINES TERMINATED BY '\n'"""

    insert_rev_hash = """LOAD DATA LOCAL INFILE '%s' INTO TABLE revision_hash
                         FIELDS OPTIONALLY ENCLOSED BY '"'
                         TERMINATED BY '\t' ESCAPED BY '"'
                         LINES TERMINATED BY '\n'"""

    path_file_rev = os.path.join(tmp_dir, etl_prefix + '_revision.csv')
    path_file_rev_hash = os.path.join(tmp_dir,
                                      etl_prefix + '_revision_hash.csv')

    # Delete previous versions of tmp files if present
    if os.path.isfile(path_file_rev):
        os.remove(path_file_rev)
    if os.path.isfile(path_file_rev_hash):
        os.remove(path_file_rev_hash)

    for rev, rev_hash in rev_iter:
        total_revs += 1

        # Initialize new temp data file
        if insert_rows == 0:
            file_rev = open(path_file_rev, 'wb')
            file_rev_hash = open(path_file_rev_hash, 'wb')
            writer = csv.writer(file_rev, dialect='excel-tab',
                                lineterminator='\n')
            writer2 = csv.writer(file_rev_hash, dialect='excel-tab',
                                 lineterminator='\n')

        # Write data to tmp file
        try:
            writer.writerow([s.encode('utf-8') if isinstance(s, unicode)
                             else s for s in rev])

            writer2.writerow([s.encode('utf-8') if isinstance(s, unicode)
                             else s for s in rev_hash])
        except(Exception), e:
            print e
            print rev

        insert_rows += 1

        # Call MySQL to load data from file and reset rows counter
        if insert_rows == file_rows:
            file_rev.close()
            file_rev_hash.close()
            con.send_query(insert_rev % path_file_rev)
            con.send_query(insert_rev_hash % path_file_rev_hash)

            logging.info("%s revisions %s." % (
                         total_revs,
                         time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                       time.localtime())))
            # Reset row counter
            insert_rows = 0
            # No need to delete tmp files, as they are empty each time we
            # open them again for writing

    # Load remaining entries in last tmp files into DB
    file_rev.close()
    file_rev_hash.close()

    con.send_query(insert_rev % path_file_rev)
    con.send_query(insert_rev_hash % path_file_rev_hash)
    # Clean tmp files
#    os.remove(path_file_rev)
#    os.remove(path_file_rev_hash)

    logging.info("%s revisions %s." % (
                 total_revs,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))
    logging.info("END: %s revisions processed %s." % (
                 total_revs,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))


def store_revs_db(rev_iter, con=None, log_file=None, size_cache=500):
    """
    Processor to insert revision info in DB
    """
    rev_insert_rows = 0
    total_revs = 0

    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    logging.info("Starting parsing process...")

    # Retrieve item form intermediate worker
    for new_rev_insert, new_rev_hash in rev_iter:
        total_revs += 1
        # ### INSERT QUERIES BUILDING ###
        # First iteration
        # Always allow at least one row in extended inserts
        if rev_insert_rows == 0:
            # Case of people
            # First values are always 0: anonymous and -1:missing
            #user_insert = "".join(["INSERT INTO people ",
                                            #"VALUES(-1, 'NA'),",
                                            #"(0, 'Anonymous')"])
            #if new_user:
                #user_insert = "".join([user_insert, ",",
                                            #new_user_insert])
            # Case of revision
            rev_insert = "".join(["INSERT INTO revision ",
                                  "VALUES", new_rev_insert])

            # Case of revision_hash
            rev_hash = "".join(["INSERT INTO revision_hash VALUES",
                                new_rev_hash])
            # Update general rows counter
            rev_insert_rows += 1

        # Extended inserts not full yet
        # Append new row to rev_insert
        elif rev_insert_rows <= size_cache:
            # Case of people
            #if new_user:
                #if len(user_insert) > 0:
                    #user_insert = "".join([user_insert, ",",
                                                #new_user_insert])
                #else:
                    #user_insert = "".join(["INSERT INTO people ",
                                            #"VALUES", new_user_insert])

            # Case of revision
            rev_insert = "".join([rev_insert, ",",
                                  new_rev_insert])

            # Case of revision_hash
            rev_hash = "".join([rev_hash, ",",
                                new_rev_hash])
            # Update general rows counter
            rev_insert_rows += 1

        # Flush extended inserts and start over new queries
        else:
            # Case of people
            #if len(user_insert) > 0:
                #send_query(con, cursor, user_insert, 5,
                                #log_file)

                #if new_user:
                    #user_insert = "".join(["INSERT INTO people ",
                                                #"VALUES",
                                                #new_user_insert])
                #else:
                    #user_insert = ""
            #else:
                #if new_user:
                    #user_insert = "".join(["INSERT INTO people ",
                                                #"VALUES",
                                                #new_user_insert])

            # Case of revision
            con.send_query(rev_insert)
            rev_insert = "".join(["INSERT INTO revision ",
                                  "VALUES", new_rev_insert])
            # Case of revision_hash
            con.send_query(rev_hash)
            rev_hash = "".join(["INSERT INTO revision_hash ",
                                "VALUES", new_rev_hash])
            # Update general rows counter
            # print "total revisions: " + unicode(total_revs)
            rev_insert_rows = 1

        if total_revs % 10000 == 0:
            logging.info("%s revisions %s." % (
                         total_revs,
                         time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                       time.localtime())))
#            print "%s revisions %s." % (
#                total_revs,
#                time.strftime("%Y-%m-%d %H:%M:%S %Z",
#                              time.localtime()))

    # Send last extended insert for revision
    con.send_query(rev_insert)

    # Send last extended insert for revision_hash
    con.send_query(rev_hash)

    logging.info("%s revisions %s." % (
                 total_revs,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))
    logging.info("END: %s revisions processed %s." % (
                 total_revs,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))

#    print "%s revisions %s." % (
#        total_revs,
#        time.strftime("%Y-%m-%d %H:%M:%S %Z",
#                      time.localtime()))
#    print "END: %s revisions processed %s." % (
#        total_revs, time.strftime("%Y-%m-%d %H:%M:%S %Z",
#                                  time.localtime()))


class RevisionText(DataItem):
    """
    Encapsulates rev_text elements for complex processing on their own
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor method for RevisionText objects. Must forward params to
        parent class DataItem (mandatory inheritance)
        """
        super(RevisionText, self).__init__(*args, **kwargs)
