# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:13:42 2014

@author: jfelipe
"""
import hashlib
import time
from wikidat.utils import maps
from data_item import DataItem


class Revision(DataItem):
    """
    Models Revision elements in Wikipedia dump files
    """

    def __init__(self, data_dict={}, lang=None):
        """
        Constructor method for Revision objects. Must forward params to
        parent class DataItem (mandatory inheritance)

        Parameters
        ----------
        data_dict: dictionary of raw data to be processed
        lang: identifier of Wikipedia language edition from which this
        element comes from (e.g. frwiki, eswiki, dewiki...)
        """
        super(Revision, self).__init__(data_dict=data_dict, lang=lang)

    def process(self, rev_iter, con=None, lang=None):
        """
        Process iterator of Revision objects extracted from dump files
        :Parameters:
            rev_iter: iterator of Revision objects
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
            contrib_dict = rev.data_dict['contrib_dict']

            # ### TEXT-RELATED OPERATIONS ###
            # Calculate SHA-256 hash, length of revision text and check
            # for REDIRECT
            # TODO: Inspect why there are pages without text

            # Stores SHA-256 hash of revision text
            text_hash = hashlib.sha256()

            if rev.data_dict['text'] is not None:
                text = rev.data_dict['text'].encode('utf-8')
                text_hash.update(text)

                rev.data_dict['len_text'] = str(len(text))

                if rev.data_dict['text'][0:9].upper() == '#REDIRECT':
                    rev.data_dict['redirect'] = '1'
                else:
                    rev.data_dict['redirect'] = '0'

                # FA and FList detection
                # TODO: Add support FA detection in more languages
                # Currently the top-10 languages are supported

                if fa_pat is not None:
                    mfa = fa_pat.search(rev.data_dict['text'])
                    # Case of standard language, one type of FA template
                    if (mfa is not None and len(mfa.groups()) == 1):
                        rev.data_dict['is_fa'] = '1'
                    # Case of fawiki or cawiki, 2 types of FA templates
                    # Possible matches: (A, None) or (None, B)
                    elif (mfa is not None and len(mfa.groups()) == 2 and
                            (mfa.groups()[1] is None or
                             mfa.groups()[0] is None)):
                        rev.data_dict['is_fa'] = '1'
                    else:
                        rev.data_dict['is_fa'] = '0'
                else:
                    rev.data_dict['is_fa'] = '0'

                # Check if FLIST is supported in this language, detect if so
                if flist_pat is not None:
                    mflist = flist_pat.search(rev.data_dict['text'])
                    if mflist is not None and len(mflist.groups()) == 1:
                        rev.data_dict['is_flist'] = '1'
                    else:
                        rev.data_dict['is_flist'] = '0'
                else:
                    rev.data_dict['is_flist'] = '0'

                # Check if GA is supported in this language, detect if so
                if ga_pat is not None:
                    mga = ga_pat.search(rev.data_dict['text'])
                    if mga is not None and len(mga.groups()) == 1:
                        rev.data_dict['is_ga'] = '1'
                    else:
                        rev.data_dict['is_ga'] = '0'
                else:
                    rev.data_dict['is_ga'] = '0'

            else:
                rev.data_dict['len_text'] = '0'
                rev.data_dict['redirect'] = '0'
                rev.data_dict['is_fa'] = '0'
                rev.data_dict['is_flist'] = '0'
                rev.data_dict['is_ga'] = '0'
                text_hash.update('')

            # SQL query building
            # Build extended inserts for people, revision and revision_hash
            rev_insert = "".join(["(", rev.data_dict['id'], ",",
                                  rev.data_dict['page_id'], ","])

            rev_hash = "".join(["(", rev.data_dict['id'], ",",
                                rev.data_dict['page_id'], ","])

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
            rev.data_dict['timestamp'] = rev.data_dict['timestamp'].\
                replace('Z', '').replace('T', ' ')
            rev_insert = "".join([rev_insert, "'",
                                  rev.data_dict['timestamp'], "',"])
            # rev_len
            rev_insert = "".join([rev_insert,
                                  rev.data_dict['len_text'], ","])
            # rev_parent_id
            if rev.data_dict['rev_parent_id'] is not None:
                rev_insert = "".join([rev_insert,
                                      rev.data_dict['rev_parent_id'], ","])
            else:
                rev_insert = "".join([rev_insert, "NULL,"])

            # rev_redirect
            rev_insert = "".join([rev_insert,
                                  rev.data_dict['redirect'], ","])

            if 'minor' in rev.data_dict:
                rev_insert = "".join([rev_insert, "0,"])
            else:
                rev_insert = "".join([rev_insert, "1,"])

            # Add is_fa and is_flist fields
            rev_insert = "".join([rev_insert,
                                  rev.data_dict['is_fa'], ",",
                                  rev.data_dict['is_flist'], ",",
                                  rev.data_dict['is_ga'], ","])

            if 'comment' in rev.data_dict and\
                    rev.data_dict['comment'] is not None:
                rev_insert = "".join([rev_insert, '"',
                                      rev.data_dict['comment'].\
                                      replace("\\", "\\\\").\
                                      replace("'", "\\'").\
                                      replace('"', '\\"'), '")'])
            else:
                rev_insert = "".join([rev_insert, "'')"])

            # Finish insert for revision_hash
            rev_hash = "".join([rev_hash,
                                "'", text_hash.hexdigest(), "')"])

            yield (rev_insert, rev_hash)

            rev.data_dict = None
            contrib_dict = None
            text = None
            text_hash = None

    def store_db(self, rev_iter, con=None, size_cache=300):
        """
        Processor to insert revision info in DB
        """
        rev_insert_rows = 0
        total_revs = 0

        # Retrieve item form intermediate worker
        for new_rev_insert, new_rev_hash in rev_iter:
            total_revs += 1
            #print "Numero total de revisiones que llegan: " + unicode(total_revs)
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
                #print "total revisions: " + unicode(total_revs)
                rev_insert_rows = 1

            if total_revs % 1000 == 0:
                print "%s revisions %s." % (
                    total_revs,
                    time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                  time.localtime()))

        # Send last extended insert for revision
        con.send_query(rev_insert)

        # Send last extended insert for revision_hash
        con.send_query(rev_hash)

        print "%s revisions %s." % (
            total_revs,
            time.strftime("%Y-%m-%d %H:%M:%S %Z",
                          time.localtime()))


class RevisionText(DataItem):
    """
    Encapsulates rev_text elements for complex processing on their own
    """

    def __init__(self, data_dict, lang):
        """
        Constructor method for RevisionText objects. Must forward params to
        parent class DataItem (mandatory inheritance)

        Parameters
        ----------
        data_dict: dictionary of raw data to be processed
        lang: identifier of Wikipedia language edition from which this
        element comes from (e.g. frwiki, eswiki, dewiki...)
        """
        super(RevisionText, self).__init__(data_dict=data_dict, lang=lang)
