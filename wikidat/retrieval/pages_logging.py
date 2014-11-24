# -*- coding: utf-8 -*-
######
# Felipe Ortega and Aaron Halfaker
######

from lxml import etree
import MySQLdb, sys, codecs, os, subprocess, time
from wikidat.utils import maps
import warnings


class Parser(object):
    """
    Parses content of Wikimedia dump files (pages-logging.xml)
    """
    def __init__(self, db, cursor, log_file):
        # DB connection
        self.db = db
        # DB cursor
        self.cursor = cursor
        # Name of the log file
        self.log_file = log_file

        # Namespaces dict
        self.ns_dict = {}
        # Dict for logitem element
        self.log_dict = {}
        # Dict for contributor element
        self.contrib_dict = {}
        # Counter for parsed logs
        self.log_num = 0
        # Counter for preparing extended insert
        self.log_insert_rows = 0

    def parse(self, path):
        self.in_stream = self._open_dump(self._check_dump(path))

        for event, elem in etree.iterparse(self.in_stream):

            # Drop tag namespace
            tag = elem.tag.split('}')[1]

            if tag == 'namespaces':
                self.ns_dict = {c.text: int(c.attrib.get('key')) for c in elem}
                self.ns_dict[''] = 0

            if tag == 'contributor':
                # Build dict {tag:text} for contributor info
                self.contrib_dict = {x.tag.split('}')[1]: x.text for x in elem}

            if tag == 'logitem':
                self.log_dict = {x.tag.split('}')[1]: x.text for x in elem}

                # Get namespace for this log item from page title prefix
                if 'logtitle' in self.log_dict:
                    ns_prefix = self.log_dict['logtitle'].split(':')
                    if (len(ns_prefix) == 2 and ns_prefix[0] in self.ns_dict):
                        self.log_dict['namespace'] = str(self.ns_dict[ns_prefix[0]])
                    else:
                        self.log_dict['namespace'] = '0'
                else:
                    self.log_dict['logtitle'] = ''
                    self.log_dict['namespace'] = '-1000'  # Fake namespace

                # Clean timestamp string
                self.log_dict['timestamp'] = self.log_dict['timestamp'].\
                    replace('Z', '').replace('T', ' ')

                # Content of log_old_flag and log_new_flag
                # for languages with flagged revisions
                if (self.log_dict['type'] == 'review' and
                    (self.log_dict['action'] == 'approve' or
                     self.log_dict['action'] == 'approve-a' or
                     self.log_dict['action'] == 'unapprove' or
                     self.log_dict['action'] == 'approve-ia' or
                     self.log_dict['action'] == 'approve-i')):

                    # Check presence of params
                    # TODO: Investigate review items without params
                    if 'params' in self.log_dict:
                        flags = self.log_dict['params'].split('\n')
                        # Standard case before March 2010
                        # 2 params: new stable revision and old stable revision
                        if (len(flags) == 2):
                            self.log_dict['new_flag'] = flags[0]
                            self.log_dict['old_flag'] = flags[1]
                        # Case after March 2010
                        # Timestamp of last stable version was introduced
                        # as a third param. We only get the first two:
                        # rev_id of new stable revision and rev_id of
                        # previous stable revision
                        if (len(flags) == 3):
                            self.log_dict['new_flag'] = flags[0]
                            self.log_dict['old_flag'] = flags[1]
                        # Standard case before March 2010
                        # Only new stable version if no previous stable version
                        # is available
                        elif (len(flags) == 1):
                            self.log_dict['new_flag'] = flags[0]
                            self.log_dict['old_flag'] = '0'

                # Build new row for loginsert
                # TODO: Investigate why we find logitems without type or action

                new_log_insert = "".join(["(", self.log_dict['id'], ","])

                if (self.log_dict['type'] is not None):
                    new_log_insert = "".join([new_log_insert, '"',
                                              self.log_dict['type'], '",'])
                else:
                    new_log_insert = "".join([new_log_insert, '"",'])

                if (self.log_dict['action'] is not None):
                    new_log_insert = "".join([new_log_insert, '"',
                                              self.log_dict['action'], '",'])
                else:
                    new_log_insert = "".join([new_log_insert, '"",'])

                new_log_insert = "".join([new_log_insert, '"',
                                          self.log_dict['timestamp'], '",'])

                if 'id' in self.contrib_dict:
                    new_log_insert = "".join([new_log_insert,
                                              self.contrib_dict['id'], ","])
                else:
                    new_log_insert = "".join([new_log_insert, '-1,'])

                if 'username' in self.contrib_dict:
                    new_log_insert = "".join([new_log_insert, '"',
                                              (self.contrib_dict['username'].
                                               replace("\\", "\\\\").
                                               replace("'", "\\'").
                                               replace('"', '\\"')), '",'])
                else:
                    new_log_insert = "".join([new_log_insert, '"",'])

                if 'namespace' in self.log_dict:
                    new_log_insert = "".join([new_log_insert,
                                              self.log_dict['namespace'],
                                              ","])
                else:
                    # FAKE namespace if absent
                    new_log_insert = "".join([new_log_insert, '-1000,'])

                new_log_insert = "".join([new_log_insert, '"',
                                          (self.log_dict['logtitle'].
                                           replace("\\", "\\\\").
                                           replace("'", "\\'").
                                           replace('"', '\\"')), '",'])

                if ('comment' in self.log_dict and
                        self.log_dict['comment'] is not None):
                    new_log_insert = "".join([new_log_insert, '"',
                                              (self.log_dict['comment'].
                                               replace("\\", "\\\\").
                                               replace("'", "\\'").
                                               replace('"', '\\"')), '",'])
                else:
                    new_log_insert = "".join([new_log_insert, '"",'])

                if ('params' in self.log_dict and
                        self.log_dict['params'] is not None):
                    new_log_insert = "".join([new_log_insert, '"',
                                              (self.log_dict['params'].
                                               replace("\\", "\\\\").
                                               replace("'", "\\'").
                                               replace('"', '\\"')), '",'])
                else:
                    new_log_insert = "".join([new_log_insert, '"",'])

                if 'new_flag' in self.log_dict:
                    new_log_insert = "".join([new_log_insert,
                                              self.log_dict['new_flag'], ','])
                else:
                    new_log_insert = "".join([new_log_insert, '0,'])

                if 'old_flag' in self.log_dict:
                    new_log_insert = "".join([new_log_insert,
                                              self.log_dict['old_flag'], ')'])
                else:
                    new_log_insert = "".join([new_log_insert, '0)'])

                if self.log_insert_rows == 0:
                    #Always allow at least one row in extended inserts
                    self.log_insert = "".join(["INSERT INTO logging ",
                                               "VALUES", new_log_insert])
                    self.log_insert_rows += 1

                elif self.log_insert_rows <= 1000:
                    #Append new row to self.loginsert
                    self.log_insert = "".join([self.log_insert, ",",
                                               new_log_insert])
                    self.log_insert_rows += 1

                # Sending extended insert to DB
                else:
                    self.send_query(self.db, self.cursor, self.log_insert,
                                    5, self.log_file)

                    self.log_insert = "".join(["INSERT INTO logging ",
                                               "VALUES", new_log_insert])
                    self.log_insert_rows += 1

                self.log_num += 1

                #Clear memory
                self.contrib_dict = None
                self.log_dict = None
                # Delete all subitems in logitem to clear memory
                elem.clear()
                # Also eliminate now-empty references from the root node to
                # <logitem>. Credit to Liza Daly
                # http://www.ibm.com/developerworks/xml/library/
                # x-hiperfparse/#listing1
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

                if self.log_num % 1000 == 0:
                    print "%s log items " % (self.log_num) +\
                        time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime())

        # Insert last data packet in DB
        self.send_query(self.db, self.cursor, self.log_insert, 5,
                        self.log_file)

        return self.log_num

    def send_query(self, db, cursor, query, ntimes, log_file):
        """
        Send query to DB. Attempt 'ntimes' consecutive times before giving up
        cursor: cursor to open DB connection
        query: query to be sent to DB
        ntimes: number of sending attempts
        log_file: name of file to log errors
        """
        # TODO: Handle errors properly with logger library
        #chances = 0
        #while chances < ntimes:
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            warnings.simplefilter('ignore', MySQLdb.Warning)
            try:
                cursor.execute(query)
                # Commit changes
                db.commit()
            except (Exception), e:
                # Rollback changes and log error
                db.rollback()
                self.f = codecs.open(log_file,'a',
                                            'utf-8')
                self.f.write(str(e)+"\n")
                self.f.write("".join([query[0:79],
                            "\n******************************"]))
                self.f.close()
                #chances += 1
            else:
                pass
                #break
                
    def _check_dump(self, path):
        """
        Verifies that a file exists at a given path and that the file has a 
        known extension type.
        
        :Parameters:
            path : `str`
                the path to a dump file
            
        """
        path = os.path.expanduser(path)
        if not os.path.isfile(path):
            raise IOError("Can't find file %s" % path)
        
        match = maps.EXT_RE.search(path)
        if match == None:
            raise IOError("No extension found for %s." % path)
        elif match.groups()[0] not in maps.EXTENSIONS:
            raise IOError("File type %r is not supported." % path)
        else:
            return path
            
    def _open_dump(self, path):
        """
        Turns a path to a dump file into a file-like object of (decompressed)
        XML data.
        
        :Parameters:
            path : `str`
                the path to the dump file to read
        """
        match = maps.EXT_RE.search(path)
        ext = match.groups()[0]
        p = subprocess.Popen(
            "%s %s" % (maps.EXTENSIONS[ext], path), 
            shell=True, 
            stdout=subprocess.PIPE,
            stderr=open(os.devnull, "w")
        )
        #sys.stderr.write(p.stdout.read(1000))
        #return False
        return p.stdout
        
if __name__ == '__main__':
    db_name = sys.argv[1]
    db_user = sys.argv[2]
    db_pass = sys.argv[3]
    f = sys.argv[4]
    log_file = sys.argv[5]
    db = MySQLdb.Connect (host = 'localhost', port = 3306, user = db_user, 
                            passwd=db_pass ,db = db_name,
                            charset="utf8", use_unicode=True)
    #conn.autocommit(True)
    cursor = db.cursor()
    
    # Arguments: DB connection, wiki lang and name of log file
    # Currently supported: 'enwiki', 'dewiki'
    parser = Parser(db, cursor, log_file)
    
    print "Parsing file " +  f
    start = time.clock()
    logs = parser.parse(f)
    end = time.clock()
    print "Successfully parsed %s log items " % logs +\
          "in %.6f mins" % ((end - start)/60.)
      
    db.close()
    