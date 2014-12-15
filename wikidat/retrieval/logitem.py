# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:14:21 2014

@author: jfelipe
"""
from data_item import DataItem
import dateutil
import datetime
import re
import os
import csv
import time
import logging


class LogItem(DataItem):
    """
    Models LogItem elements extracted from the 'logging' DB table in Wikipedia
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor method for Page objects. Must forward params to
        parent class DataItem (mandatory inheritance)

        Parameters
        ----------
        data_dict: dictionary of raw data to be processed
        lang: identifier of Wikipedia language edition from which this
        element comes from (e.g. frwiki, eswiki, dewiki...)
        """
        super(LogItem, self).__init__(*args, **kwargs)


def process_logitem(log_iter):
    """
    Processor for LogItem objects extracted from the 'logging' DB table in
    Wikipedia
    """
    ip_pat = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    time_unit_ft = re.compile(r"""sec|min|h|d|week|fortnight|month|year|
                                  indefinite|infinite""")

    # Case 'month', rounded to 30 days per month
    # Case 'year', multiply by 365.25 days per year
    # Case 'fortnight' is equivalent to 2 weeks
    # Case 'infinite' will default to timedelta.max
    time_units = {'sec': 'seconds',
                  'min': 'minutes',
                  'h': 'hours',
                  'd': 'days',
                  'week': 'weeks',
                  'fortnight': 'weeks',
                  'month': 'days',
                  'year': 'days'
                  }
    time_fac = {'sec': 1,
                'min': 1,
                'h': 1,
                'd': 1,
                'week': 1,
                'fortnight': 2,
                'month': 30,
                'year': 365.25
                }

    for logitem in log_iter:
        # Clean timestamp string
        logitem['timestamp'] = (logitem['timestamp'].
                                replace('Z', '').replace('T', ' '))
        # INFO FLAGGED REVISIONS
        # Content of log_old_flag and log_new_flag
        # for languages with flagged revisions
        if (logitem['type'] == 'review' and
            (logitem['action'] == 'approve' or
             logitem['action'] == 'approve-a' or
             logitem['action'] == 'unapprove' or
             logitem['action'] == 'approve-ia' or
             logitem['action'] == 'approve-i')):

            # Check presence of params
            # TODO: Investigate review items without params
            if 'params' in logitem:
                flags = logitem['params'].split('\n')

                # Standard case before March 2010
                # Only new stable version if no previous stable version
                # is available
                if (len(flags) == 1):
                    logitem['new_flag'] = flags[0]
                    logitem['old_flag'] = '0'
                # Standard case before March 2010
                # 2 params: new stable revision and old stable revision
                # ----
                # Case after March 2010
                # Timestamp of new stable version was introduced
                # as a third param. This is redundant with info from
                # table revision. Thus, we only get the first two params:
                # rev_id of new stable revision and rev_id of
                # previous stable revision
                elif (len(flags) == 2 or len(flags) == 3):
                    logitem['new_flag'] = flags[0]
                    logitem['old_flag'] = flags[1]

        # INFO BLOCKED USERS
        if (logitem['type'] == 'block' and
            (logitem['action'] == 'block' or
             logitem['action'] == 'unblock' or
             logitem['action'] == 'reblock')):
            # Identify target user from log_title field
            title = logitem['logtitle'].split(':')
            if len(title) == 2:
                target = title[1]
                if re.search(ip_pat, target):
                    # Case of IP addresses
                    logitem['target_ip'] = target
                else:
                    # Case of logged user
                    logitem['target'] = target

            # Calculate duration of block action from log_params field
            # This field might be blank
            # Case 1: Figure + range (e.g. '1 week', '2 days', '6 months')
            # Case 2: Timestamp with expiration date for block
            # e.g. Wed, 22 Jan 2014 10:14:10 GMT
            if 'params' in logitem and logitem['params']:
                # Identify formation of duration param
                par_dur = logitem['params'].split('\n')
                if re.search('GMT', par_dur[0]):
                    exp = dateutil.parser.parse(par_dur.rsplit(' ', 1)[0])
                    ts = dateutil.parser.parse(logitem['timestamp'])
                    logitem['duration'] = (exp-ts).total_seconds()
                else:
                    exp_par = re.split(r'(\D+)', par_dur[0])
                    duration = exp_par[0]
                    units = exp_par[1]
                    # Try automated detection of block duration, expressed
                    # in "natural language" units
                    if (units == 'infinite' or
                            units == 'indefininte'):
                        logitem['duration'] = (datetime.timedelta.max.
                                               total_seconds())
                    elif duration:
                        time_unit = re.search(time_unit_ft,
                                              units).group()
                        delta_args = {time_units[time_unit]:
                                      int(duration) * time_fac[time_unit]}
                        logitem['duration'] = datetime.timedelta(**delta_args)
                    else:
                        # TODO: Inspect this case later on
                        # Address case of empty duration
                        logitem['duration'] = 0

        # INFO DELETIONS
        # TODO:

        # INFO PROTECTIONS
        # TODO:

        # INFO USER REGISTRATIONS
        if (logitem['type'] == 'newusers' and
            (logitem['action'] == 'newusers' or
             logitem['action'] == 'create' or
             logitem['action'] == 'create2' or
             logitem['action'] == 'autocreate' or
             logitem['action'] == 'byemail')):
                logitem['newuser'] = True  # Flag new user for later

        # INFO RIGHTS GRANTING
        if (logitem['type'] == 'rights' and logitem['action'] == 'rights'):
            logitem['rights'] = True  # Flag new rights granting for later
            pars = logitem['params'].split('\n')
            # Case of old format for parameters, with previous status in first
            # line, then new list of privileges in new line
            if len(pars) > 1:
                logitem['right_old'] = pars[0]
                logitem['right_new'] = pars[1]
            else:
                # Case of new single-line format oldgroups --> new groups
                if (re.search('"4::oldgroups"', pars[0])):
                    priv_list = (pars[0].partition('"4::oldgroups"')[2].
                                 partition('"5::newgroups"'))
                    priv_old = re.findall(r'\"(.+?)\"', priv_list[0])
                    priv_new = re.findall(r'\"(.+?)\"', priv_list[2])
                    logitem['right_old'] = unicode(priv_old)
                    logitem['right_new'] = unicode(priv_new)

                # Case of primitive free format
                else:
                    logitem['right_old'] = None
                    logitem['right_new'] = pars[0]

        yield(logitem)
        logitem = None


def logitem_to_file(log_iter):
    """
    Processor for LogItem objects extracted from the 'logging' DB table in
    Wikipedia, using intermediate tmp files for bulk data loading
    """
    for logitem in process_logitem(log_iter):
        contrib_dict = logitem['contrib_dict']

        logitem_insert = (int(logitem['id']), logitem['type'],
                          logitem['action'], logitem['timestamp'],
                          (int(contrib_dict['id']) if 'id' in contrib_dict
                           else -1),
                          (contrib_dict['username'] if 'username' in
                           contrib_dict else ""),
                          int(logitem['namespace']),
                          logitem['logtitle'],
                          (logitem['comment'] if 'comment' in logitem and
                           logitem['comment'] else u""),
                          (logitem['params'] if 'params' in logitem and
                           logitem['params'] else u""),
                          (int(logitem['new_flag']) if 'new_flag' in logitem
                           else 0),
                          (int(logitem['old_flag']) if 'old_flag' in logitem
                           else 0),
                          )

        # Case of new user to process
        if 'newuser' in logitem and logitem['newuser']:
            # TODO: Build data packet to be inserted in table newusers
            pass

        # Case of new rights granting action
        if 'rights' in logitem and logitem['rights']:
            # TODO: Build data packet to be inserted in table new rights
            pass

        yield(logitem_insert)


def logitem_file_to_db(log_iter, con=None, log_file=None,
                       tmp_dir=None, file_rows=1000000, etl_prefix=None):
    """
    Store processed logitems in DB from intermediate tmp data files
    """
    insert_rows = 0
    total_logs = 0

    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    logging.info("Starting revisions processing...")

    insert_logitem = """LOAD DATA INFILE '%s' INTO TABLE logging
                        FIELDS OPTIONALLY ENCLOSED BY '"'
                        TERMINATED BY '\t' ESCAPED BY '"'
                        LINES TERMINATED BY '\n'"""

    # TODO: insert newuser
    # TODO: insert block action

    path_file_logitem = os.path.join(tmp_dir, etl_prefix + '_logging.csv')
    # Delete previous versions of tmp files if present
    if os.path.isfile(path_file_logitem):
        os.remove(path_file_logitem)

    for logitem in log_iter:
        total_logs += 1

        # Initialize new temp data file
        if insert_rows == 0:
            file_logitem = open(path_file_logitem, 'wb')
            writer = csv.writer(file_logitem, dialect='excel-tab',
                                lineterminator='\n')
        # Write data to tmp file
        try:
            writer.writerow([s.encode('utf-8') if isinstance(s, unicode)
                             else s for s in logitem])
        except(Exception), e:
            print e
            print logitem

        insert_rows += 1

        # Call MySQL to load data from file and reset rows counter
        if insert_rows == file_rows:
            file_logitem.close()
            con.send_query(insert_logitem % path_file_logitem)

            logging.info("%s revisions %s." % (
                         total_logs,
                         time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                       time.localtime())))
            # Reset row counter
            insert_rows = 0
            # No need to delete tmp files, as they are empty each time we
            # open them again for writing

    # Load remaining entries in last tmp files into DB
    file_logitem.close()

    con.send_query(insert_logitem % path_file_logitem)
    # TODO: Clean tmp files, uncomment the following lines
#    os.remove(path_file_logitem)

    # Log end of tasks and exit
    logging.info("COMPLETED: %s logging records processed %s." % (
                 total_logs,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))
