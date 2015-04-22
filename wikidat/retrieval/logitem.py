# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:14:21 2014

@author: jfelipe
"""
from .data_item import DataItem
import dateutil.parser
import ipaddress
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
    lead_zero_pat = re.compile(r"(0\d\d)")
    triple_zero_pat = re.compile(r"000")
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

            logitem['flagged'] = True
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

            # TODO: Evaluate the possibility of extracting flagged-revs
            # related data to an independent DB table

        # INFO BLOCKED USERS
        if (logitem['type'] == 'block' and
            (logitem['action'] == 'block' or
             logitem['action'] == 'unblock' or
             logitem['action'] == 'reblock')):

            logitem['block'] = {}  # Flag block action for later
            # Identify target user from log_title field
            title = logitem['logtitle'].split(':')
            if len(title) == 2:
                target = title[1]
                if re.search(ip_pat, target):
                    # Case of IP addresses
                    # Fix malformed records: del leading 0s if present
                    target = re.sub(triple_zero_pat, '0', target)
                    target = re.sub(lead_zero_pat,
                                    lambda x: x.group().lstrip('0'), target)
                    try:
                        logitem['block']['target_ip'] = int(ipaddress.ip_address(target))
                    except ValueError:
                        print("Invalid IP address to block: ", target)
                        logitem['block']['target_ip'] = 0
                else:
                    # Case of logged user
                    logitem['block']['target'] = target

            # Calculate duration of block action from log_params field
            # This field might be blank
            # Case 1: Figure + range (e.g. '1 week', '2 days', '6 months')
            # Case 2: Timestamp with expiration date for block
            # e.g. Wed, 22 Jan 2014 10:14:10 GMT
            if 'params' in logitem and logitem['params']:
                # Identify formation of duration param
                par_dur = logitem['params'].split('\n')[0]
                par_dur = par_dur.replace('Z', '').replace('T', ' ')
                try:
                    # exp = dateutil.parser.parse(par_dur.rsplit(' ', 1)[0])
                    exp = dateutil.parser.parse(par_dur)
                    if re.search('GMT', par_dur):
                        ts = dateutil.parser.parse(logitem['timestamp']+'GMT')
                    else:
                        ts = dateutil.parser.parse(logitem['timestamp'])
                    logitem['block']['duration'] = (exp-ts).total_seconds()
                # Try automated detection of block duration, expressed
                # in "natural language" units
                except Exception:
                    exp_par = re.split(r'(\D+)', par_dur)
                    try:
                        duration = exp_par[0]
                        units = exp_par[1].lower()
                    except IndexError:
                        print("No valid pair duration/units found!")
                        print("params:", logitem['params'])
                        logitem['block']['duration'] = 0.0

                    if (units == 'infinite' or units == 'indefininte'):
                        logitem['block']['duration'] = (datetime.timedelta.max.total_seconds())
                    elif duration:
                        try:
                            time_unit = re.search(time_unit_ft,
                                                  units).group()
                            delta_args = {time_units[time_unit]:
                                          int(duration) * time_fac[time_unit]}
                            logitem['block']['duration'] = datetime.timedelta(**delta_args).total_seconds()
                        except AttributeError:
                            print("params:", logitem['params'])
                            logitem['block']['duration'] = 0.0
                        except OverflowError:
                            logitem['block']['duration'] = (datetime.timedelta.max.total_seconds())
                    else:
                        # TODO: Inspect this case later on
                        # Address case of empty duration
                        logitem['block']['duration'] = 0.0
            else:
                # TODO: Inspect this case later on
                # Address case of empty duration
                logitem['block']['duration'] = 0.0

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

                # TODO: Evaluate if we need additional info about newusers
                logitem['newuser'] = {}  # Flag new user for later

        # INFO RIGHTS GRANTING
        if (logitem['type'] == 'rights' and logitem['action'] == 'rights'):

            logitem['rights'] = {}  # Flag new rights granting for later
            try:
                logitem['rights']['username'] = logitem['logtitle'].split(':')[1]
            except IndexError:
                print("No user name info in change of user level.")
                if 'params' in logitem:
                    print("params:", logitem['params'])
                logitem['rights']['username'] = ""

            if 'params' in logitem and logitem['params']:
                pars = logitem['params'].split('\n')
                # Case of old format for parameters, with previous status
                # in first line, then new list of privileges in new line
                if len(pars) > 1:
                    logitem['rights']['right_old'] = pars[0]
                    logitem['rights']['right_new'] = pars[1]
                else:
                    # Case of new single-line format oldgroups --> new groups
                    if (re.search('"4::oldgroups"', pars[0])):
                        priv_list = (pars[0].partition('"4::oldgroups"')[2].
                                     partition('"5::newgroups"'))
                        priv_old = re.findall(r'\"(.+?)\"', priv_list[0])
                        priv_new = re.findall(r'\"(.+?)\"', priv_list[2])
                        logitem['rights']['right_old'] = str(priv_old)
                        logitem['rights']['right_new'] = str(priv_new)

                    # Case of primitive free format
                    else:
                        logitem['rights']['right_old'] = ""
                        logitem['rights']['right_new'] = pars[0]
            elif logitem['comment']:
                logitem['rights']['right_old'] = ""
                logitem['rights']['right_new'] = logitem['comment']
            else:
                # No information recorded about new user levels
                logitem['rights']['right_old'] = ""
                logitem['rights']['right_new'] = ""
        yield(logitem)
        del logitem


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
                          logitem['namespace'],
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

        # Case of BLOCKED user
        if 'block' in logitem:
            block_insert = (int(logitem['id']), logitem['action'],
                            (int(contrib_dict['id']) if 'id' in contrib_dict
                             else -1), logitem['timestamp'],
                            (logitem['block']['target']
                             if 'target' in logitem['block'] else ""),
                            (logitem['block']['target_ip']
                             if 'target_ip' in logitem['block'] else ""),
                            logitem['block']['duration'],
                            )
        else:
            block_insert = None

        # Case of NEWUSER to process
        if 'newuser' in logitem:
            newuser_insert = (int(logitem['id']),
                              (int(contrib_dict['id']) if 'id' in contrib_dict
                               else -1),
                              (contrib_dict['username'] if 'username' in
                              contrib_dict else ""), logitem['timestamp'],
                              logitem['action'],
                              )
        else:
            newuser_insert = None

        # Case of new RIGHTS granting action
        if 'rights' in logitem:
            # TODO: Build data packet to be inserted in table new rights
            rights_insert = (int(logitem['id']), int(contrib_dict['id']),
                             logitem['rights']['username'],
                             logitem['timestamp'],
                             logitem['rights']['right_old'],
                             logitem['rights']['right_new'],
                             )
        else:
            rights_insert = None

        dict_insert = {'logitem': logitem_insert,
                       # 'flagged_revs': flagged_insert,
                       'block': block_insert,
                       'newuser': newuser_insert,
                       'rights': rights_insert
                       }
        yield(dict_insert)
        del dict_insert


def logitem_file_to_db(log_iter, con=None, log_file=None,
                       tmp_dir=None, file_rows=1000000, etl_prefix=None):
    """
    Store processed logitems in DB from intermediate tmp data files
    """
    insert_rows = 0
    total_logs = 0

    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    print("Starting logitem data loading at %s." % (
        time.strftime("%Y-%m-%d %H:%M:%S %Z",
                      time.localtime())))
    logging.info("Starting logitem data loading at %s." % (
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))

    insert_logitem = """LOAD DATA INFILE '%s' INTO TABLE logging
                        FIELDS OPTIONALLY ENCLOSED BY '"'
                        TERMINATED BY '\t' ESCAPED BY '"'
                        LINES TERMINATED BY '\n'"""

    insert_block = """LOAD DATA INFILE '%s' INTO TABLE block
                      FIELDS OPTIONALLY ENCLOSED BY '"'
                      TERMINATED BY '\t' ESCAPED BY '"'
                      LINES TERMINATED BY '\n'"""

    insert_newuser = """LOAD DATA INFILE '%s' INTO TABLE user_new
                        FIELDS OPTIONALLY ENCLOSED BY '"'
                        TERMINATED BY '\t' ESCAPED BY '"'
                        LINES TERMINATED BY '\n'"""

    insert_rights = """LOAD DATA INFILE '%s' INTO TABLE user_level
                       FIELDS OPTIONALLY ENCLOSED BY '"'
                       TERMINATED BY '\t' ESCAPED BY '"'
                       LINES TERMINATED BY '\n'"""

    path_file_logitem = os.path.join(tmp_dir, etl_prefix + '_logging.csv')
    path_file_block = os.path.join(tmp_dir, etl_prefix + '_block.csv')
    path_file_newuser = os.path.join(tmp_dir, etl_prefix + '_user_new.csv')
    path_file_rights = os.path.join(tmp_dir, etl_prefix + '_user_level.csv')
    # Delete previous versions of tmp files if present
    if os.path.isfile(path_file_logitem):
        os.remove(path_file_logitem)
    if os.path.isfile(path_file_block):
        os.remove(path_file_block)
    if os.path.isfile(path_file_newuser):
        os.remove(path_file_newuser)
    if os.path.isfile(path_file_rights):
        os.remove(path_file_rights)

    for logdict in log_iter:
        total_logs += 1

        logitem = logdict['logitem']
        block = logdict['block']
        newuser = logdict['newuser']
        rights = logdict['rights']

        # Initialize new temp data file
        if insert_rows == 0:
            # In this case, buffer size to trigger data load only tracks
            # num. of logitems already processed. We take the same mark to
            # load data for all associated tables
            file_logitem = open(path_file_logitem, 'w')
            writer = csv.writer(file_logitem, dialect='excel-tab',
                                lineterminator='\n')
            file_block = open(path_file_block, 'w')
            writer_block = csv.writer(file_block, dialect='excel-tab',
                                      lineterminator='\n')
            file_newuser = open(path_file_newuser, 'w')
            writer_new = csv.writer(file_newuser, dialect='excel-tab',
                                    lineterminator='\n')
            file_rights = open(path_file_rights, 'w')
            writer_rights = csv.writer(file_rights, dialect='excel-tab',
                                       lineterminator='\n')
        # Write data to tmp file
        try:
            writer.writerow([s if isinstance(s, str)
                             else str(s) for s in logitem])
            if block:
                writer_block.writerow([s if isinstance(s, str)
                                       else str(s) for s in block])
            if newuser:
                writer_new.writerow([s if isinstance(s, str)
                                     else str(s) for s in newuser])
            if rights:
                writer_rights.writerow([s if isinstance(s, str)
                                        else str(s) for s in rights])
        except Exception as e:
            print("Error writing logitem temp files...")
            print(e)
            print(logitem)

        insert_rows += 1

        # Call MySQL to load data from file and reset rows counter
        if insert_rows == file_rows:
            file_logitem.close()
            con.send_query(insert_logitem % path_file_logitem)

            file_block.close()
            con.send_query(insert_block % path_file_block)
            file_newuser.close()
            con.send_query(insert_newuser % path_file_newuser)
            file_rights.close()
            con.send_query(insert_rights % path_file_rights)

            logging.info("%s logitems %s." % (
                         total_logs,
                         time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                       time.localtime())))
            # Reset row counter
            insert_rows = 0
            # No need to delete tmp files, as they are empty each time we
            # open them again for writing

    # Load remaining entries in last tmp files into DB
    file_logitem.close()
    file_block.close()
    file_newuser.close()
    file_rights.close()

    con.send_query(insert_logitem % path_file_logitem)
    con.send_query(insert_block % path_file_block)
    con.send_query(insert_newuser % path_file_newuser)
    con.send_query(insert_rights % path_file_rights)
    # TODO: Clean tmp files, uncomment the following lines
    # os.remove(path_file_logitem)

    # Log end of tasks and exit
    logging.info("COMPLETED: %s logging records processed %s." % (
                 total_logs,
                 time.strftime("%Y-%m-%d %H:%M:%S %Z",
                               time.localtime())))
