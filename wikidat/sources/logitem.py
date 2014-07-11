# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:14:21 2014

@author: jfelipe
"""
from data_item import DataItem
import dateutil
import datetime
import re


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
    time_unit_ft = re.compile(r"sec|min|h|d|week|fortnight|year|indefinite|infinite")

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
                'hour': 1,
                'day': 1,
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
            if logitem['params']:
                # Identify formation of duration param
                par_dur = logitem['params'].split('\n')
                if re.search('GMT', par_dur[0]):
                    exp = dateutil.parser.parse(par_dur.rsplit(' ', 1)[0])
                    ts = dateutil.parser.parse(logitem['timestamp'])
                    logitem['duration'] = (exp-ts).total_seconds()
                else:
                    exp_par = re.split(r'(\D+)', par_dur[0])
                    # Try automated detection of block duration, expressed
                    # in "natural language" units
                    if (exp_par[0] == 'infinite' or
                            exp_par[0] == 'indefininte'):
                        logitem['duration'] = (datetime.timedelta.max.
                                               total_seconds())
                    else:
                        time_unit = re.search(time_unit_ft,
                                              exp_par[1]).group()
                        delta_args = {time_units[time_unit]:
                                      int(exp_par[0]) * time_fac[time_unit]}
                        logitem['duration'] = datetime.timedelta(**delta_args)

            # TODO: Build data packet to be inserted in table blocks

        # INFO DELETIONS

        # INFO PROTECTIONS

        # INFO USER REGISTRATIONS
        # TODO: Check later, but no special actions required so far
#        if (logitem['type'] == 'newusers' and
#            (logitem['action'] == 'newusers' or
#             logitem['action'] == 'create' or
#             logitem['action'] == 'create2' or
#             logitem['action'] == 'autocreate' or
#             logitem['action'] == 'byemail')):

        # INFO RIGHTS GRANTING


def process_logitem_to_file(log_iter):
    """
    Processor for LogItem objects extracted from the 'logging' DB table in
    Wikipedia, using intermediate tmp files for bulk data loading
    """
    pass


def store_logitem_file_db(log_iter, con=None, log_file=None,
                          tmp_dir=None, file_rows=1000000,
                          etl_prefix=None):
    """
    Store processed logitems in DB from intermediate tmp data files
    """
    pass
