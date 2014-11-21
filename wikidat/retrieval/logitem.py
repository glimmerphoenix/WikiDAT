# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:14:21 2014

@author: jfelipe
"""
from data_item import DataItem


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
