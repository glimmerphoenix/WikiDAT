# -*- coding: utf-8 -*-
"""
Created on Mon Mar 31 13:26:54 2014

@author: jfelipe
"""


class DataItem(dict):
    """
    Abstract class for data items to be processed by the system. Must be
    instantiated for any subclass describing a processable data item.
    Example data items: page, revision, user, logitem, etc.
    """
    def __init__(self, *args, **kwargs):
        """
        Constructor method for DataItem objects
        """
        super(DataItem, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        # optional processing for data items will go here
        super(DataItem, self).__setitem__(key, value)
