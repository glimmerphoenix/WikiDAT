# -*- coding: utf-8 -*-
"""
Created on Mon Mar 31 13:26:54 2014

@author: jfelipe
"""


class DataItem(object):
    """
    Abstract class for data items to be processed by the system. Must be
    instantiated for any subclass describing a processable data item.
    Example data items: page, revision, user, logitem, etc.
    """

    def __init__(self, data_dict={}, lang=None):
        """
        Constructor method for DataItem objects

        Parameters
        ----------
        data_dict: dictionary of raw data to be processed
        lang: identifier of Wikipedia language edition from which this
        element comes from (e.g. frwiki, eswiki, dewiki...)
        """
        self.data_dict = data_dict
        self.lang = lang

    def process(self):
        """Processing interface, not implemented"""
        raise NotImplementedError(self.__class__.__name__ + '.process()')
