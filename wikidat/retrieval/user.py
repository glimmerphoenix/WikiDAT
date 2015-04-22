# -*- coding: utf-8 -*-
"""
Created on Mon Mar 31 18:02:54 2014

@author: jfelipe
"""

from wikidat.utils import dbutils
from .data_item import DataItem


class User(DataItem):
    """
    Models Page elements in Wikipedia database dumps
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor method for User objects. Must forward params to
        parent class DataItem (mandatory inheritance)

        Parameters
        ----------
        data_dict: dictionary of raw data to be processed
        lang: identifier of Wikipedia language edition from which this
        element comes from (e.g. frwiki, eswiki, dewiki...)
        """
        super(User, self).__init__(*args, **kwargs)
