# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 16:36:16 2014

Main file WikiDAT

@author: jfelipe
"""

from wikidat.tasks import tasks

if __name__ == '__main__':

    # Testing with default options:
    #   - lang: 'scowiki'
    #   - date: latest dump
    task = tasks.RevisionHistoryTask(lang='ruwiki')
    task.execute(page_fan=1, rev_fan=3, db_user='auser', db_passw='apassw')
