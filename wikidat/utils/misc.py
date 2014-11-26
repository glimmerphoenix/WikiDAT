# -*- coding: utf-8 -*-
"""
Created on Wed Nov 26 12:26:46 2014

@author: jfelipe
"""

SUFFIXES = {1000: ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
            1024: ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']}


def hfile_size(size, kb_1024_bytes=True):
    '''Convert a file size to human-readable form.
    Source: http://getpython3.com/diveintopython3/your-first-python-program.html#divingin

    Keyword arguments:
    size -- file size in bytes
    a_kilobyte_is_1024_bytes -- if True (default), use multiples of 1024
                                if False, use multiples of 1000

    Returns: string

    '''
    if size < 0:
        raise ValueError('File size must be non-negative')

    multiple = 1024 if kb_1024_bytes else 1000
    for suffix in SUFFIXES[multiple]:
        size /= multiple
        if size < multiple:
            return '{0:.1f} {1}'.format(size, suffix)

    raise ValueError('File size too large')
