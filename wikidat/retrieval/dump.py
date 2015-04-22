# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:13:19 2014

@author: jfelipe
"""
from lxml import etree
import subprocess
import os
from .page import Page
from .revision import Revision
from .logitem import LogItem
from wikidat.utils import maps


class DumpFile(object):
    """
    Models dump files and associated methods to extract their data
    """
    def __init__(self, path):
        self.path = path

    def open_dump(self):
        """
        Turns a path to a dump file into a file-like object of (decompressed)
        XML data.

        :Parameters:
            path : `str`
                the path to the dump file to read
        """
        match = maps.EXT_RE.search(self.path)
        ext = match.groups()[0]
        p = subprocess.Popen(
            "%s %s" % (maps.EXTENSIONS[ext], self.path),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=open(os.devnull, "w")
        )
        # sys.stderr.write(p.stdout.read(1000))
        # return False
        return p.stdout

    def get_namespaces(self):
        in_stream = self.open_dump()
        for event, elem in etree.iterparse(in_stream, recover=True,
                                           huge_tree=True):
            # Drop tag namespace
            tag = elem.tag.split('}')[1]

            # Return namespace info (dict)
            if tag == 'namespaces':
                ns_dict = {int(c.attrib.get('key')): c.text for c in elem}
                ns_dict[0] = ''
                return ns_dict


def process_xml(dump_file=None):
    rev_parent_id = None
    page_dict = None

    in_stream = dump_file.open_dump()
    for event, elem in etree.iterparse(in_stream, recover=True,
                                       huge_tree=True):
        # Drop tag namespace
        tag = elem.tag.split('}')[1]

        # Load namespace info
        if tag == 'namespaces':
                ns_dict = {int(c.attrib.get('key')): c.text for c in elem}
                ns_dict[0] = ''
                ns_names = {c.text: int(c.attrib.get('key')) for c in elem}
                ns_names[''] = 0

        # Retrieve contributor info to be embedded in current revision
        # TODO: Handle contributor information properly
        if tag == 'contributor':
            # Build dict {tag:text} for contributor info
            contrib_dict = {x.tag.split('}')[1]: x.text for x in elem}
#                yield User(data_dict=contrib_dict)

        if tag == 'revision':
            # First revision for current page, retrieve page info
            if page_dict is None:
                page = elem.getparent()
                # Build dict {tag:text} for all children of page
                # above first revision tag
                page_dict = {x.tag.split('}')[1]: x.text for x in page}

            # Build dict {tag:text} for all children of revision
            rev_dict = {x.tag.split('}')[1]: x.text for x in elem}
            # Embed page_id, contrib_dict and return item
            rev_dict['page_id'] = page_dict['id']
            # To skip pattern matching for non-articles
            rev_dict['ns'] = page_dict['ns']
            rev_dict['contrib_dict'] = contrib_dict
            rev_dict['rev_parent_id'] = rev_parent_id

            rev_dict['item_type'] = 'revision'
            yield Revision(rev_dict)

            # Save rev_id (rev_parent_id of the next revision item)
            rev_parent_id = rev_dict['id']
            # Clear up revision and contributor dictionaries
            rev_dict = None
            contrib_dict = None
            # Clear memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        if tag == 'page':
            page_dict['item_type'] = 'page'
            yield Page(page_dict)
            # Clear memory
            page_dict = None
            rev_parent_id = None
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        if tag == 'logitem':
            log_dict = {x.tag.split('}')[1]: x.text for x in elem}
            log_dict['contrib_dict'] = contrib_dict
            # Get namespace for this log item from page title prefix
            if 'logtitle' in log_dict and log_dict['logtitle']:
                ns_prefix = log_dict['logtitle'].split(':')
                if (len(ns_prefix) == 2 and ns_prefix[0] in ns_names):
                    log_dict['namespace'] = ns_names[ns_prefix[0]]
                else:
                    log_dict['namespace'] = 0
            else:
                log_dict['logtitle'] = ''
                log_dict['namespace'] = -1000  # Fake namespace

            yield LogItem(log_dict)
            # Clear memory
            log_dict = None
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
