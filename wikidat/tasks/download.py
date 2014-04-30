# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 18:02:16 2014

Download manager for dump files

@author: jfelipe
"""
from bs4 import BeautifulSoup
import multiprocessing as mp
import itertools
import urllib2
import requests
import re
import os
import hashlib


class RevisionDownloader(object):
    """
    Downloads revision dump files from http://dumps.wikimedia.org
    There are different instances of revision dumps (metadata only, complete
    history, etc.) which are managed by its subclasses
    """

    def __init__(self, mirror="http://dumps.wikimedia.org/", language):
        self.language = language
        self.mirror = mirror
        self.target_url = "".join([self.mirror, self.language])
        html_dates = requests.get(self.target_url)
        soup_dates = BeautifulSoup(html_dates.text)

        # Get hyperlinks and timestamps of dumps for each available date
        # Ignore first line with link to parent folder
        self.dump_urldate = [link.get('href')
                             for link in soup_dates.find_all('a')][1:]
        self.dump_dates = [link.text
                           for link in soup_dates.find_all('td', 'm')][1:]
        self.match_pattern = ""  # Store re in subclass for type of dump file
        self.dump_dir = language + "_dumps"
        self.dump_paths = []  # List of paths to dumps in local filesystem
        self.md5_codes = {}  # Dict for md5 codes to verify dump files

    def download(self, dump_date):
        """
        Download all dump files for a given language in their own folder
        Return
        """
        # Obtain content for dump summary page on requested date
        base_url = "".join([self.target_url, "/", dump_date])
        html_dumps = requests.get(self.target_url)
        soup_dumps = BeautifulSoup(html_dumps.text)

        # First of all, check that status of dump files is Done (ready)
        status_dumps = soup_dumps.find('p', class_='status').span.text
        if status_dumps != 'Dump complete':
            # TODO: Raise error if dump is not ready on requested date
            # Think about offering an alternative to the user (latest dump)
            pass

        # Dump file(s) ready, proceed with list of files and download
        self.dump_urls = [link.get('href') for link in (soup_dumps.
                          find_all(href=re.compile(self.match_pattern)))]

        # Create directory for dump files if needed
        if not os.path.exists(self.dump_dir):
            os.makedirs(self.dump_dir)

        for url1, url2 in itertools.izip_longest(self.dump_urls[::2],
                                                 self.dump_urls[1::2],
                                                 fillvalue=None):
            # Due to bandwith limitations in WMF mirror servers, you will not
            # be allowed to download more than 2 dump files at the same time
            proc_get1 = mp.Process(target=self._get_file,
                                   args=(dump_url, self.dump_dir,))
            proc_get1.start()
            # Control here for even number of dumps (last element is None)
            if url2 is not None:
                proc_get2 = mp.Process(target=self._get_file,
                                       args=(dump_url, self.dump_dir,))
                proc_get2.start()
                proc_get2.join()

            # Wait until all downloads are finished
            proc_get1.join()

        # Verify integrity of downloaded dumps
        self._verify(dump_date)
        # Return list of paths to dumpfiles for data extraction
        return self.dump_paths

    def _get_file(self, dump_url, dump_dir):
        """
        Retrieve individual dump file from dump_url and save it in dump_dir
        """
        file_name = dump_url.split('/')[-1]
        dump_file = urllib2.urlopen(base_url + dump_url)
        path_file = os.path.join(dump_dir, file_name)
        store_file = open(os.path.join(dump_dir, file_name), 'wb')
        file_meta = dump_file.info()
        file_size = int(file_meta.getheaders("Content-Length")[0])
        print "Downloading: %s Bytes: %s" % (file_name, file_size)
        store_file.write(dump_file.read())
        store_file.close()
        self.dump_paths.append(path_file)

    def _verify(self, dump_date):
        """
        Verify integrity of downloaded dump files against MD5 checksums
        """
        html_dumps = requests.get("".join([self.target_url, "/",
                                           dump_date]))
        soup_dumps = BeautifulSoup(html_dumps.text)
        md5_url = soup_dumps.find('p', class_='checksum').a['href']
        md5_codes = requests.get("".join([self.mirror, md5_url])).text
        md5_codes = md5_codes.split('\n')

        for fileitem in md5_codes:
            f = item.split()
            if len(f) > 0:
                self.md5_codes[f[1]] = f[0]  # dict[fname] = md5code

        for path in self.dump_paths:
            filename = path.split()[1]  # Get filename from path
            file_md5 = hashlib.md5(open(path).read()).hexdigest()
            original_md5 = self.md5_codes[filename]
            # TODO: Compare md5 hash of retrieved file with original
            if file_md5 != original_md5:
                # Raise error if they do not match
                pass


class RevisionHistoryDownloader(RevisionDownloader):
    """
    Downloads revision history files from http://dumps.wikimedia.org
    These are files with complete revision history information (all text)
    """

    def __init__(self, mirror, language):
        super(RevisionHistoryDownloader, self).__init__(mirror=mirror,
                                                        language=language)
        self.match_pattern = 'pages-meta-history[\S]*\.xml\.7z'


class RevisionMetaDownloader(RevisionDownloader):
    """
    Downloads revision meta files from http://dumps.wikimedia.org
    These are files with complete metadata for every revision (including
    rev_len, as stored in Wikipedia DB) but no revision text
    """

    def __init__(self, mirror, language):
        super(RevisionHistoryDownloader, self).__init__(mirror=mirror,
                                                        language=language)
        self.match_pattern = 'stub-meta-history[\d]+\.xml\.gz'
