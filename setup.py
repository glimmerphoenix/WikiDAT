# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 16:36:40 2014

Wikipedia Data Analysis Toolkit
Project metadata file

@author: jfelipe
"""
from distutils.core import setup

setup(
    name='WikiDAT',
    version='0.1',
    description='Wikipedia Data Analysis Toolkit',
    author='Felipe Ortega',
    author_email='glimmerphoenix@gmail.com',
    url='http://glimmerphoenix.github.io/WikiDAT/',
    packages=['wikidat', 'wikidat.sources', 'wikidat.tools', 'wikidat.utils'],
    license='GPL v3',
    long_description=open('README.md').read(),
    install_requires=[
        "MySQL-python >= 1.2.3",
        "lxml >= 3.3.1",
        "python-dateutil >= 1.5",
        "beautifulsoup4 >= 4.3.2",
        "requests >= 2.2.1",
    ],
)
