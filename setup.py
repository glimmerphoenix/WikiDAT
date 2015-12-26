# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 16:36:40 2014

Wikipedia Data Analysis Toolkit
Project metadata file

@author: jfelipe
"""
from setuptools import setup, find_packages

setup(
    name='WikiDAT',
    version='0.1',
    description='Wikipedia Data Analysis Toolkit',
    author='Felipe Ortega',
    author_email='glimmerphoenix@gmail.com',
    url='http://glimmerphoenix.github.io/WikiDAT/',
    packages=find_packages(),
    license='GPL v3',
    long_description=open('README.md').read(),
    install_requires=[
        "pymysql>=0.6.2",
        "lxml >= 3.3.1",
        "beautifulsoup4>=4.3.2",
        "python-dateutil>=1.5",
        "requests>=2.2.1",
        "ujson>=1.3.0",
        "redis>=2.10.3"
    ],
)
