# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 16:36:16 2014

Main file WikiDAT

Source for combining ConfigParser and argparse:
http://blog.vwelch.com/2011/04/combining-configparser-and-argparse.html

@author: jfelipe
"""

import argparse
import configparser
import codecs
import sys
import json
from wikidat.tasks import tasks


def get_config(filename='config.ini'):
    """
    Read options from configuration file
    """
    mandatory_secs = ['General', 'Database', 'ETL']
    config = configparser.SafeConfigParser()
    config._interpolation = configparser.ExtendedInterpolation()

    # Open file with UTF-8 encoding
    with codecs.open(filename, 'r', encoding='utf-8') as f:
        config.readfp(f)
    f.close()

    for section in mandatory_secs:
        if not config.has_section(section):
            print "Sorry, information about %s is mandatory " % section
            print "but no section named %s was found." % section
            print "Please add the required options for section %s" % section
            print "to proceed with execution."
            print "The program will quit now!"
            sys.exit()

    # Read all options with type
    opts = dict(config.items('General'))
    if config.has_option('General', 'download_files'):
        opts['download_files'] = config.getboolean('General', 'download_files')

    opts_database = dict(config.items('Database'))
    if config.has_option('Database', 'port'):
        opts_database['port'] = config.getint('Database', 'port')
    opts.update(opts_database)

    opts_etl = dict()
    if config.has_option('ETL', 'etl_lines'):
        opts_etl['etl_lines'] = config.getint('ETL', 'etl_lines')
    if config.has_option('ETL', 'page_fan'):
        opts_etl['page_fan'] = config.getint('ETL', 'page_fan')
    if config.has_option('ETL', 'rev_fan'):
        opts_etl['rev_fan'] = config.getint('ETL', 'rev_fan')
    if config.has_option('ETL', 'page_cache_size'):
        opts_etl['page_cache_size'] = config.getint('ETL', 'page_cache_size')
    if config.has_option('ETL', 'rev_cache_size'):
        opts_etl['rev_cache_size'] = config.getint('ETL', 'rev_cache_size')
    if config.has_option('ETL', 'base_ports'):
        opts_etl['base_ports'] = json.loads(config.get('ETL', 'base_ports'))
    if config.has_option('ETL', 'control_ports'):
        opts_etl['control_ports'] = json.loads(config.get('ETL',
                                                          'control_ports'))
    if config.has_option('ETL', 'detect_FA'):
        opts_etl['detect_FA'] = config.getboolean('ETL', 'detect_FA')
    if config.has_option('ETL', 'detect_FLIST'):
        opts_etl['detect_FLIST'] = config.getboolean('ETL', 'detect_FLIST')
    if config.has_option('ETL', 'detect_GA'):
        opts_etl['detect_GA'] = config.getboolean('ETL', 'detect_GA')
    opts.update(opts_etl)

    return opts

if __name__ == '__main__':
    # Initialize command-line args parser
    conf_parser = argparse.ArgumentParser(
        description='Wikipedia Data Analysis Toolkit',
        # Turn off help, so we print all options in response to -h
        add_help=False
        )

    conf_parser.add_argument('-c', '--conf_file', metavar='FILE',
                             help=''.join(['Configuration file from which ',
                                           'default values will be read. ',
                                           'These default values will be ',
                                           'overridden by any command-line ',
                                           'arguments introduced hereafter.']),
                             default='config.ini')

    # Partially parse command-line options to check whether a config
    # file has been specified
    args, remain_args = conf_parser.parse_known_args()

    # Set up default command-line options
    # TODO: Deal with default value for 'db_name'
    opts = {'lang': 'scowiki',
            'date': 'latest',
            'mirror': 'http://dumps.wikimedia.your.org/',
            'download_files': True,
            'dumps_dir': None,
            'etl_lines': 1,
            'page_fan': 1,
            'rev_fan': 1,
            'page_cache_size': 1000000,
            'rev_cache_size': 1000000,
            'db_user': 'root',
            'db_passw': '',
            'db_engine': 'ARIA',
            'base_ports': 10000,
            'control_ports': 11000,
            'detect_FA': True,
            'detect_FLIST': True,
            'detect_GA': True
            }
    # If some options are overridden by config file, update them
    if args.conf_file:
        file_opts = get_config(args.conf_file)
    opts.update(file_opts)

    # Create new parser, inherit all options read from config file
    # Don't surpress add_help here so it will handle -h
    parser = argparse.ArgumentParser(
        # Inherit options from config_parser
        parents=[conf_parser],
        # print script description with -h/--help
        description=__doc__,
        # Don't mess with format of description
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )

    parser.set_defaults(**opts)
    parser.add_argument('--lang', metavar='LANG',
                        help=''.join(['Code of wikipedia language to be ',
                                      'processed (e.g. eswiki).'])
                        )
    parser.add_argument('--date', metavar='DATE',
                        help=''.join(['Full date of dump files to be ',
                                      'processed (e.g. 20140519) or the ',
                                      'string "latest" for the last ',
                                      'available dump.'])
                        )
    parser.add_argument('--mirror', metavar='URL_MIRROR',
                        help=''.join(['URL of the mirror to obtain the dump ',
                                      'files.'])
                        )
    parser.add_argument('--dumps_dir', metavar='PATH',
                        help=''.join(['Absolute or relative path to the ',
                                      'folder containing the dump files to ',
                                      'be processed.'])
                        )
    parser.add_argument('--download_files', dest='download_files',
                        action='store_true',
                        help=''.join(['Download dump files for the specified ',
                                      'language, date and mirror site.']))
    parser.add_argument('--no_download_files', dest='download_files',
                        action='store_false',
                        help=''.join(['Skip download of dump files.']))
    parser.add_argument('--etl_lines', type=int, metavar='NUM_ETL_LINES',
                        help=''.join(['Number of ETL processing lines to be ',
                                      'executed. More lines could be added ',
                                      'if enough CPUs/cores are available ',
                                      'for data processing.'])
                        )
    parser.add_argument('--page_fan', type=int, metavar='NUM_PAGE_WORKERS',
                        help=''.join(['Number of worker process to deal with ',
                                      'page elements in each ETL line.'])
                        )
    parser.add_argument('--rev_fan', type=int, metavar='NUM_REV_WORKERS',
                        help=''.join(['Number of worker process to deal with ',
                                      'revision elements in each ETL line.'])
                        )
    parser.add_argument('--page_cache_size', type=int, metavar='CACHE_SIZE',
                        help=''.join(['Num. of rows to accumulate in tmp ',
                                      'data dir for page elements before ',
                                      'flushing data to local DB.'])
                        )
    parser.add_argument('--rev_cache_size', type=int, metavar='CACHE_SIZE',
                        help=''.join(['Num. of rows to accumulate in tmp ',
                                      'data dir for revision elements before ',
                                      'flushing data to local DB.'])
                        )
    parser.add_argument('--db_name', type=str, metavar='DB_NAME',
                        help=''.join(['Name of local DB.'])
                        )
    parser.add_argument('--db_user', type=str, metavar='DB_USER',
                        help=''.join(['User login to connect to local DB.'])
                        )
    parser.add_argument('--db_passw', metavar='DB_PASSWORD',
                        help=''.join(['Password to connect to local DB.'])
                        )
    parser.add_argument('--db_engine', metavar='DB_ENGINE',
                        help=''.join(['Specific DB engine to store data ',
                                      'locally. Currently, only ARIA or ',
                                      'MyISAM engines are supported.'])
                        )
    parser.add_argument('--base_ports', nargs='+', type=int,
                        help=''.join(['List of base port numbers to be ',
                                      'used by each ETL line. Communication ',
                                      'port numbers will be chosen as ',
                                      'consecutive ports starting from the '
                                      'specified base port. ',
                                      'Each ETL consumes at least 4 port ',
                                      'numbers (1 ventilator, 1 page worker ',
                                      '1 revision worker and 1 sink).']))
    parser.add_argument('--control_ports', nargs='+', type=int,
                        help=''.join(['List of control port numbers to be ',
                                      'used by each ETL line. Each ETL will ',
                                      'utilize a socket open on this port to ',
                                      'stop all workers when all elements ',
                                      'from a dump file have been ',
                                      'processed.']))
    parser.add_argument('--detect_FA', dest='detect_FA', action='store_true',
                        help=''.join(['Revisions corresponding to Featured ',
                                      'Articles will be detected.']))
    parser.add_argument('--no_detect_FA', dest='detect_FA',
                        action='store_false',
                        help=''.join(['Skip detection of revisions of ',
                                      'Featured Articles.']))
    parser.add_argument('--detect_FLIST', dest='detect_FLIST',
                        action='store_true',
                        help=''.join(['Revisions corresponding to Featured ',
                                      'Lists will be detected.']))
    parser.add_argument('--no_detect_FLIST', dest='detect_FLIST',
                        action='store_false',
                        help=''.join(['Skip detection of revisions of ',
                                      'Featured Lists.']))
    parser.add_argument('--detect_GA', dest='detect_GA',
                        action='store_true',
                        help=''.join(['Revisions corresponding to Good ',
                                      'Articles will be detected.']))
    parser.add_argument('--no_detect_GA', dest='detect_GA',
                        action='store_false',
                        help=''.join(['Skip detection of revisions of ',
                                      'Good Articles.']))
    # Finally, any option directly specified on the command-line will
    # override previous values assigned to any argument
    args = parser.parse_args(remain_args)
    print args

    # TODO: Control for incompatible combinations of command-line arguments

    # Testing with default options:
    #   - lang: 'scowiki'
    #   - date: latest dump
    task = tasks.RevisionHistoryTask(lang=args.lang,
                                     date=args.date,
                                     etl_lines=args.etl_lines)

    task.execute(page_fan=args.page_fan, rev_fan=args.rev_fan,
                 page_cache_size=args.page_cache_size,
                 rev_cache_size=args.rev_cache_size,
                 host=args.host, port=args.port,
                 db_name=args.db_name, db_user=args.db_user,
                 db_passw=args.db_passw, db_engine=args.db_engine,
                 mirror=args.mirror, download_files=args.download_files,
                 base_ports=args.base_ports,
                 control_ports=args.control_ports,
                 dumps_dir=args.dumps_dir)
