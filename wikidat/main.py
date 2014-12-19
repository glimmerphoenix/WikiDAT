# -*- coding: utf-8 -*-
"""Main module of WikiDAT

Currently, it reads configuration options from either a file or command-line
input arguments (merging them appropriately). Then, it calls individual tools
if their corresponding optional sections are present in the config file.

Command-line arguments always have higher precedence than options in config
file.

Source for combining ConfigParser and argparse logic:
http://blog.vwelch.com/2011/04/combining-configparser-and-argparse.html

"""

import argparse
import configparser
import codecs
import sys
import time
import json
from wikidat.tasks import tasks


def get_config(filename='config.ini'):
    r"""Read options from configuration file

    Parameters
    ----------
    filename: string
        Name of configuration file with options. Mandatory sections are
        ``General`` and ``Database``. These sections must always be included
        in that file.

    Returns
    -------
    opts: dict
        A ``dict`` with all configured options, to be merged with command-line
        options.

    """
    mandatory_secs = ['General', 'Database']
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
    if config.has_option('General', 'debug'):
        opts['debug'] = config.getboolean('General', 'debug')

    opts_database = dict(config.items('Database'))
    if config.has_option('Database', 'port'):
        opts_database['port'] = config.getint('Database', 'port')
    opts.update(opts_database)

    if config.has_section('ETL:RevHistory'):
        opts_etl_revhist = dict()
        sec = 'ETL:RevHistory'
        if config.has_option(sec, 'etl_lines'):
            opts_etl_revhist['etl_lines'] = config.getint(sec, 'etl_lines')
        if config.has_option(sec, 'page_fan'):
            opts_etl_revhist['page_fan'] = config.getint(sec, 'page_fan')
        if config.has_option(sec, 'rev_fan'):
            opts_etl_revhist['rev_fan'] = config.getint(sec, 'rev_fan')
        if config.has_option(sec, 'page_cache_size'):
            opts_etl_revhist['page_cache_size'] = config.getint(sec, 'page_cache_size')
        if config.has_option(sec, 'rev_cache_size'):
            opts_etl_revhist['rev_cache_size'] = config.getint(sec, 'rev_cache_size')
        if config.has_option(sec, 'base_ports'):
            opts_etl_revhist['base_ports'] = json.loads(config.get(sec, 'base_ports'))
        if config.has_option(sec, 'control_ports'):
            opts_etl_revhist['control_ports'] = json.loads(config.get(sec, 'control_ports'))
        if config.has_option(sec, 'detect_FA'):
            opts_etl_revhist['detect_FA'] = config.getboolean(sec, 'detect_FA')
        if config.has_option(sec, 'detect_FLIST'):
            opts_etl_revhist['detect_FLIST'] = config.getboolean(sec, 'detect_FLIST')
        if config.has_option(sec, 'detect_GA'):
            opts_etl_revhist['detect_GA'] = config.getboolean(sec, 'detect_GA')
        opts.update(opts_etl_revhist)

    if config.has_section('ETL:PagesLogging'):
        opts_etl_logging = dict()
        sec = 'ETL:PagesLogging'
        if config.has_option(sec, 'etl_lines'):
            opts_etl_logging['etl_lines'] = config.getint(sec, 'etl_lines')
        if config.has_option(sec, 'log_fan'):
            opts_etl_logging['log_fan'] = config.getint(sec, 'log_fan')
        if config.has_option(sec, 'log_cache_size'):
            opts_etl_logging['log_cache_size'] = config.getint(sec, 'log_cache_size')
        if config.has_option(sec, 'base_ports'):
            opts_etl_logging['base_ports'] = json.loads(config.get(sec, 'base_ports'))
        if config.has_option(sec, 'control_ports'):
            opts_etl_logging['control_ports'] = json.loads(config.get(sec, 'control_ports'))
        opts.update(opts_etl_logging)

    opts['tool_secs'] = set(config.sections()) - set(mandatory_secs)
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
            'debug': False,
            'etl_lines': 1,
            'page_fan': 1,
            'rev_fan': 1,
            'log_fan': 1,
            'page_cache_size': 200000,
            'rev_cache_size': 1000000,
            'log_cache_size': 1000000,
            'db_user': 'auser',
            'db_passw': 'apassw',
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
    parser.add_argument('--debug', dest='debug',
                        action='store_true',
                        help=''.join(['Turn debug mode ON ']))
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
                        help=''.join(['Number of worker processes to deal with ',
                                      'page elements in each ETL line.'])
                        )
    parser.add_argument('--rev_fan', type=int, metavar='NUM_REV_WORKERS',
                        help=''.join(['Number of worker processes to deal with ',
                                      'revision elements in each ETL line.'])
                        )
    parser.add_argument('--log_fan', type=int, metavar='NUM_LOG_WORKERS',
                        help=''.join(['Number of worker processes to deal with ',
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
    parser.add_argument('--log_cache_size', type=int, metavar='CACHE_SIZE',
                        help=''.join(['Num. of rows to accumulate in tmp ',
                                      'data dir for page elements before ',
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
    start = time.time()
    print
    print "*** WIKIPEDIA DATA ANALYSIS TOOLKIT ***"
    print
    if args.debug:
        print args

    # TODO: Control for incompatible combinations of command-line arguments

    if 'ETL:RevHistory' in opts['tool_secs']:
        # Testing with default options:
        #   - lang: 'scowiki'
        #   - date: latest dump
        task = tasks.RevHistoryTask(lang=args.lang,
                                    date=args.date,
                                    etl_lines=args.etl_lines,
                                    host=args.host, port=args.port,
                                    db_name=args.db_name, db_user=args.db_user,
                                    db_passw=args.db_passw,
                                    db_engine=args.db_engine)

        task.execute(page_fan=args.page_fan, rev_fan=args.rev_fan,
                     page_cache_size=args.page_cache_size,
                     rev_cache_size=args.rev_cache_size,
                     mirror=args.mirror, download_files=args.download_files,
                     base_ports=args.base_ports,
                     control_ports=args.control_ports,
                     dumps_dir=args.dumps_dir,
                     debug=args.debug)

    if 'ETL:RevMeta' in opts['tool_secs']:
        pass

    if 'ETL:PagesLogging' in opts['tool_secs']:
        # Testing with default options:
        #   - lang: 'scowiki'
        #   - date: latest dump
        task = tasks.PagesLoggingTask(lang=args.lang,
                                      date=args.date,
                                      etl_lines=1,
                                      host=args.host, port=args.port,
                                      db_name=args.db_name,
                                      db_user=args.db_user,
                                      db_passw=args.db_passw,
                                      db_engine=args.db_engine)

        task.execute(log_fan=args.log_fan,
                     log_cache_size=args.log_cache_size,
                     mirror=args.mirror, download_files=args.download_files,
                     base_ports=args.base_ports,
                     control_ports=args.control_ports,
                     dumps_dir=args.dumps_dir,
                     debug=args.debug)

    print "Finish time is %s" % (time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                               time.localtime()))
    end = time.time()
    print "Total execution time: %.2f mins." % ((end-start)/60.)
    print "All tasks FINISHED, exit."
