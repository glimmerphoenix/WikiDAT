# -*- coding: utf-8 -*-
"""
Created on Mon May 12 14:04:33 2014

@author: jfelipe

Basic DB schema to import data from Wikipedia dump files.

Subsequent implementations of this module will use SQLAlchemy to manage
DB-related tasks
"""
check_database = """SHOW DATABASES LIKE '{dbname!s}'"""

drop_database = """DROP DATABASE IF EXISTS {dbname!s}"""
create_database = """CREATE DATABASE {dbname!s}
                     CHARACTER SET utf8 COLLATE utf8_general_ci
                     """

# TABLE page: Metadata about pages
"""
page_id:
  -- Unique identifier number. The page_id will be preserved across
  -- edits and rename operations, but not deletions and recreations.

page_namespace:
  -- A page name is broken into a namespace and a title.
  -- The namespace keys are UI-language-independent constants,
  -- defined in includes/Defines.php

page_title:
  -- The rest of the title, as text.
  -- Spaces are transformed into underscores in title storage.

page_restrictions:
  -- Comma-separated set of permission keys indicating who
  -- can move or edit the page.
"""
drop_page = """DROP TABLE IF EXISTS page"""
create_page = """CREATE TABLE page (
                 page_id INT UNSIGNED NOT NULL,
                 page_namespace SMALLINT NOT NULL,
                 page_title VARCHAR(255) BINARY NOT NULL,
                 page_restrictions TINYBLOB NOT NULL default ''
                 ) MAX_ROWS=1000000000 AVG_ROW_LENGTH=2048 ENGINE {engine!s};
                 """

# TABLE revision: Metadata for revisions of every page
"""
rev_id:
  -- Primary key to identify each revision

rev_page:
  -- Key to page_id. This should _never_ be invalid.

rev_user:
  -- Key to user.user_id of the user who made this edit.
  -- Stores 0 for anonymous edits and for some mass imports.
  -- Revisions without user will be marked as '-1'

rev_timestamp:
  -- Timestamp

rev_len:
  -- Uncompressed length in bytes of the revision's current source text.

rev_parent_id:
  -- Key to revision.rev_id
  -- This field is used to add support for a tree structure
     (The Adjacency List Model)

rev_is_redirect:
  -- Records whether this revision is a redirect

rev_minor_edit:
  -- Records whether the user marked the 'minor edit' checkbox.
  -- Many automated edits are marked as minor.

rev_fa:
  -- Indicates whether this revision is a FA (1) or not (0)

rev_flist:
  -- Indicates whether this revision is a Featured List (1) or not (0)

rev_ga:
  -- Indicates whether this revision is a Good Article (1) or not (0)

rev_comment:
  -- Text comment summarizing the change.
  -- This text is shown in the history and other changes lists,
  -- rendered in a subset of wiki markup by Linker::formatComment()

"""
drop_revision = """DROP TABLE IF EXISTS revision"""
create_revision = """CREATE TABLE revision (
                     rev_id INT UNSIGNED NOT NULL,
                     rev_page INT UNSIGNED NOT NULL,
                     rev_user INT NOT NULL default '0',
                     rev_timestamp DATETIME NOT NULL,
                     rev_len INT UNSIGNED NOT NULL,
                     rev_parent_id INT UNSIGNED default NULL,
                     rev_is_redirect TINYINT(1) UNSIGNED NOT NULL default '0',
                     rev_minor_edit TINYINT(1) UNSIGNED NOT NULL default '0',
                     rev_fa TINYINT(1) UNSIGNED NOT NULL default '0',
                     rev_flist TINYINT(1) UNSIGNED NOT NULL default '0',
                     rev_ga TINYINT(1) UNSIGNED NOT NULL default '0',
                     rev_comment TEXT NOT NULL default ''
                     ) MAX_ROWS=100000000000 AVG_ROW_LENGTH=2048
                     ENGINE {engine!s}
                     """

# TABLE revision_hash: MD5 hashes of text of every revision
"""
rev_id:
  - Unique id for every revision

rev_page:
  -- Key to page_id. This should _never_ be invalid.

rev_user:
  -- Key to user.user_id of the user who made this edit.
  -- Stores 0 for anonymous edits and for some mass imports.

rev_hash:
  -- MD5 hash of text of this revision

"""
drop_revision_hash = """DROP TABLE IF EXISTS revision_hash"""
create_revision_hash = """CREATE TABLE revision_hash (
                          rev_id INT UNSIGNED NOT NULL,
                          rev_page INT UNSIGNED NOT NULL,
                          rev_user INT NOT NULL default '0',
                          rev_hash varbinary(256) NOT NULL
                          ) MAX_ROWS=100000000000 AVG_ROW_LENGTH=512
                          ENGINE {engine!s}
                          """

# TABLE namespaces: identifiers of MediaWiki namespaces
# http://www.mediawiki.org/wiki/Namespaces
"""
code:
  -- Numeric id identifying this namespache

name:
  -- Name of this namespace, defaults to '' for main (encyclopedic articles)
"""
drop_namespaces = """DROP TABLE IF EXISTS namespaces"""
create_namespaces = """CREATE TABLE namespaces (
                       code SMALLINT NOT NULL,
                       name VARCHAR(50) NOT NULL
                       ) ENGINE {engine!s}
                       """
# TABLE user: identifiers of registered users
"""
user_id:
  -- Key to user.user_id of the user who made this edit.
  -- Stores 0 for anonymous edits and for some mass imports
  -- Revisions without user will be marked with '-1'
  -- Instead of NULL to speed up query lookup and sort.

user_name:
  -- Text username or IP address of the editor.
"""
drop_user = """DROP TABLE IF EXISTS user"""
create_user = """CREATE TABLE user (
                 user_id INT NOT NULL DEFAULT 0,
                 user_name VARCHAR(255) BINARY DEFAULT ''
                 ) MAX_ROWS=100000000000 AVG_ROW_LENGTH=512
                 ENGINE {engine!s}
                 """

# TABLE logging: log of administrative and relevant tasks
"""
log_id:
  -- Unique id of every log entry

log_type:
  -- Type of log action

log_action:
  -- Specific action logged by this entry

log_timestamp:
  -- Timestamp of the logged action

log_user:
  -- Id of user who performs the action

log_username:
  -- Login name of user who performs the action

log_namespace:
  -- MediaWiki namespace in which the action was performed

log_title:
  -- Title of the page on which the action was performed

log_comment:
  -- Comment describing further details about the logged action
  -- These comments provide additional insights on user blocks, page
  -- protection, etc.

log_params:
  -- Additional parameters characterizing each action

log_new_flag:
  -- In flagged revisions, rev_id of the new revised version of the
  -- reviewed page

log_old_flag:
  -- In flagged revisions, rev_id of the previous revised version of the
  -- page

"""
drop_logging = """DROP TABLE IF EXISTS logging"""
create_logging = """CREATE TABLE logging (
                    log_id INT UNSIGNED NOT NULL,
                    log_type VARCHAR(15) BINARY NOT NULL,
                    log_action VARCHAR(15) BINARY NOT NULL,
                    log_timestamp DATETIME NOT NULL,
                    log_user INT UNSIGNED NOT NULL,
                    log_username VARCHAR(255) BINARY NOT NULL DEFAULT '',
                    log_namespace INT(5) NOT NULL default 0,
                    log_title VARCHAR(255) BINARY NOT NULL DEFAULT '',
                    log_comment VARCHAR(255) BINARY NOT NULL DEFAULT '',
                    log_params VARCHAR(255) BINARY NOT NULL DEFAULT '',
                    log_new_flag INT UNSIGNED NOT NULL DEFAULT 0,
                    log_old_flag INT UNSIGNED NOT NULL DEFAULT 0
                    ) ENGINE {engine!s}
                    """

drop_block = """DROP TABLE IF EXISTS block"""
create_block = """CREATE TABLE block (
                  block_id INT UNSIGNED NOT NULL,
                  block_action VARCHAR(15) BINARY NOT NULL,
                  block_user INT UNSIGNED NOT NULL,
                  block_timestamp DATETIME NOT NULL,
                  block_target VARCHAR(255) BINARY DEFAULT '',
                  block_ip INT(10) UNSIGNED NOT NULL,
                  block_duration DECIMAL(15,1) NOT NULL
                  ) ENGINE {engine!s}
                  """

drop_user_new = """DROP TABLE IF EXISTS user_new"""
create_user_new = """CREATE TABLE user_new (
                    user_log_id INT UNSIGNED NOT NULL,
                    user_id INT UNSIGNED NOT NULL,
                    user_name VARCHAR(255) NOT NULL,
                    user_timestamp DATETIME NOT NULL,
                    user_action VARCHAR(15) NOT NULL
                    ) ENGINE {engine!s}
                    """

drop_user_level = """DROP TABLE IF EXISTS user_level"""
create_user_level = """CREATE TABLE user_level (
                  level_log_id INT UNSIGNED NOT NULL,
                  level_granter_id INT UNSIGNED NOT NULL,
                  level_username VARCHAR(255) NOT NULL,
                  level_timestamp DATETIME NOT NULL,
                  level_old VARCHAR(255) NOT NULL,
                  level_new VARCHAR(255) NOT NULL
                  ) ENGINE {engine!s}
                  """

drop_revision_IP = """DROP TABLE IF EXISTS revision_IP"""
create_revision_IP = """CREATE TABLE revision_IP (
                        rev_id INT UNSIGNED NOT NULL,
                        ip INT UNSIGNED NOT NULL
                        ) ENGINE {engine!s}
                        """

drop_revision_user_zero = """DROP TABLE IF EXISTS revision_user_zero"""
create_revision_user_zero = """CREATE TABLE revision_user_zero (
                               rev_id INT UNSIGNED NOT NULL,
                               user_name VARCHAR(255) NOT NULL
                               ) ENGINE {engine!s}
                               """

drop_IP_country = """DROP TABLE IF EXISTS IP_country"""
create_IP_country = """CREATE TABLE IP_country (
                        ip INT UNSIGNED NOT NULL,
                        country VARCHAR(3) NOT NULL
                        ) ENGINE {engine!s}
                        """
insert_namespaces = """INSERT INTO namespaces VALUES(%s, %s)"""

pk_page = """ALTER TABLE page ADD PRIMARY KEY page_id(page_id)"""
pk_revision = """ALTER TABLE revision ADD PRIMARY KEY rev_id(rev_id)"""
pk_namespaces = """ALTER TABLE namespaces ADD PRIMARY KEY code(code)"""
pk_user = """ALTER TABLE user ADD PRIMARY KEY user_id(user_id)"""
pk_logging = """ALTER TABLE logging ADD PRIMARY KEY log_id(log_id)"""
pk_block = """ALTER TABLE block ADD PRIMARY KEY block_id(block_id)"""
pk_user_new = """ALTER TABLE user_new ADD PRIMARY KEY user_id(user_id)"""
pk_user_level = """ALTER TABLE user_level ADD PRIMARY KEY
                   level_log_id(level_log_id)"""
pk_revision_IP = """ALTER TABLE revision_IP ADD PRIMARY KEY rev_id(rev_id)"""
pk_revision_user_zero = """ALTER TABLE revision_user_zero
                           ADD PRIMARY KEY rev_id(rev_id)"""
pk_IP_country = """ALTER TABLE IP_country ADD PRIMARY KEY ip(ip)"""
