-- CREATE DATABASE wx_furwiki_032012;

-- USE wx_furwiki_032012;

CREATE TABLE page (
  -- Unique identifier number. The page_id will be preserved across
  -- edits and rename operations, but not deletions and recreations.
  page_id int unsigned NOT NULL ,
  
  -- A page name is broken into a namespace and a title.
  -- The namespace keys are UI-language-independent constants,
  -- defined in includes/Defines.php
  page_namespace smallint NOT NULL,
  
  -- The rest of the title, as text.
  -- Spaces are transformed into underscores in title storage.
  page_title varchar(255) BINARY NOT NULL,

  -- Comma-separated set of permission keys indicating who
  -- can move or edit the page.
  page_restrictions tinyblob NOT NULL default '',

  PRIMARY KEY page_id (page_id)
) MAX_ROWS=1000000000 AVG_ROW_LENGTH=2048 ENGINE MyISAM;

--
-- Every edit of a page creates also a revision row.
-- This stores metadata about the revision, and a reference
-- to the text storage backend.
--
CREATE TABLE revision (
  -- Primary key to identify each revision
  rev_id int unsigned NOT NULL ,
  
  -- Key to page_id. This should _never_ be invalid.
  rev_page int unsigned NOT NULL,
  
  -- Key to user.user_id of the user who made this edit.
  -- Stores 0 for anonymous edits and for some mass imports.
  -- Revisions without user will be marked as '-1'
  rev_user int NOT NULL default '0',
  
  -- Timestamp
  rev_timestamp datetime NOT NULL,
  
  -- Uncompressed length in bytes of the revision's current source text.
  rev_len int unsigned NOT NULL,
  
  -- Key to revision.rev_id
  -- This field is used to add support for a tree structure (The Adjacency List Model)
  rev_parent_id int unsigned default NULL,

  -- Records whether this revision is a redirect
  rev_is_redirect tinyint(1) unsigned NOT NULL default '0',

  -- Records whether the user marked the 'minor edit' checkbox.
  -- Many automated edits are marked as minor.
  rev_minor_edit tinyint(1) unsigned NOT NULL default '0',

  -- Records whether this revision is a FA
  rev_fa tinyint(1) unsigned NOT NULL default '0',
  
  -- Records whether this revision is a Featured List
  rev_flist tinyint(1) unsigned NOT NULL default '0',
  
  -- Records whether this revision is a Good Article
  rev_ga tinyint(1) unsigned NOT NULL default '0',
  
  -- Text comment summarizing the change.
  -- This text is shown in the history and other changes lists,
  -- rendered in a subset of wiki markup by Linker::formatComment()
  rev_comment text NOT NULL default '',

  PRIMARY KEY rev_id (rev_id)
) MAX_ROWS=100000000000 AVG_ROW_LENGTH=2048 ENGINE MyISAM;

CREATE TABLE people (
  -- Key to user.user_id of the user who made this edit.
  -- Stores 0 for anonymous edits and for some mass imports
  -- Revisions without user will be marked with '-1'
  -- Instead of NULL to speed up query lookup and sort
  rev_user int NOT NULL default 0,

  -- Text username or IP address of the editor.
  rev_user_text varchar(255) binary default '',

  PRIMARY KEY rev_user (rev_user)
) MAX_ROWS=100000000000 AVG_ROW_LENGTH=512 ENGINE MyISAM;

INSERT INTO people VALUES(-1, 'NA'),(0, 'Anonymous');

CREATE TABLE revision_hash (
  rev_id int unsigned NOT NULL ,
  -- Key to page_id. This should _never_ be invalid.
  rev_page int unsigned NOT NULL,
  -- Key to user.user_id of the user who made this edit.
  -- Stores 0 for anonymous edits and for some mass imports.
  rev_user int NOT NULL default '0',
  rev_hash varbinary(256) NOT NULL,
  PRIMARY KEY rev_id(rev_id)
) MAX_ROWS=100000000000 AVG_ROW_LENGTH=512 ENGINE MyISAM;

-- Special table storing info about namespaces
CREATE TABLE namespaces (
  code smallint NOT NULL,
  name varchar(50) NOT NULL,
  PRIMARY KEY code (code)
) ENGINE MyISAM;

CREATE TABLE logging (
log_id int unsigned not null,
log_type varchar(15) binary not null,
log_action varchar(15) binary not null,
log_timestamp datetime not null,
log_user int unsigned not null,
log_username varchar(255) binary NOT NULL default '',
log_namespace int(5) not null default 0,
log_title varchar(255) binary NOT NULL default '',
log_comment varchar(255) binary NOT NULL default '',
log_params varchar(255) binary NOT NULL default '',
log_new_flag int unsigned not null default 0,
log_old_flag int unsigned not null default 0,
PRIMARY KEY log_id (log_id)
) ENGINE MyISAM;