# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 12:59:52 2014

@author: jfelipe

This is a collection of useful database queries to pre-calculate certain
data for basic activity metrics (pages and users).

Some queries produce tables to keep the pre-calculated values in DB
Other queries illustrate how to combine DB information to produce descriptive
activity metrics.

Template strings will be used to facilitate the subsequent substitution
of required fields to complete the query.
"""

"""
***********************************
*** TABLES FOR DATA PREPARATION ***
***********************************
"""

"""
***********
** PAGES **
***********
"""
"""
Table name: articles
Content: All pages in namespace = 0
Goal: Facilitate tracking article-based metrics
"""

del_articles = "DROP TABLE IF EXISTS articles"
articles = """CREATE TABLE articles AS
              (SELECT page_id, page_title
              FROM page
              WHERE page_namespace = 0)"""
ind_articles = """ALTER TABLE articles
                  ADD PRIMARY KEY page_id(page_id)"""

"""
Table name: page_min_ts
Content: Timestamp of first revision for every user
Goal: Track start of user's revision history, calculate lifetime
"""
del_page_min_ts = "DROP TABLE IF EXISTS page_min_ts"
page_min_ts = """CREATE TABLE page_min_ts AS
                 (SELECT rev_id, rev_page, rev_user,
                 MIN(rev_timestamp) rev_timestamp , rev_len, rev_is_redirect
                 FROM revision
                 GROUP BY rev_page)"""
ind_page_min_ts = """ALTER TABLE page_min_ts
                     ADD PRIMARY KEY "rev_id(rev_id)"""
ind2_page_min_ts = """ALTER TABLE page_min_ts ADD INDEX
                      rev_page(rev_page)"""

"""
Table name: page_min_articles
Content: Timestamp of first revision for every user in main namespace
Goal: Track start of user's revision history in articles
"""
del_page_min_articles = "DROP TABLE IF EXISTS page_min_articles"
page_min_articles = """CREATE TABLE page_min_articles AS
                       (SELECT * from page_min_ts a
                       INNER JOIN articles b
                       ON a.rev_page = b.page_id)"""
ind_page_min_articles = """ALTER TABLE page_min_articles
                           ADD PRIMARY KEY rev_id(rev_id)"""
ind2_page_min_articles = """ALTER TABLE page_min_articles
                            ADD PRIMARY KEY rev_page(rev_page)"""

"""
FEATURED ARTICLES (FAs): Any article in main namespace can be promoted or
demoted from FA status at any time. This complicates apparently simple count
metrics considering FAs.

Below, the approach to calculate this metrics is as follows:

1. Create a temporary table storing, for each month in revision history, the
earliest timestamp for articles promoted to FA status at least once in that
month or in previous months, as well as the latest timestamp for such
articles on which they still retained the FA status.

2. Create a temporay table storing, for each month in revision history, the
latest timestamp for all articles promoted to FA status at least once in
that month or in previous months on which they did not have FA status.

3. FAs up to a certain month: combine the two tables above and count all
pages for which the latest timestamp retaining FA status is > the latest
timestamp on which they did not have FA status (that is, all pages whose
latest known status is FA up to that month)

4. New FAs in a certain month: count all pages that were granted FA status
for the first time in that month (that is, min timestamp with FA status falls
within that month). We assume it is very unlikely that a page just promoted
to FA status is demoted within the same month of its promotion.
"""

"""
Table name: fa_ts_{month!s}
Content: Min and max rev_timestamp of pages who reached FA status at least
once up to that month, on which they still retained such FA status.
Goal: Track longitudinal changes in FA status of pages
"""
## TODO: Beware of padding 23:59:59 to up_date
del_fa_ts_month = """DROP TABLE IF EXISTS fa_ts_{month!s}"""
fa_ts_month = """CREATE TABLE fa_ts_{month!s} AS
                 (SELECT rev_id, rev_page,
                 MIN(rev_timestamp) min_ts_fa,
                 MAX(rev_timestamp) max_ts_fa
                 FROM revision WHERE rev_fa = 1 AND
                 rev_timestamp <= {up_date!s}
                 GROUP BY rev_page)"""
"""
Table name: max_ts_nofa_{month!s}
Content: Max rev_timestamp of pages who reached FA status at least
once up to that month, on which they did not have such FA status.
Goal: Track longitudinal changes in FA status of pages
"""
## TODO: Beware of padding 23:59:59 to up_date
del_nofa_maxts_month = """DROP TABLE IF EXISTS max_ts_nofa_{month!s}"""
nofa_maxts_month = """CREATE TABLE max_ts_nofa_{month!s} AS (
                      SELECT rev_id, a.rev_page,
                      MAX(rev_timestamp) max_ts_nofa
                      FROM revision a JOIN
                          (SELECT rev_page FROM fa_ts_{month!s}) b
                      ON a.rev_page = b.rev_page
                      WHERE rev_fa = 0 AND rev_timestamp <= {up_date!s}
                      GROUP BY rev_page )"""

"""
***********
** USERS **
***********
"""
"""
Table name: revision_{month!s}
Content: All revisions in a specific month
Goal: Partition whole revision history for monthly metrics
"""
del_rev_month = "DROP TABLE IF EXISTS revision_{month!s}"
rev_month = """CREATE TABLE revision_{month!s} AS
               (SELECT rev_id, rev_page, rev_user, rev_timestamp
               FROM revision
               WHERE rev_user > 0 AND
               rev_timestamp >= '{low_date!s} 00:00:00' AND
               rev_timestamp <= '{up_date!s} 23:59:59')"""

"""
Table name: revision_{month!s}_nobots
Content: All revisions in a specific month by human editors (discarding bots)
Goal: Partition whole revision history for monthly metrics
"""
del_rev_month_nobots = "DROP TABLE IF EXISTS revision_{month!s}_nobots"
rev_month_nobots = """CREATE TABLE revision_{month!s}_nobots AS
                      (SELECT rev_id, rev_page, rev_user, rev_timestamp
                      FROM revision_{month!s} a LEFT JOIN
                          (SELECT ug_user FROM user_groups
                          WHERE ug_group = 'bot') b
                          ON rev_user = ug_user
                          WHERE ug_user is NULL)"""

"""
******************************************
*** QUERIES FOR BASIC ACTIVITY METRICS ***
******************************************
"""
"""
***********
** PAGES **
***********
"""
## TODO: Beware of padding 23:59:59 to up_date
"""
Name: month_total_length
Goal: Cumulative length of articles (in bytes) up to a certain month
(all users)
"""
month_total_length = """SELECT SUM(rev_len)
                        FROM page_min_articles
                        WHERE rev_timestamp <= %(up_date)s
                        AND rev_is_redirect = 0"""

"""
Name: articles_till_month
Goal: Cumulative number of articles up to a certain month (main namespace)
"""
articles_till_month = """SELECT COUNT(*) FROM page_min_articles
                         WHERE rev_timestamp <= %(up_date)s AND
                         rev_is_redirect = 0"""

"""
Name: month_new_articles
Goal: New articles created within a certain month (main namespace)
"""
month_new_articles = """SELECT COUNT(*) FROM page_min_articles
                        WHERE rev_timestamp >= %(low_date)s AND
                        rev_timestamp <= %(up_date)s AND
                        rev_is_redirect = 0"""

"""
Name: redirects_till_month
Goal: Cumulative number of redirects up to a certain month (main namespace)
"""
redirects_till_month = """SELECT COUNT(*) FROM page_min_articles
                          WHERE rev_is_redirect = 1 AND
                          rev_timestamp <= %(up_date)s"""

"""
Name: redirects_till_month
Goal: Cumulative number of redirects up to a certain month (main namespace)
"""
month_new_redirects = """SELECT COUNT(*) FROM page_min_articles
                        WHERE rev_timestamp >= %(low_date)s AND
                        rev_timestamp <= %(up_date)s AND
                        rev_is_redirect = 1"""

"""
For rationale to understand the following two queries see above section on
TABLES FOR DATA PREPARATION corresponding to FEATURED ARTICLES (FAs)
"""

"""
Name: month_fa
Goal: Count all pages who retained FA status up to this month
"""
month_fa = """SELECT COUNT(*) FROM fa_ts_{month!s} a JOIN
              max_ts_nofa_{month!s} b
              ON a.rev_page = b.rev_page
              WHERE max_ts_nofa < max_ts_fa"""

"""
Name: month_new_fa
Goal: Count all pages who did not retain FA status up to this month
"""
## TODO: Beware of padding 00:00:00 to low_date
month_new_fa = """SELECT COUNT(*) FROM fa_ts_{month!s}
                  WHERE min_ts_fa >= {low_date!s}"""

"""
***********
** USERS **
***********
"""
"""
Name: month_users
Goal: Editors with at least 1 edit in a certain month
"""
month_users = """SELECT COUNT(DISTINCT(rev_user))
                 FROM revision_{month!s}_nobots"""

"""
Name: month_active_users
Goal: Editors with at least 5 edits in a certain month
"""
month_active_users = """SELECT COUNT(*) FROM
                        (SELECT rev_user, COUNT(*) AS nrevs FROM
                        revision_{month!s}_nobots
                        GROUP BY rev_user
                        HAVING nrevs > 5) x"""

"""
Name: month_very_active_users
Goal: Editors with at least 5 edits in a certain month
"""
month_very_active_users = """SELECT COUNT(*) FROM
                             (SELECT rev_user, COUNT(*) AS nrevs FROM
                             revision_{month!s}_nobots
                             GROUP BY rev_user
                             HAVING nrevs > 100) x"""

"""
Name: month_sysops
Goal: Number of administrators with at least 1 edit in a certain month
"""
month_sysops = """SELECT COUNT(DISTINCT(rev_user)) FROM
                  revision_{month!s}_nobots a JOIN
                  (SELECT ug_user FROM user_groups
                  WHERE ug_group = 'sysop') b
                  ON rev_user = ug_user """

## TODO: Beware of padding 00:00:00 to low_date and 23:59:59 to up_date
"""
Name: month_edits
Goal: Total number of edits in a certain month (all users)
"""
month_edits = """SELECT COUNT(*) FROM revision
                 WHERE rev_timestamp >= %(low_date)s AND
                 rev_timestamp <= %(up_date)s"""

"""
Name: month_edits_reg
Goal: Total number of edits in a certain month (registered users)
"""
month_edits_reg = """SELECT COUNT(*) FROM revision_{month!s}"""

"""
Name: month_edits_nobots
Goal: Total number of edits in a certain month (registered users w/o bots)
"""
month_edits_nobots = """SELECT COUNT(*) FROM revision_{month!s}_nobots"""
