import MySQLdb, warnings, codecs, time

if __name__ == '__main__':
    
    langs = ('fiwiki', 'cswiki', 'idwiki', 'thwiki', 'arwiki', 'kowiki', 
            'hewiki', 'nowiki', 'huwiki', 'viwiki', 'ukwiki', 'dawiki', 
            'fawiki', 'rowiki', 'cawiki', 'bgwiki', 'hrwiki', 'elwiki', 
            'skwiki', 'srwiki', 'ltwiki', 'slwiki', 'etwiki', 'mswiki',
            'dewiki', 'eswiki', 'frwiki', 'itwiki', 'jawiki', 'nlwiki', 
            'plwiki', 'ptwiki', 'ruwiki', 'zhwiki', 'svwiki', 'trwiki',
            'enwiki','euwiki', 'glwiki',)
    
    ##### FIXME: Pending due to bad FA pattern!!!
    #langs = ('trwiki')
    
    def send_query(cursor, query, ntimes, log_file):
        """
        Send query to DB. Attempt 'ntimes' consecutive times before giving up
        cursor: cursor to open DB connection
        query: query to be sent to DB
        ntimes: number of sending attempts
        log_file: name of file to log errors
        """
        # TODO: Handle errors properly with logger library
        #chances = 0
        #while chances < ntimes:
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            warnings.simplefilter('ignore', MySQLdb.Warning)
            try:
                nres = cursor.execute(query)
                results = cursor.fetchall()
                if nres == 0:
                    return ((0,),)
                else:
                    return results
            except (Exception), e:
                f = codecs.open(log_file,'a','utf-8')
                f.write(str(e)+"\n")
                f.write("".join([query[0:79],
                            "\n******************************"]))
                f.close()
                #chances += 1
            else:
                pass
                
    data_082011 = open("data_082011_FAs.csv", 'w')
    #data_082011.write("lang,very_active_wikipedians,active_wikipedians," +\
            #"other_contributors,admins,articles,redirects,edits_per_month," +\
            #"new_articles,fa_articles,new_fa_articles" + "\n")
    data_082011.write('lang,fa_articles,new_fa_articles' + "\n")
    
    for lang in langs:
        # Get DB connection
        if (lang == 'dewiki' or lang == 'nlwiki' or lang == 'frwiki'):
            db_name = lang + '_042012'
        else:
            db_name = lang + '_052012'
            
        log_file = 'aug_2011_' + db_name + '_FAs.log'
        
        f = codecs.open(log_file,'a','utf-8')
        f.write("starting language " + lang + " " +\
        time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime()) + "\n")
        f.close()
        
        data_line = lang
        
        conn = MySQLdb.Connect (host = 'localhost', port = 3306,
                                user = 'root', 
                                passwd='phoenix',db = db_name,
                                charset="utf8", use_unicode=True)
        conn.autocommit(True)
        cursor = conn.cursor()
        
        drop_aug_2011 = "DROP TABLE IF EXISTS revision_082011"
        query_aug_2011 = "CREATE TABLE revision_082011 AS " +\
                        "( SELECT rev_id, rev_page, rev_user, rev_timestamp " +\
                        "FROM revision " +\
                        "WHERE rev_user > 0 AND " +\
                        "rev_timestamp >= '2011-08-01 00:00:00' AND " +\
                        "rev_timestamp <= '2011-08-31 23:59:59' )"
        
        drop_aug_2011_nobots = "DROP TABLE IF EXISTS revision_082011_nobots"
        query_aug_2011_nobots = "CREATE TABLE revision_082011_nobots AS " +\
                        "( SELECT rev_id, rev_page, rev_user, rev_timestamp " +\
                        "FROM revision_082011 a LEFT JOIN " +\
                        "( SELECT ug_user FROM user_groups " +\
                        "WHERE ug_group = 'bot') b " +\
                        "ON rev_user = ug_user " +\
                        "WHERE ug_user is NULL )"
        
        # Create table revision_082011 and revision_082011_nobots
        send_query(cursor, drop_aug_2011, 5, log_file)  
        send_query(cursor, query_aug_2011, 5, log_file)
        send_query(cursor, drop_aug_2011_nobots, 5, log_file)  
        send_query(cursor, query_aug_2011_nobots, 5, log_file)
        
        # Registered users in 08-2011
        query_wikipedians = "SELECT COUNT(DISTINCT(rev_user)) " +\
                            "FROM revision_082011"
        editors = send_query(cursor, query_wikipedians, 5, log_file)
        
        # Active wikipedians 08-2011
        query_active_wikipedians = "SELECT count(*) FROM " +\
                                "(SELECT rev_user, COUNT(*) AS nrevs FROM " +\
                                "revision_082011_nobots " +\
                                "GROUP BY rev_user "+\
                                "HAVING nrevs > 5) x "
        active_wikipedians = send_query(cursor, query_active_wikipedians, 5, log_file)
        
        # Very active wikipedians 08-2011
        query_very_active_wikipedians = "SELECT count(*) FROM " +\
                                "(SELECT rev_user, COUNT(*) AS nrevs FROM " +\
                                "revision_082011_nobots " +\
                                "GROUP BY rev_user "+\
                                "HAVING nrevs > 100) x "
        very_active_wikipedians = send_query(cursor, query_very_active_wikipedians, 5, log_file)
        
        other_contributors = editors[0][0] - active_wikipedians[0][0]
        
        # Sysops 08-2011
        query_sysops = "SELECT COUNT(DISTINCT(rev_user)) FROM " +\
                        "revision_082011_nobots a JOIN " +\
                        "( SELECT ug_user FROM user_groups " +\
                        "WHERE ug_group = 'sysop') b " +\
                        "ON rev_user = ug_user "
        sysops = send_query(cursor, query_sysops, 5, log_file)
        
        data_line = "".join([data_line, ",", str(very_active_wikipedians[0][0]),
        ",", str(active_wikipedians[0][0]), ",", str(other_contributors), ",",
        str(sysops[0][0])])
                        
        # Total edits (all users) in 08-2011
        query_edits = "SELECT COUNT(*) FROM revision " +\
                        "WHERE rev_timestamp >= '2011-08-01 00:00:00' AND " +\
                        "rev_timestamp <= '2011-08-31 23:59:59'"
        edits = send_query(cursor, query_edits, 5, log_file)
        # Total edits registered users in 08-2011
        query_edits_reg = "SELECT COUNT(*) FROM revision_082011"
        edits_reg = send_query(cursor, query_edits_reg, 5, log_file)
        # Total edits registered users w/o bots 08-2011
        query_edits_nobots = "SELECT COUNT(*) FROM revision_082011_nobots"
        edits_nobots = send_query(cursor, query_edits_nobots, 5, log_file)
        
        # First edit per page (all history)
        drop_page_min_ts = "DROP TABLE IF EXISTS page_min_ts"
        query_page_min_ts = "CREATE TABLE page_min_ts AS " +\
                        "( SELECT rev_id, rev_page, rev_user, " +\
                        "MIN(rev_timestamp) rev_timestamp , rev_is_redirect "+\
                        "FROM revision GROUP BY rev_page )"
        send_query(cursor, drop_page_min_ts, 5, log_file)
        send_query(cursor, query_page_min_ts, 5, log_file)
        
        # Number of articles till 08-2011
        query_page_till_082011 = "SELECT COUNT(*) FROM page_min_ts " +\
                        "WHERE rev_timestamp <= '2011-08-31 23:59:59' AND " +\
                        "rev_is_redirect = 0 "
        articles = send_query(cursor, query_page_till_082011, 5, log_file)
        # New articles in 08-2011
        query_new_page_082011 = "SELECT COUNT(*) FROM page_min_ts " +\
                        "WHERE rev_timestamp >= '2011-08-01 00:00:00' AND " +\
                        "rev_timestamp <= '2011-08-31 23:59:59' AND " +\
                        "rev_is_redirect = 0"
        new_articles = send_query(cursor, query_new_page_082011, 5, log_file)
        # Number of redirects till 08-2011
        query_page_redir_till_082011 = "SELECT COUNT(*) FROM page_min_ts " +\
                        "WHERE rev_is_redirect = 1 AND "+\
                        "rev_timestamp <= '2011-08-31 23:59:59' "
        redirects = send_query(cursor, query_page_redir_till_082011, 5, log_file)
        
        # Min ts in FAs
        drop_fa_ts_082011 = "DROP TABLE IF EXISTS fa_ts_082011"
        query_fa_ts_082011 = "CREATE TABLE fa_ts_082011 AS " +\
                        "( SELECT rev_id, rev_page, " +\
                        "MIN(rev_timestamp) min_ts_fa,  " +\
                        "MAX(rev_timestamp) max_ts_fa  " +\
                        "FROM revision WHERE rev_fa = 1 AND " +\
                        "rev_timestamp <= '2011-08-31 23:59:59' " +\
                        "GROUP BY rev_page )"
        send_query(cursor, drop_fa_ts_082011, 5, log_file)
        send_query(cursor, query_fa_ts_082011, 5, log_file)
        
        # FAs till 08-2011
        # Get list of all FAs in lang
        query_ids_fa = "SELECT DISTINCT(rev_page) from fa_ts_082011 "
        ids_fa = send_query(cursor, query_ids_fa, 5, log_file)
        # Retrieve max_ts where articles does not have FA tag
        # Insert in 4th column of fa_ts_082011
        query_max_ts_nofa_082011 = "CREATE TABLE max_ts_nofa_082011 AS ( " +\
                "SELECT rev_id, a.rev_page, MAX(rev_timestamp) max_ts_nofa " +\
                "FROM revision a JOIN (SELECT rev_page FROM fa_ts_082011) b " +\
                "ON a.rev_page = b.rev_page " +\
                "WHERE rev_fa = 0 AND rev_timestamp <= '2011-08-31 23:59:59' " +\
                "GROUP BY rev_page )"
        send_query(cursor, query_max_ts_nofa_082011, 5, log_file)
        
        query_fa_till_082011 = "SELECT COUNT(*) " +\
                "FROM fa_ts_082011 a JOIN max_ts_nofa_082011 b " +\
                "ON a.rev_page = b.rev_page " +\
                "WHERE max_ts_nofa < max_ts_fa "

        fa_articles = send_query(cursor, query_fa_till_082011, 5, log_file)
        
        # New FAs in 08-2011
        query_fa_08_2011 = "SELECT COUNT(*) FROM fa_ts_082011 " +\
                        "WHERE min_ts_fa >= '2011-08-01 00:00:00' "
        new_fa_articles = send_query(cursor, query_fa_08_2011, 5, log_file)
        
        data_line = "".join([data_line, ",", str(articles[0][0]), ",",
        str(redirects[0][0]), ",", str(edits[0][0]), ",", 
        str(new_articles[0][0]), ",",
        str(fa_articles[0][0]), ",", str(new_fa_articles[0][0])])
        
        data_082011.write(data_line + "\n")
        
        # Close DB connection
        conn.close()
        
        f = codecs.open(log_file,'a','utf-8')
        f.write("finished language " + lang + " " +\
        time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime()) + "\n")
        f.close()
    
    # Close data file
    data_082011.close()
    