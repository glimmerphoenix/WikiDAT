library(RMySQL)
library(RColorBrewer)
library(zoo)

langs = c("enwiki", "dewiki","frwiki","eswiki","ruwiki", "nlwiki")
cols = brewer.pal(6, 'Set2')
dbs = c("enwiki_052012", "dewiki_042012", "frwiki_042012", "eswiki_052012", 
        "ruwiki_052012", "nlwiki_042012")

new_editors = list(enwiki = numeric(0), dewiki = numeric(0), frwiki = numeric(0), 
                   eswiki = numeric(0), ruwiki = numeric(0), 
                   nlwiki = numeric(0) )

for (i in seq(1,6) ) { 
    con = dbConnect(MySQL(), user = "jfelipe", password = "xxxx",
                    dbname = dbs[i])

    new_ed = dbGetQuery(con, "SELECT YEARWEEK(rev_timestamp, 5) time, 
                        COUNT(*) as new_editors 
                        FROM (SELECT a.* FROM user_min_ts a LEFT JOIN 
                        (SELECT ug_user from user_groups where ug_group = 'bot') b
                        ON a.rev_user = b.ug_user
                        WHERE b.ug_user is NULL) x
                        WHERE YEAR(rev_timestamp) >= 2004 AND
                        rev_timestamp < '2012-04-01'
                        GROUP BY time 
                        ORDER BY time")
    dbDisconnect(con)
    # We must specify a day of week to get the date, otherwise the 
    # string that we try to convert is ambiguous. %u is weekday as decimal
    # number, Monday = 1.
    new_editors_ts = zoo(new_ed$new_editors, 
                    as.POSIXct(paste(new_ed$time, '1'), format = "%Y%W %u"))

    new_editors[[i]] = new_editors_ts
}

all_new_editors = merge(new_editors$enwiki, new_editors$dewiki, new_editors$frwiki, 
                        new_editors$eswiki, new_editors$ruwiki, 
                        new_editors$nlwiki)

# Compute moving average i a 12-week period
mean_new_editors = rollapply(all_new_editors, 12, function(x) mean(x))

pdf("figs/ts_new_editors.pdf", width = 11, height = 7)

plot(all_new_editors, screens = 1,
    main = "Evolution of new editors (first revision) in 6 Wikipedias",
    col = cols )

legend("topleft", legend = langs, col = cols, lty = 1)

dev.off()

pdf("figs/ts_new_editors_screens.pdf", width = 13, height = 10)

plot(merge(all_new_editors, mean_new_editors), screens = rep(seq(1,6), 2), 
    ylab = paste("New editors", langs), 
    main = "Evolution of new editors (1st revision) in 6 Wikipedias",
    col = c(cols, rep('black', 6)), lty = rep(c(1,2), each = 6), 
    lwd = rep(c(1,1.8), each = 6) )

dev.off()

