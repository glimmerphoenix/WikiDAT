# Before running this script, you must parse the pages-logging dump for
# simplewiki and configure the database name below

library(RMySQL)

db_name = "simplewiki_062012"

con = dbConnect(MySQL(), user = "wiki", password = "mypass",
                dbname = db_name)

blocks = dbGetQuery(con, "select YEAR(log_timestamp) year, 
                    MONTH(log_timestamp) month, COUNT(*) as nblock 
                    FROM logging
                    WHERE log_type = 'block'
                    GROUP BY year, month 
                    ORDER BY year, month")

pdf("figs/ts_blocks.pdf", width = 11, height = 7)
blocks_ts = ts(blocks$nblock, frequency = 12, 
               start = c(blocks$year[1], blocks$month[1]))

plot(blocks_ts, main = "Evolution of block actions in simplewiki")
dev.off()

protects = dbGetQuery(con, "select YEAR(log_timestamp) year, 
                    MONTH(log_timestamp) month, COUNT(*) as nblock 
                    FROM logging
                    WHERE log_type = 'protect'
                    GROUP BY year, month 
                    ORDER BY year, month")
dbDisconnect(con)

pdf("figs/ts_protections.pdf", width = 11, height = 7)
protects_ts = ts(protects$nblock, frequency = 12, 
               start = c(protects$year[1], protects$month[1]))

plot(protects_ts, main = "Evolution of protect actions in simplewiki")
dev.off()

pdf("figs/stl_blocks.pdf", width = 11, height = 7)
plot(stl(blocks_ts, s.window = 3))
dev.off()

pdf("figs/stl_protections.pdf", width = 11, height = 7)
plot(stl(protects_ts, s.window = 3))
dev.off()