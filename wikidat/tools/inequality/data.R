# Scripts to retrieve example data for the workshop.
library(RMySQL)

langs_pages = c("eswiki", "dewiki", "enwiki")
langs_users = c("frwiki", "nlwiki", "ruwiki")

pages = data.frame("lang" = character(0),
                   "page_id" = numeric(0), 
                   "page_title" = character(0),
                   "page_ns" = numeric(0))

revisions = data.frame ("lang" = character(0),
                        "rev_id" = numeric(0),
                        "rev_page" = numeric(0),
                        "rev_user" = numeric(0), 
                        "rev_timestamp" = character(0),
                        "rev_parent_id" = numeric(0), 
                        "rev_is_minor" = numeric(0),
                        "rev_is_redirect" = numeric(0),
                        "rev_fa" = numeric(0))

users = data.frame ("lang" = character(0),
                    "rev_id" = numeric(0), 
                    "rev_page" = numeric(0),
                    "rev_user" = numeric(0), 
                    "rev_timestamp" = character(0),
                    "rev_parent_id" = numeric(0), 
                    "rev_is_minor" = numeric(0),
                    "rev_is_redirect" = numeric(0),
                    "rev_fa" = numeric(0),
                    "group" = character(0))

for (lang in langs_pages) {
    if (lang == "dewiki") {
      db = paste(lang, "_042012", sep = "")
    }
    else {
      db = paste(lang, "_052012", sep = "")
    }
    con = dbConnect(MySQL(), user = "root", password = "phoenix", dbname = db)
    
    page_list = dbGetQuery(con, paste("SELECT DISTINCT(page_id) as page_id",
                                      "FROM page"))
    # Take random sample 500 pages
    sampling = sample(page_list$page_id, 50) # replace = FALSE by default
    
    count_pages = 0
    
    for (page_id in sampling) {
        page_data = dbGetQuery(con, paste("SELECT '", lang,
                                          "' as lang, page_id, page_title,",
                                          "page_namespace FROM page",
                                          "WHERE page_id =", page_id))
        revision_data = dbGetQuery(con, paste("SELECT '", lang,
                                              "' as lang, rev_id, rev_page,",
                                              "rev_user, rev_timestamp,",
                                              "rev_parent_id, rev_minor_edit",
                                              "rev_is_redirect, rev_fa",
                                              "FROM revision",
                                              "WHERE rev_page =", page_id))
        pages = rbind(pages, page_data)
        revisions = rbind(revisions, revision_data)
        sink('pages.log')
        print(paste("page ", count_pages, "of 50 in", lang))
        sink()
        count_pages = count_pages + 1
    }
    dbDisconnect(con)
}

# Transform dates as the correct data type
revisions = transform(revisions, rev_timestamp = as.Date(rev_timestamp))

save(pages, file = "pages.RData")
save(revisions, file = "revisions.RData")

rm(pages)
rm(revisions)

for (lang in langs_users) {
    if ((lang == "frwiki") | (lang == "nlwiki")) {
        db = paste(lang, "_042012", sep = "")
    }
    else {
        db = paste(lang, "_052012", sep = "")
    }
    con = dbConnect(MySQL(), user = "root", password = "phoenix", dbname = db)
    # Get list of registered users
    user_list = dbGetQuery(con, paste("SELECT DISTINCT(rev_user) as rev_user",
                                      "FROM revision WHERE rev_user > 0"))
    # Take random sample 500 registered users
    sampling = sample(user_list$rev_user, 50) # replace = FALSE by default

    count_users = 0
    
    for (rev_user in sampling) {
        revision_data = dbGetQuery(con, paste("SELECT '", lang,
                                              "' as lang, rev_id, rev_page,",
                                              "rev_user, rev_timestamp,",
                                              "rev_parent_id, rev_minor_edit",
                                              "rev_is_redirect, rev_fa",
                                              "FROM revision",
                                              "WHERE rev_user =", rev_user))
        privilege = dbGetQuery(con, paste("SELECT ug_group FROM user_groups",
                                          "WHERE ug_user =", rev_user))
        if (length(privilege$ug_group) > 0) {
          revision_data = transform(revision_data, group = privilege$ug_group)
        }
        else {
          revision_data = transform(revision_data, group = "registered")
        }
        
        users = rbind(users, revision_data)
        
        sink('users.log')
        print(paste("user ", count_users, "of 50 in", lang))
        sink()
        count_users = count_users + 1
    }

    dbDisconnect(con)
}

# Transform dates as the correct data type
users = transform(users, rev_timestamp = as.Date(rev_timestamp))

save(users, file = "users.RData")
rm(users)