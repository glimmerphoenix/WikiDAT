-- user_min_ts --
-- Timestamp of first revision for every user
CREATE TABLE user_min_ts AS SELECT rev_user, rev_id, rev_page,
MIN(rev_timestamp) rev_timestamp FROM revision GROUP BY rev_user;