-- Top user each day by comments 
WITH daily_user_total AS (
    SELECT 
        DATE(created_at) AS event_date
        user, 
        SUM(comments) AS user_daily_comments
    FROM hn_posts
    GROUP BY DATE(created_at), user 
)
SELECT
    event_date,
    user, 
    user_daily_comments, 
    RANK() OVER (PARTITION BY event_date ORDER BY user_daily_comments DESC) AS daily_rank
FROM daily_user_total
WHERE daily_rank = 1
ORDER BY event_date;

-- Users active on 5+ consecutive days 
WITH user_daily_comments AS (
    SELECT
        user, 
        DATE(created_at) AS event_date, 
        COUNT(*) AS posts_that_day
    FROM hn_posts
    GROUP BY user, DATE(created_at)
), 
consecutive_days AS (
    SELECT  
        user, 
        event_date, 
        posts_that_day, 
        ROW_NUMBER() OVER (PARTITION BY user ORDER BY event_date) - ROW_NUMBER() OVER (ORDER BY event_date) AS grp 
        FROM user_daily_activity
)
SELECT 
    user, 
    COUNT(*) AS consecutive_active_days
FROM consecutive_days
GROUP BY user, grp 
HAVING count(*) >= 5
GROUP BY user 
ORDER BY consecutive_active_days DESC
LIMIT 10; 

-- Users with biggest comment growth week-over-week 
WITH weekly_user_totals AS (
    SELECT 
        user, 
        strftime('%Y-%W', created_at) AS week, 
        SUM(comments) AS weekly_comments
    FROM hn_posts
    GROUP BY user, strftime('%Y-%W', created_at)
)
SELECT 
    w1.user, 
    w1.week AS week1, 
    w1.weekly_comments AS week1_comments, 
    w2.week AS week2, 
    w2.weekly_comments AS week2_comments, 
    w2.weekly_comments - w1.weekly_comments AS growth 
FROM weekly_user_totals w1
JOIN weekly_user_totals w2 ON w1.user = w2.user 
    AND w2.week = (SELECT MIN(week) FROM weekly_user_totals WHERE week > w1.week)
ORDER BY growth DESC 
LIMIT 10; 