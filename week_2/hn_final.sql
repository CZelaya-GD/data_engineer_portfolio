-- Day 13: COMPLETE HN ANALYZER (All patterns -> single file)

/*
Days 8-12 -> hnanalysis.sql deliverable 
Top posts + users + time + growth -> Production Dashboard 
*/

WITH top_posts AS (
    -- Day 8-10: Top posts by score + comments (foundation)
    SELECT *
    FROM hn_posts
    WHERE score > 50 AND comments > 10
), 

user_daily AS (
    -- Day 12: user activity per day
    SELECT 
        DATE(created_at) AS event_date, 
        user, 
        COUNT(*) AS posts, 
        SUM(comments) AS total_comments
    FROM hn_posts 
    GROUP BY DATE(created_at), user
), 

daily_leaders AS (
    -- Day 12 Block 1 : Top user per day
    SELECT 
        event_date, 
        user, 
        total_comments, 
        RANK() OVER (PARTITION BY event_date ORDER BY total_comments DESC) AS daily_rank
    FROM user_daily
), 

consistent_users AS (
    -- Day 12 Block 2: 5+ consecutive days (ROW_NUMBER magic)
    SELECT 
        user, 
        COUNT(*) AS streak_days
    FROM(
        SELECT 
            user, 
            event_date, 
            ROW_NUMBER() OVER (PARTITION BY user ORDER BY event_date) - 
            ROW_NUMBER() OVER (ORDER BY event_date) AS grp 
        FROM user_daily
    ) gaps 
    GROUP BY user, grp
    HAVING COUNT(*) >= 5
    GROUP BY user
    HAVING COUNT(*) > 0 
)

-- FINALE: Complete HN dashboard
SELECT 
    'DAILY LEADERS' AS metric, 
    event_date, 
    user, 
    total_comments
FROM daily_leaders 
WHERE daily_rank = 1 

UNION ALL 

SELECT 
    'CONSISTENT USERS' AS metric, 
    'N/A' AS event_date, 
    user, 
    streak_days AS total_comments
FROM consistent_users

UNION ALL 

SELECT 
    'TOP POSTS (PAST 30 DAYS)' AS metric, 
    DATE(created_at) AS event_date, 
    user, 
    score + comments AS engagement_score 
FROM hn_posts 
WHERE created_at >= DATE('now', '-30 days')
ORDER BY engagement_score DESC 
LIMIT 10; 