-- hnanalysis.sql
-- Purpose: Production-ready Hacker News analytics dashboard
-- Inputs: hn_posts table (id, title, user, score, comments, created_at)
-- Output: metric | event_date | user | value
-- Usage: Point BI tool / Python script at this query for HN KPIs

WITH top_posts AS (
    -- Top posts by score + comments (foundation)
    SELECT *
    FROM hn_posts
    WHERE score > 50 
        AND comments > 10
), 

user_daily AS (
    -- user activity per day
    SELECT 
        DATE(created_at) AS event_date, 
        user, 
        COUNT(*) AS posts, 
        SUM(comments) AS total_comments
    FROM hn_posts 
    GROUP BY DATE(created_at), user
), 

daily_leaders AS (
    -- Top user per day
    SELECT 
        event_date, 
        user, 
        total_comments, 
        RANK() OVER (
            PARTITION BY event_date 
            ORDER BY total_comments DESC
            ) AS daily_rank
    FROM user_daily
), 

consistent_users AS (
    -- 5+ consecutive days (ROW_NUMBER magic)
    SELECT 
        user, 
        COUNT(*) AS streak_days
    FROM(
        SELECT 
            user, 
            event_date, 
            ROW_NUMBER() OVER (
                PARTITION BY user 
                ORDER BY event_date
                )
            - ROW_NUMBER() OVER (ORDER BY event_date) AS grp 
        FROM user_daily
    ) gaps 
    GROUP BY user, grp
    HAVING COUNT(*) >= 5
)

-- 🎯 PRODUCTION DASHBOARD: 3 HN KPIs
SELECT 
    'DAILY_LEADER' AS metric, 
    event_date, 
    user, 
    total_comments
FROM daily_leaders 
WHERE daily_rank = 1 

UNION ALL 

SELECT 
    'CONSISTENT_USER' AS metric, 
    NULL AS event_date, 
    user, 
    streak_days AS total_comments
FROM consistent_users

UNION ALL 

SELECT 
    'TOP_POSTS_30D' AS metric, 
    DATE(created_at) AS event_date, 
    user, 
    (score + comments) AS value
FROM top_posts
WHERE created_at >= DATE('now', '-30 days')
ORDER BY metric,
event_date DESC, 
Value DESC; 