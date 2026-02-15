-- Total comments per day
SELECT 
    DATE(created_at) AS event_date, 
    SUM(comments) AS total_comments
FROM hn_posts
GROUP BY DATE(created_at)
ORDER BY event_date; 

-- Rank days by total comments 
WITH daily_comments AS (
    SELECT 
        DATE(created_at) AS event_date, 
        SUM(comments) AS total_comments 
    FROM hn_posts
    GROUP BY DATE(created_at)
)
SELECT 
    event_date,
    total_comments, 
    RANK() OVER (ORDER BY total_comments DESC) AS comments_rank 
FROM daily_comments 
ORDER BY comments_rank 
LIMIT 10; 

-- Day-over-day growth in comments 
WITH daily_comments AS (
    SELECT 
        DATE(created_at) AS event_date, 
        SUM(comments) AS total_comments 
    FROM hn_posts
    GROUP BY DATE(created_at)
)
SELECT 
    event_date, 
    total_comments, 
    LAG(total_comments) OVER (ORDER BY event_date) AS prev_day_comments, 
    total_comments - LAG(total_comments) OVER (ORDER BY event_date) AS comment_growth 
FROM daily_comments
ORDER BY event_date; 