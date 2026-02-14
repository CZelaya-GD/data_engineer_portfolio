-- 1) Subquery: Top 10 users by comments
SELECT user, total_comments
FROM (   --  <- Subquery starts (unnamed temp table)
    SELECT
        user,      -- Group by user (your Day 8 Logic)
        SUM(comments) AS total_comments     -- Aggregate like pandas .sum()
    FROM hn_posts
    GROUP BY user    -- pandas.groupby('user')
) AS t        -- Alias "t" for outer query reference
ORDER BY total_comments DESC 
LIMIT 10; 


-- 2) Single CTE: reuse top_users in later filters 
WITH top_users_by_comments AS (      -- <- CTE definition: name + query
    SELECT 
        user, 
        SUM(comments) AS total_comments
    FROM hn_posts
    GROUP BY user
)     --  <- CTE ends, now reusable
SELECT *      --  Reuse like any table
FROM top_users_by_comments
WHERE total_comments > 100    -- Filter the CTE
ORDER BY total_comments DESC; 


-- 3) Multi-CTE: top_users -> their avg score 
WITH top_users_by_comments AS (       -- CTE 1: your Day 8 top users
    SELECT 
        user, 
        SUM(comments) AS total_comments
    FROM hn_posts 
    GROUP BY user 
), 
user_scores AS (
    SELECT
        user, 
        AVG(score) AS avg_score
    FROM hn_posts 
    GROUP BY user
)
SELECT 
    u.user,
    u.total_comments,
    s.avg_score
FROM top_users u 
JOIN user_scores s USING (user)
ORDER BY u.total_comments DESC 
LIMIT 10; 