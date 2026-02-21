"""
Day 17: HN Dashboard SQL Queries. 

Purpose: Clean, readable queries for week_3/etl/serve_hn.py
Inputs: hn_posts.db (Day 15 ETL output)
Outputs: JSON-ready DataFrames for 4 API enpoints
"""
# ============================================================================
# 1. DAILY LEADERS (enhanced readability)
# ============================================================================

DAILY_LEADERS = """
-- Top user per day by comment volume 

WITH daily_totals AS (
    SELECT
        DATE(created_at) as event_date, 
        user, 
        COUNT(*) as total_comments
    FROM hn_posts
    WHERE user IS NOT NULL 
    GROUP BY event_date, user
    ), 
daily_max AS (
    SELECT 
        event_date, 
        MAX(total_comments) as max_comments_per_day
    FROM daily_totals
    GROUP BY event_date
)
SELECT 
    'DAILY_LEADER' as metric_type,
    daily_totals.event_date,
    daily_totals.user, 
    daily_totals.total_comments
FROM daily_totals
JOIN daily_max ON daily_totals.event_date = daily_max.event_date
    AND daily_totals.total_comments = daily_max.max_comments_per_day

ORDER BY daily_totals.event_date DESC 
LIMIT 20
"""

# ============================================================================
# 2. TOP USERS - Most active users (last 7 days)
# ============================================================================

TOP_USERS_LAST_7D = """
-- Most active users by comments and engagement (past week)
SELECT 
    user, 
    COUNT(*) as total_comments, 
    ROUND(AVG(score), 2) as average_score,
    COUNT(DISTINCT DATE(created_at)) as active_days

FROM hn_posts
WHERE created_at >= date('now', '-7 days')
    AND user IS NOT NULL
GROUP BY user 
HAVING total_comments >= 2
ORDER BY total_comments DESC, average_score DESC
LIMIT 10
"""

# ============================================================================
# 3. TRENDING TOPICS - Most discussed titles (last 7 days)
# ============================================================================

TRENDING_TITLES_LAST_7D = """
-- Most frequently discussed topics (duplicate titles = trending)
WITH title_frequencies AS (
    SELECT 
        LOWER(title) as normalized_title, 
        COUNT(*) as mention_count, 
        COUNT(DISTINCT id) as unique_discussions
    
    FROM hn_posts 
    WHERE created_at >= date('now', '-7 days')
        AND title IS NOT NULL
        AND LENGTH(title > 15)
        AND title NOT LIKE '%hacker news%'
    GROUP BY LOWER(title)
    HAVING mention_count >=2
)
SELECT 
    normalized_title, 
    mention_count, 
    unique_discussions
FROM title_frequencies
ORDER BY mention_count DESC, unique_discussions DESC
LIMIT 8
"""

# ============================================================================
# 4. RECENT ACTIVITY - 24hr pipeline health summary
# ============================================================================

ACTIVITY_LAST_24H = """
-- Pipeline health metrics for last 24 hours
SELECT 
    COUNT(*) as total_issues,
    COUNT(DISTINCT user) as distinct_users, 
    ROUND(AVG(comments), 1) as average_comments, 
    ROUND(AVG(score), 2) as average_score,
    MIN(created_at) as earliest_activity, 
    MAX(created_at) as latest_activity, 
    COUNT(DISTINCT DATE(created_at)) as active_days

FROM hn_posts 
WHERE created_at >= date('now', '-1 day')
"""

# ============================================================================
# 5. WEEK OVER WEEK - Growth trends 
# ============================================================================

WEEK_OVER_WEEK_GROWTH = """
-- Week-over-week activity growth trends
WITH weekly_totals AS (
    SELECT 
        strftime('%Y-W%W', created_at) as week_number, 
        COUNT(*) as total_issues, 
        AVG(comments) as average_comments
    FROM hn_posts
    GROUP BY week_number
)

SELECT 
    week_number, 
    total_issues, 
    average_comments, 
    LAG(total_issues) OVER (ORDER BY week_number) as previous_week_issues, 
    ROUND(
        (total_issues - LAG(total_issues) OVER (ORER BY week_number)) * 100.0
        / NULLIF(LAG(total_issues) OVER (ORDER BY week_number), 0), 1
    ) as week_over_week_growth_percentage
FROM weekly_totals
ORDER BY week_number DESC 
LIMIT 8
"""