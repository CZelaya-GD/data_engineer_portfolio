### Hacker News Analytics (hnanlysis.sql)

- Input: Raw 'hn_posts' table (Hacker News dump)

- Output: Single dashboard query with: 
    - Daily top commenters (engagement leaders)
    - Users active 5+ consecutive days (power users)
    - Top posts in the last 3- days (high engagement)

- SQL feature: CTEs, window functions (RANK. ROW_NUMBER), UNION ALL. 

- Usage: Run 'hnanalysis.sql' in SQLite or any warehouse and connect to BI 