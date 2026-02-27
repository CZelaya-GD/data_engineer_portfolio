# 🚀 HN GitHub Issues Analytics Pipeline (Week 3 Production Demo)

## 🎯 What This Pipeline Does
ETL + API serving **real-time analytics** on **112K GitHub issues** posted to Hacker News (Feb 2026 data).

**Business Value**: 
- **ETL**: Scrapes 1000+ HN GitHub issues daily
- **API**: Real-time analytics (`localhost:5000/api/users`)
- **Production**: Docker + healthchecks + logging

## 🏗️ Architecture (ETL → API)
- Day 15 ETL: GitHub API → hn_posts.db (112K rows)
- ↓
- Day 17 API: hn_posts.db → 4 JSON endpoints
- ↓
- Dashboard: curl /api/users → github-actions[bot] = 61% dominance

## 📊 Live Endpoints (http://localhost:5000)

| Endpoint | Sample Data | Use Case |
|----------|-------------|----------|
| `/api/dashboard` | `github-actions[bot]: 19 comments (Feb 11)` | **Daily leaders** |
| `/api/users` | `github-actions[bot]: 59 comments, 6 days` | **Top users** (7d) |
| `/api/trending` | `"add my name to contributors": 37 mentions` | **Hot topics** (7d) |
| `/api/activity` | `0 issues` (weekend normal) | **Pipeline health** (24h) |



## 🔧 Production Standards Applied

| Standard              | Implementation                     |
| --------------------- | ---------------------------------- |
| Docstrings            | Full Purpose/Inputs/Outputs/Raises |
| Input Validation      | FileNotFoundError for missing DB   |
| Logging               | logging.info/error (no print)      |
| Type Hints            | Complete throughout                |
| Single Responsibility | execute_query() DRY reusable       |

## 🗄️ Database Schema (hn_posts.db)

```sql
-- Day 15 ETL output
CREATE TABLE hn_posts (
    id TEXT PRIMARY KEY,
    title TEXT,
    user TEXT,
    score INTEGER,
    comments INTEGER,
    created_at TEXT
);
-- 112K rows of GitHub → HN issues data
```


## 🚀 Deploy Instructions 
```bash
# Clone + install
git clone https://github.com/CZelaya-GD/data_engineer_portfolio
cd etlpipeline

# Day 15: ETL (GitHub → SQLite)
python etl_hn_github.py  # → data/hn_posts.db

# Day 17: API Server
python serve_hn.py       # → http://localhost:5000

# Production check
curl http://localhost:5000/health
```

## 🐛 Troubleshooting
| Error               | Cause      | Fix              |
| ------------------- | ---------- | ---------------- |
| FileNotFoundError   | Missing DB | Run Day 15 ETL   |
| sqlite3.Error       | SQL syntax | Check queries.py |
| 0 issues (activity) | Weekend    | Normal behavior  |

## 📈 Real Insights (Feb 2026)
1. github-actions[bot] dominates 61% of HN GitHub issues

2. Contributors spam = #1 trending topic (37x "add my name")

3. Copilot emerging (0.61 avg score vs bots ~0.3)

## 🎯 Next Steps (Week 4 Preview)
- Dockerize this pipeline

- Schedule daily ETL with Airflow

- Deploy to GCP Cloud Run