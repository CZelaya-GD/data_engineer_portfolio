# 🚀 Week 2: HN SQL Analytics Dashboard

**Day 14 Deliverable** → **Feeds etlpipeline Docker API** • **112K rows**

## 🎯 Production HN Dashboard (3 KPIs)

| Metric | SQL Pattern | Scale |
|--------|-------------|-------|
| `DAILY_LEADER` | **RANK() window** | Daily top users |
| `CONSISTENT_USER` | **ROW_NUMBER() gaps** | 5+ day streaks |
| `TOP_POSTS_30D` | **CTEs + filtering** | Recent engagement |

## 🚀 Run Dashboard
```bash
sqlite3 ../etlpipeline/data/hn_posts.db < hnanalysis.sql > hn_kpis.csv
head hn_kpis.csv
# DAILY_LEADER,2026-02-25,github-actions[bot],59
```