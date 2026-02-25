# 🚀 CZelaya Data Engineer Portfolio


## 🎯 3 Live Projects (Week 1-3)

| **Week** | **Pipeline** | **Tech Stack** | **Live Demo** | **Rows Processed** |
|----------|--------------|----------------|---------------|-------------------|
| **1** | Data Cleaner | Python, Type hints | `datacleaner.py sample.csv` | 10K CSV rows |
| **2** | HN SQL Analysis | SQLite, CTEs, Window | `hnanalysis.sql` | 112K HN issues |
| **3** | **ETL Production** | **Docker + Flask API** | **`localhost:5000/api/users`** | **112K → REST API** |

## 🔥 Featured: HN ETL Pipeline (Phase 1 Capstone)
GitHub Issues (112K) → Docker ETL → SQLite → REST API (4 endpoints)

```bash
git clone https://github.com/CZelaya-GD/data_engineer_portfolio

cd etlpipeline

docker compose up -d

curl localhost:5000/api/users | jq '.'

# → github-actions[bot]: 59 comments, 61% HN dominance
```
📊 Skills Matrix

- 🐍 Python → Functions, OOP, pandas, logging, type hints
- 🗄️  SQL   → CTEs, Window functions, date partitioning  
- 🐳 Docker → Multi-stage builds, healthchecks, 0.0.0.0 binding
- 🌐  API   → Flask production server, 4 JSON endpoints

## 🎓 100-Day Mastery (Phase 1 ✅)

- Days 1-21: Python → SQL → ETL → Docker Production
- Days 22-49: Airflow orchestration (next)
- Days 50-100: GCP deployment 
