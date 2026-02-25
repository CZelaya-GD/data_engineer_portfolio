# 🚀 Week 1: Production CSV ETL Pipeline

**Day 1-7 → 500+ lines production code** • **Used by etlpipeline (112K HN)**

## 🎯 Production Components

| File | Purpose | Scale |
|------|---------|-------|
| `datacleaner_pipeline.py` | **Full ETL** (validation → safe_int → HN data) | 10K+ rows |
| `row_validation.py` | **300+ line validator** (headers, types, logging) | Production-ready |
| `safe_integer_converter.py` | **Robust int parser** (handles str/float/None) | Core utility |
| `generate_hn_data.py` | **Week 1→2 bridge** (CSV → HN dataset) | Feeds 112K pipeline |

## 🚀 Live Demo
```bash
cd week_1
python datacleaner_pipeline.py
# → data/output/week1_cleaned.csv (production validated)
# → Feeds hn_analysis_dashboard.sql → etlpipeline Docker API
```