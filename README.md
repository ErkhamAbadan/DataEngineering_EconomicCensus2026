# Distributed Business Data Pipeline – SE2026 Supporting Project

This repository documents an end-to-end **distributed data pipeline** built to support large-scale business data collection for **Sensus Ekonomi 2026 (SE2026)** conducted by **Badan Pusat Statistik (BPS)**.

The project focuses on scalable web scraping, data quality control, SQL-based cleansing, and validation to produce a high-quality final business dataset.

---

## 📌 Project Overview

SE2026 is a nationwide economic census conducted every 10 years to capture a comprehensive picture of business activities across Indonesia (excluding agriculture).

This project supports the census by:
- Collecting business data from public platforms
- Processing data at scale using distributed scraping
- Ensuring data quality, consistency, and geographic validity

---

## 📊 Data Scale

- Total input queries: **±350,000**
- Distributed scraping output: **60+ CSV files**
- Integrated raw dataset: **±1,500,000 rows**
- Final validated dataset: **±300,000 business records**

---

## 🧱 High-Level Data Pipeline

Input Queries
↓
Query Partitioning (6 parts × 10 PCs)
↓
Distributed Scraping & Processing
↓
Raw Business Data (60+ CSV files)
↓
Scraping Completeness Check (QC)
↓
Data Merge & Integration (SQL)
↓
Data Cleansing & Deduplication (SQL)
↓
Location Validation & Similarity Scoring
↓
Final Validated Dataset

---

## 🔍 Pipeline Stages

### 1. Query Distribution
- ±350K business queries split into **6 logical partitions**
- Each partition distributed across **10 PCs**
- Resulting in **60 initial query CSV files**

Purpose:
- Reduce scraping time
- Enable parallel processing
- Improve fault isolation

---

### 2. Distributed Scraping & Processing
- Scraping executed independently on each PC
- Processing includes:
  - Business metadata extraction
  - Feature engineering (additional columns based on rules)
  - Skipping non-essential assets (e.g. images) to improve speed
- Output stored as CSV per task

---

### 3. Scraping Completeness Check (Quality Control)
- Verification that all assigned queries were successfully scraped
- Identification of:
  - Missing queries
  - Incomplete or corrupted files
- Failed parts were **re-scraped selectively**, not from scratch

---

### 4. Data Merge & Integration
- Initial merging via Python proved unstable due to:
  - File corruption
  - Column inconsistency
- Final approach:
  - Schema normalization
  - CSV correction
  - Full merge executed in **SQL**
- Resulting dataset size: **±1.5 million rows**

---

### 5. Data Cleansing & Deduplication (SQL)
Executed entirely using SQL queries:
- Key-based deduplication across business identifiers
- Removal of duplicate records
- Filtering records with:
  - NULL values
  - Blank critical fields
- Rule-based filtering to improve consistency

---

### 6. Location Validation & Similarity Scoring
- Similarity scoring between:
  - Input query
  - Scraped business name (`idsbr`)
- Records classified as:
  - `Ditemukan`
  - `Tidak Ditemukan`
- Geographic validation:
  - Latitude & longitude filtering
  - Businesses outside **Surabaya** excluded
- Only records above a defined similarity threshold retained

---

## ⚙️ Challenges & Solutions

| Challenge | Solution |
|--------|---------|
| Large number of corrupted CSV files | Schema correction + SQL-based merging |
| Slow initial scraping | Optimized worker logic & skipped image loading |
| Long-running scraping tasks | Distributed execution across 10 PCs |
| Scraping failures | Selective re-scraping based on QC |
| Limited monitoring time | Remote monitoring via AnyDesk during off-hours |

---

## 🧠 Technical Stack

- **Python** – scraping & preprocessing
- **SQL (MySQL)** – merging, cleansing, deduplication, validation
- **CSV-based data exchange**
- **Distributed execution (multi-PC)**

---

## 🔐 Ethics & Data Governance

- Data sourced exclusively from **public platforms**
- Used only for **statistical and analytical purposes**
- Processing followed **BPS data governance guidelines**
- Raw and final datasets are **not publicly shared**
- Documentation and code are provided for technical reference only

---

## 🎯 Key Takeaways

- Built a scalable, fault-tolerant data pipeline
- Managed large datasets (1M+ rows) end-to-end
- Applied SQL for production-grade data quality control
- Gained hands-on experience in distributed data processing

---

## 📎 Notes

This repository focuses on **pipeline design and engineering practices**.  
Sensitive data and official census outputs are intentionally excluded.

---
