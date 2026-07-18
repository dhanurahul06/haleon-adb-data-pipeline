# Haleon Practice Project — Azure Databricks End-to-End Data Pipeline
### Key Vault → Databricks (JDBC) → Unity Catalog (Bronze/Silver/Gold) → Power BI

An end-to-end data engineering pipeline built entirely on Azure Databricks —
no Azure Data Factory — implementing secure secret management, direct
database ingestion, incremental loading, a medallion architecture (Bronze →
Silver → Gold), scheduled orchestration, and business intelligence reporting.

Built as part of an Industrial Training placement under a Data Platform
Engineering Lead in a Data Architecture & Engineering department, at the
mentor's direction to rebuild the pipeline as an ADB-native architecture.

> **Note on the dataset:** since real Haleon data wasn't available for
> practice use, this project uses a real, publicly available dataset —
> six years of daily pharmaceutical sales data across 8 drug categories,
> sourced from Kaggle (`milanzdravkovic/pharma-sales-data`) — chosen for its
> genuine transactional structure and relevance to Haleon's consumer health
> industry. No real Haleon proprietary data is used anywhere in this project.

---

## Architecture Overview

```
Azure SQL Database (Dev/Prod)
        ↓  [Direct JDBC read, credentials from Key Vault]
Databricks Notebook — Bronze Ingestion
        ↓  [Native PySpark watermark-based incremental logic]
Unity Catalog — bronze.pharma_sales_raw
        ↓  [Databricks Notebook — Silver Transform]
Unity Catalog — sanitized.pharma_sales   (adds IsWeekend flag)
        ↓  [Databricks Notebook — Gold Transform]
Unity Catalog — optimized.pharma_sales_summary   (monthly aggregates)
        ↓
Power BI Dashboard

Orchestration: Databricks Workflow (multi-task job), scheduled daily at 9:00 AM IST
Secrets: Azure Key Vault, accessed via a Databricks secret scope
```

---

## Why ADB-Only (No ADF)

This project was originally built with Azure Data Factory orchestrating
ingestion into ADLS Gen2, with Databricks handling transformation. On mentor
guidance, it was rebuilt to run entirely within Databricks:

- **Ingestion**: replaced ADF's Copy Activity with a direct JDBC read inside
  a Databricks notebook, using `spark.read.jdbc()`
- **Incremental logic**: replaced ADF's Lookup Activity + watermark pattern
  with the same logic implemented natively in PySpark, reading/writing the
  watermark directly via JDBC
- **Orchestration & scheduling**: replaced ADF's pipeline + trigger with a
  native **Databricks Workflow** (multi-task job with `depends_on` chaining
  and a cron schedule)
- **Secrets**: replaced ADF's Linked Service credentials with **Azure Key
  Vault**, accessed through a Databricks secret scope — credentials are never
  hardcoded in any notebook

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Azure SQL Database | Source system — relational sales data |
| Azure Key Vault | Secure storage for SQL credentials |
| Azure Databricks | Ingestion, transformation, orchestration, scheduling |
| Unity Catalog | Governance — Bronze/Silver/Gold tables, access control |
| PySpark | Data processing and transformation logic |
| Power BI | Reporting and visualization |
| GitHub Actions | CI (working) — notebook syntax/lint validation on every push |
| Databricks Asset Bundles (DAB) | CD design — Dev→Prod deployment (see below) |

---

## Project Components

### 1. Secrets Management
- Azure Key Vault (`rahul-de-kv-dev`) stores SQL Server username and password
- A Databricks secret scope (`kv-dev-scope`) is backed by this Key Vault
- Notebooks retrieve credentials via `dbutils.secrets.get()` — never hardcoded

### 2. Bronze — Ingestion (`01_bronze_ingestion.py`)
- Connects directly to Azure SQL Database via JDBC (no ADF)
- Reads a watermark value from a `LoadControl` table to determine what's new
- Pulls only rows newer than the watermark using JDBC query pushdown
- Writes raw data into a Unity Catalog Bronze table
- Updates the watermark in SQL after a successful load
- **Proven incremental**: verified that a full load correctly captures all
  existing data, and a subsequent run with no new data correctly loads zero
  rows; a follow-up test inserting new rows confirmed only the new rows were
  picked up on the next run

### 3. Silver — Sanitized (`02_silver_transform.py`)
- Reads the Bronze table from Unity Catalog
- Adds a derived `IsWeekend` column (Yes/No based on day of week)
- Writes to `sanitized.pharma_sales`

### 4. Gold — Optimized (`03_gold_transform.py`)
- Reads the Silver table from Unity Catalog
- Aggregates into monthly sales totals per drug category, split by
  weekend/weekday
- Writes to `optimized.pharma_sales_summary`

### 5. Orchestration & Scheduling
- All three notebooks chained into a single Databricks Workflow
  (`pharma-sales-pipeline-dev`) with explicit task dependencies
- Scheduled to run daily at **9:00 AM IST**
- Verified via manual "Run now" — all three tasks succeed in sequence

### 6. Power BI Dashboard
- Connects directly to the Gold Unity Catalog table via a Databricks SQL
  Warehouse
- Four visuals: sales trend line chart (by month, weekday vs weekend),
  stacked bar chart (category comparison by year), KPI card (total sales),
  and donut chart (category share)

### 7. CI/CD
- **CI (working):** GitHub Actions workflow (`.github/workflows/ci.yml`)
  runs on every push — validates Python syntax of all notebook files, runs
  flake8 linting, and confirms the SQL schema file is present and non-empty
- **CD (designed):** Databricks Asset Bundles (DAB) — full bundle
  configuration and deployment workflow designed and documented in
  [`DAB_CICD_DESIGN.md`](./DAB_CICD_DESIGN.md), covering Dev→Prod promotion.
  Execution was blocked by local CLI installation issues on Windows; the
  design is complete and ready to run in a Linux/WSL/CI environment.

---

---

## Reliability — Exception Handling & Alerting

- **Exception handling:** every major step across all three notebooks
  (credential retrieval, JDBC reads, table writes) is wrapped in try/except
  blocks with specific, readable error messages — replacing raw Spark/JVM
  stack traces with actionable diagnostics. The Bronze ingestion read
  includes a retry loop (3 attempts, 30s backoff) specifically to handle
  Azure SQL Database's serverless auto-pause behavior, which was hit and
  confirmed working during testing — a transient "database is not currently
  available" error was caught, retried automatically, and succeeded on the
  next attempt.
- **Email alerting:** the Databricks Workflow (`pharma-sales-pipeline-dev`)
  has notifications configured for both job success and job failure,
  verified with a real test run and a received confirmation email.

## Repository Structure

```
├── README.md
├── DAB_CICD_DESIGN.md              # DAB bundle design + CD workflow (documented)
├── notebooks/
│   ├── 01_bronze_ingestion.py      # JDBC read, watermark logic, Bronze write
│   ├── 02_silver_transform.py      # IsWeekend enrichment
│   └── 03_gold_transform.py        # Monthly aggregation
├── sql/
│   └── create_tables.sql           # PharmaSalesDaily + LoadControl schema
├── powerbi/
│   └── pharma_sales_analytics.pbix # 4-visual dashboard
└── .github/
    └── workflows/
        └── ci.yml                  # Working CI: syntax + lint validation
```

---

## Key Concepts Implemented

- **Medallion Architecture** — Bronze (raw) → Silver (sanitized/enriched) →
  Gold (optimized/aggregated), entirely within Unity Catalog
- **Incremental / watermark-based loading** — implemented natively in
  PySpark, proven with real before/after row-count evidence
- **Secrets management** — Azure Key Vault + Databricks secret scopes,
  no credentials hardcoded anywhere in source control
- **Native orchestration** — Databricks Workflows with task dependencies
  and cron scheduling, replacing external orchestration tooling
- **CI/CD** — working CI via GitHub Actions; CD designed via Databricks
  Asset Bundles, with an honest account of what was executed vs. designed

---

## Related Project

The original ADF-based version of this pipeline (SQL → ADF → ADLS Gen2 →
Databricks → Unity Catalog → Power BI) is documented separately and was
superseded by this ADB-only architecture per mentor guidance.

---

## Author

**Rahul Varma** — Final Year B.Tech Information Technology, MIT Manipal
ITR Project | Data Architecture & Engineering Department
[LinkedIn](https://www.linkedin.com/in/dhanunjay-rahul-varma-646893270/) 

---

*Project built iteratively with real troubleshooting throughout — including
a serverless-compute/orchestration compatibility issue solved via Databricks
Jobs + REST API, an Azure AD permission restriction that required pivoting
the CI/CD authentication approach, and various environment configuration
fixes along the way.*
