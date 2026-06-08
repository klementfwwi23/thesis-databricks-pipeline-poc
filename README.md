# Declarative vs. Imperative Data Pipelines in Databricks

[![License](https://img.shields.io/badge/License-Academic%20Use-blue.svg)](LICENSE)
[![Databricks](https://img.shields.io/badge/Databricks-DLT%20%7C%20PySpark-orange.svg)](https://databricks.com)
[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)

This repository contains the **Proof of Concept (PoC) implementation** for the Bachelor thesis:

> **"Evaluation of Declarative Lakeflow Spark Pipelines Compared to Imperative ELT Scripts in Batch-Oriented Medallion Architectures"**  
> Author: Franziska Klement  
> Institution: DHBW Heidenheim  
> Submission: June 2026

## Overview

This project implements and compares two pipeline paradigms for data engineering in Databricks:

- **Declarative Pipeline**: Built using Delta Live Tables (DLT) with PySpark-based transformations and centralized data quality rule catalog
- **Imperative Pipeline**: Built using PySpark with programmatic transformations and inline data quality checks

Both pipelines implement the **Medallion Architecture** (Bronze → Silver → Gold) and process the **TPC-DS benchmark dataset** (store sales data) available in Databricks sample datasets to enable direct comparison across three evaluation dimensions:

1. **Development Effort & Maintainability** (Lines of Code, Cyclomatic Complexity, Maintainability Index)
2. **Runtime & Performance** (Execution time, scalability, cost)
3. **Data Quality** (Implementation effort, rule reusability, evolution effort)

---

## Architecture

### Medallion Architecture Layers

Both pipelines implement the following three-tier architecture using **PySpark**:

**Bronze Layer (Raw Data + Metadata)**
- Ingests TPC-DS benchmark data from Databricks sample datasets
- **No transformations** – raw data preservation
- **Metadata enrichment**: ingestion timestamp, source file information
- Declarative: DLT materialized views
- Imperative: PySpark `read` with manual metadata columns

**Silver Layer (Cleansed Data)**
- Data cleansing and standardization
- Null handling and data type corrections
- Duplicate removal
- Schema validation
- Declarative: DLT transformations with PySpark
- Imperative: PySpark DataFrames with explicit write operations

**Gold Layer (Business Models)**
- Business-focused dimensional models
- Fact and dimension tables
- Aggregations and metrics
- Optimized for analytical queries
- Declarative: DLT materialized views with PySpark
- Imperative: PySpark aggregations written to Delta tables

**Key Difference**: Declarative pipelines define **what data transformations should happen using high‑level logic and metadata**, while imperative ETL jobs define **how each transformation is executed step by step in procedural code**.

### Data Quality Framework

**Declarative Paradigm**:
- **Centralized rule catalog**: All DQ rules defined in `dq_bronze_rule_catalog.py`
- **Rule references**: Quality monitoring tables reference rules
- **Catalog-based architecture**: Single source of truth for rule definitions
- **DLT expectations**: Automatic quality enforcement during pipeline execution
- **Quality metrics**: Published to Unity Catalog views for monitoring

**Imperative Paradigm**:
- **Inline rule definitions**: DQ rules implemented directly in each quality module
- **Explicit rule assignment**: Each check explicitly defines its rule ID (e.g., `F.lit("IP_SILVER_013").alias("rule_id")`)
- **Distributed architecture**: Rules defined where they are used (per layer)
- **Function-based checks**: Custom `issue_frame()` and `metrics_frame()` functions
- **Quality metrics**: Written to dedicated Delta tables

**Key Difference**: Declarative uses a **centralized catalog with references**, imperative uses **distributed inline definitions**.

---

## Setup Instructions

### Prerequisites

- **Databricks Workspace**: Azure Databricks (Unity Catalog enabled)
- **Cluster Configuration**:
  - default "serverless" settings
- **Data Source**: TPC-DS benchmark data (pre-loaded in Databricks sample datasets)

### Installation Steps

**1. Clone this repository**

```bash
git clone https://github.com/[your-username]/thesis-databricks-poc.git
cd thesis-databricks-poc
```

**2. Import to Databricks Workspace**

Option A: Using Databricks CLI
```bash
databricks workspace import-dir . /Users/[your-email]/thesis-poc --overwrite
```

Option B: Using Databricks UI
- Navigate to Workspace → Import
- Select "Import from Git"
- Enter repository URL and branch/tag

**3. Configure Environment**

Create a Databricks cluster using the specifications in `config/cluster_config.json`:

```bash
databricks clusters create --json-file config/cluster_config.json
```

**4. Configure Unity Catalog Schemas**

Create target schemas for both pipelines:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.declarative;
CREATE SCHEMA IF NOT EXISTS workspace.imperative;
```

**5. Access TPC-DS Sample Data**

The pipelines use Databricks sample datasets. No additional data setup required.

Default path: `samples.tpcds_sf1000`

Update source paths in pipeline scripts if using custom TPC-DS data location.

**6. Run Pipelines**

**Declarative Pipeline (DLT)**:
- Create a new DLT pipeline in Databricks UI
- Add files: 
  - `declarative/transformations/01_dp_bronze.py`
  - `declarative/transformations/02_dp_silver.py`
  - `declarative/transformations/03_dp_gold.py`
  - (opional) data quality files
- Set target schema: `workspace.declarative`
- Set storage location: `/mnt/dlt/declarative/`
- Click "Start" to run the pipeline

**Imperative Pipeline (PySpark Jobs)**:
- Create Databricks Jobs for each layer:
  - Job 1: Bronze (`01_ip_bronze_tables.py`)
  - Job 2: Silver (`02_ip_silver_tables.py`)
  - Job 3: Gold (`03_ip_gold_models.py`)
  - (opional) data quality files, for each file one job
- Configure job dependencies: Bronze → Silver → Gold (sequential)
- Run jobs via Databricks Jobs UI or Workflows API

---

## Analysis & Metrics

### KPI Measurement Notebooks

**1. Development Effort & Maintainability** (`01_development_code_metrics.ipynb`)

Measures:
- **Lines of Code (SLOC)**: Using Radon 6.0.1 static analysis
- **Cyclomatic Complexity**: McCabe metric per function/file
- **Maintainability Index**: Radon MI composite metric (0-100 scale)
- **Duplicated Logic Blocks**: difflib SequenceMatcher (≥80% similarity threshold)

Run this notebook to generate comparison tables and visualizations for Dimension 1.

**2. Data Quality Metrics** (`03_data_quality_metrics.ipynb`)

Measures:
- **Implementation Effort**: DQ-specific SLOC and artifact count
- **Rule Reusability**: Unique rules vs. total rule applications (reuse ratio)
- **Evolution Effort**: Simulated change scenarios (add, modify, extend rules)
- **Standardization Coverage**: Tables meeting minimum DQ requirements per layer

Run this notebook to generate comparison tables and visualizations for Dimension 3.

### Preliminary Results Summary

**Key Insights**:
- **Declarative** paradigm achieves lower total SLOC (-19.5%) and better maintainability (MI +10.6%)
- **Imperative** paradigm shows higher DQ rule reuse (1.47x) but with more duplicate code blocks
- **Centralized catalog** (declarative) reduces unique rule count but shows lower reuse ratio due to single-definition pattern

*(Full results, interpretation, and statistical analysis available in the thesis document)*

---

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Cloud Platform** | Azure Databricks | - |
| **Data Lake** | Delta Lake | 2.4+ |
| **Orchestration (Declarative)** | Delta Live Tables (DLT) | - |
| **Orchestration (Imperative)** | Databricks Jobs | - |
| **Compute** | Apache Spark | 3.4.1 |
| **Language** | Python (PySpark) | 3.12 |
| **Data Format** | Delta, Parquet | - |
| **Benchmark Data** | TPC-DS | Sample datasets |
| **Catalog** | Unity Catalog | - |
| **Analysis Tools** | Radon, Lizard, pandas, matplotlib | 6.0.1, 1.17.10 |

---
## Citation

If you use or reference this work, please cite:

```bibtex
@thesis{klement2026declarative,
  author       = {Franziska Klement},
  title        = {Evaluation of Declarative Lakeflow Spark Pipelines Compared to Imperative ELT Scripts in Batch-Oriented Medallion Architectures},
  school       = {Duale Hochschule Baden-Württemberg Heidenheim},
  year         = {2026},
  type         = {Bachelor's Thesis},
  month        = {June},
  url          = {https://github.com/klementf.wwi23/thesis-databricks-poc}
}
```

---

## License

This project is provided for **academic review and research purposes only**.

Copyright © 2026 Franziska Klement

Redistribution, modification, and commercial use are prohibited without explicit written permission from the author. This code is part of a Bachelor thesis submitted to DHBW Heidenheim and is subject to academic integrity policies.

See [LICENSE](LICENSE) file for full terms.

---

## Author

**Franziska Klement**  
DHBW Heidenheim – Wirtschaftsinformatik
Contact: klementf.wwi23@student.dhbw-heidenheim.de  
Location: Heidenheim, Germany

---

## Acknowledgments

- **DHBW Heidenheim**: For academic supervision and resources
- **Databricks**: For providing the platform, Delta Live Tables framework, and TPC-DS sample datasets
- **TPC Benchmark Council**: For the TPC-DS benchmark specification
- **Open Source Community**: Radon, Lizard, pandas, matplotlib, and other analysis tools

---

**Last Updated**: June 8, 2026  
**Status**: writing (Thesis Submitted & Repository Archived)  
**Version**: v2.0-final
