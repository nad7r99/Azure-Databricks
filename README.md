# Formula1 Data Lakehouse — Databricks, Spark, Delta Lake, Unity Catalog & Lakeflow Jobs

An end-to-end, production-style Data Lakehouse project built on Databricks, using the Medallion Architecture (Landing → Bronze → Silver → Gold) to ingest, clean, model, and analyze historical Formula 1 racing data — from full-load batch processing all the way to a fully automated incremental pipeline.

This project was built as the hands-on capstone for a Databricks Data Engineering course covering the platform architecture, Unity Catalog, Delta Lake internals, dimensional modeling, orchestration with Lakeflow Jobs, and Databricks SQL analytics.


# Project Overview

The goal of this project is to build a reliable, governed, and scalable data platform that answers real analytical questions about Formula 1 — such as driver standings, constructor standings, and "who are the greatest drivers of all time?" — while following the same architectural patterns used in real production data engineering projects.

Data source: jolpica-f1 (Ergast-format relational data), covering circuits, races, constructors, drivers, race results, and sprint results.


# Architecture
### Databricks Platform Architecture
- Control Plane (Databricks-managed): Web UI, Cluster Manager, Unity Catalog metadata, workspace storage
- Compute Plane:
     - Classic Compute — provisioned in the customer's cloud subscription
     - Serverless Compute — provisioned from Databricks' own pre-allocated resource pool for faster startup


     
### Data Lakehouse Concept :
Combines the flexibility and low cost of Data Lakes with the ACID transactions and governance of Data Warehouses, powered by:

 - Delta Lake — the storage layer (Parquet + transaction log) providing ACID guarantees, versioning, and time travel
 - Unity Catalog — centralized governance, access control, and metadata management


### Medallion Architecture

  - Landing → Bronze → Silver → Gold

Layer:-->Purpose:

Landing  -->	Raw source files land here untouched (ADLS container + Unity Catalog Volume)

Bronze   -->	Raw data ingested as-is into Delta tables, with schema enforcement + audit metadata (ingestion_timestamp, source_file)

Silver   -->	Cleaned, standardized, deduplicated, business-key-validated data

Gold	    -->   Dimensional model (Star Schema) optimized for reporting and analytics



### Unity Catalog Object Model

<img width="650" height="202" alt="Screenshot from 2026-08-05 18-27-23" src="https://github.com/user-attachments/assets/517ee6f4-d4af-41a4-af26-71a9287a2ae7" />

+ Storage Credentials & External Locations (secure cloud storage access)



### Gold Layer — Dimensional Model (Star Schema)

<img width="621" height="103" alt="Screenshot from 2026-08-05 18-28-51" src="https://github.com/user-attachments/assets/0ca24190-0ac4-490c-9f6f-282d407a6c7d" />


- fact_session_results — one row per driver per race/sprint session, with a session_type column to distinguish Race vs Sprint results (unified from two separate fact tables for simpler downstream aggregation)
- dim_races, dim_drivers, dim_constructors — descriptive context, enriched with a derived region attribute from nationality



# Key Engineering Practices Implemented
- Explicit schema definition (StructType/StructField and DDL-style strings) instead of relying on inferSchema, for reliability and performance
- Configurable error handling via read modes (permissive, dropMalformed, failFast)
- Metadata enrichment — every Bronze record is tagged with ingestion_timestamp and source_file
- Reusable helper functions and a centralized environment configuration notebook to eliminate hard-coded catalog/schema/path values across notebooks
- Balanced Spark coding style — a "middle ground" between fully step-by-step and fully chained transformations, for readability and maintainability without sacrificing conciseness
- Delta Lake internals — transaction log (_delta_log), DESCRIBE HISTORY, time travel (VERSION AS OF / TIMESTAMP AS OF), and RESTORE TABLE


# Incremental Data Processing

The pipeline evolved from a simple full-refresh design to a production-style incremental pipeline:

- Source files arrive in batch-based folders (e.g. landing/2025-01/, landing/2025-02/...), with a cutover batch containing full historical data plus the first new batch
- Bronze layer: data is appended and partitioned by batch_id, using replaceWhere to safely reprocess a batch without duplicating records
- Silver/Gold layers: a unified MERGE strategy (via a reusable write_to_silver helper function) handles inserts and updates for both snapshot and change data, while:
    - Preserving created_timestamp on first insert only
    - Updating updated_timestamp on every merge
    - Protecting against out-of-order/older batch reprocessing using a batch_id comparison condition
 
    - 
### Orchestration (Batch Control)

A dedicated batch_control Delta table tracks the lifecycle of every batch (in_progress → completed), enabling full automation:

Identify Next Batch  →  Create New Batch (in_progress)  →  Run Bronze/Silver/Gold  →  Complete Batch

# Orchestration with Lakeflow Jobs
- Multi-task workflows built entirely within Databricks — no external orchestrator (e.g. Airflow, ADF) required
- Job Compute used over All-Purpose Compute (≈50% cheaper, automatic start/stop, workload isolation)
- Triggers implemented:
    - File Arrival Trigger — job starts automatically when a "batch complete" flag file lands in a monitored folder
    - Table Update Trigger — job starts automatically when a new row is inserted into a monitored control table
- Task-level configuration: dependencies, retries, notifications, and metric thresholds

# Analytics & Reporting (Databricks SQL)
- Driver Standings and Constructor Standings views built with Spark SQL, using RANK() OVER (PARTITION BY season ORDER BY total_points DESC, total_wins DESC)
- A custom "Greatness Score" analysis — going beyond raw points (which are skewed by F1's evolving points system and race-calendar length across eras) to fairly compare     legendary drivers across different periods of the sport
- Interactive dashboards built and published via Databricks SQL, backed by a dedicated SQL Warehouse


# Tech Stack

<img width="748" height="414" alt="Screenshot from 2026-08-05 18-39-38" src="https://github.com/user-attachments/assets/35601978-f534-4a49-92e7-54b3606a8093" />



# Repository Structure

<img width="629" height="379" alt="Screenshot from 2026-08-05 18-40-27" src="https://github.com/user-attachments/assets/0d0f2e4b-3ecb-4889-997c-724b9ce74fed" />



# About This Project

This project was completed as part of a Databricks Data Engineering course, covering the platform end-to-end — from architecture fundamentals to a fully automated, incrementally-processing production pipeline.

Course: Real World Project on Formula1 using Databricks, Spark, Delta Lake, Unity Catalog, Lakeflow Jobs


# Connect
If you have questions, suggestions, or just want to talk data engineering — feel free to reach out or open an issue!

# about me
I'm Nader Mohamed, Studting at faculty of science Math & CS department, Care about Data.








