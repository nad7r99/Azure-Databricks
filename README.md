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












