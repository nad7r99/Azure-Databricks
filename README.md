# Formula1 Data Lakehouse — Databricks, Spark, Delta Lake, Unity Catalog & Lakeflow Jobs

An end-to-end, production-style Data Lakehouse project built on Databricks, using the Medallion Architecture (Landing → Bronze → Silver → Gold) to ingest, clean, model, and analyze historical Formula 1 racing data — from full-load batch processing all the way to a fully automated incremental pipeline.

This project was built as the hands-on capstone for a Databricks Data Engineering course covering the platform architecture, Unity Catalog, Delta Lake internals, dimensional modeling, orchestration with Lakeflow Jobs, and Databricks SQL analytics.


# Project Overview

The goal of this project is to build a reliable, governed, and scalable data platform that answers real analytical questions about Formula 1 — such as driver standings, constructor standings, and "who are the greatest drivers of all time?" — while following the same architectural patterns used in real production data engineering projects.

Data source: jolpica-f1 (Ergast-format relational data), covering circuits, races, constructors, drivers, race results, and sprint results.


# Architecture
Databricks Platform Architecture
- Control Plane (Databricks-managed): Web UI, Cluster Manager, Unity Catalog metadata, workspace storage
- Compute Plane:
     - Classic Compute — provisioned in the customer's cloud subscription
     - Serverless Compute — provisioned from Databricks' own pre-allocated resource pool for faster startup


     
Data Lakehouse Concept :
Combines the flexibility and low cost of Data Lakes with the ACID transactions and governance of Data Warehouses, powered by:

 - Delta Lake — the storage layer (Parquet + transaction log) providing ACID guarantees, versioning, and time travel
 - Unity Catalog — centralized governance, access control, and metadata management


Medallion Architecture

  -- Landing → Bronze → Silver → Gold

Layer:     Purpose:

Landing  -->	Raw source files land here untouched (ADLS container + Unity Catalog Volume)

Bronze   -->	Raw data ingested as-is into Delta tables, with schema enforcement + audit metadata (ingestion_timestamp, source_file)

Silver   -->	Cleaned, standardized, deduplicated, business-key-validated data

Gold	    -->   Dimensional model (Star Schema) optimized for reporting and analytics

























