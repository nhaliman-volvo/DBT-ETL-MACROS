# DBT Macro Suite: High-Level Overview

This repository organizes a set of custom DBT macros into three main categories — **Column Validation**, **Helper**, and **Metadata & Logging** — to support robust, reusable, and metadata-driven data transformation workflows.

These macros are designed to enhance DBT projects with advanced validation logic, dynamic metadata handling, and traceable logging mechanisms.

---

## 1. Column Validation Macros

These macros are used to detect and classify common data quality issues such as nulls, duplicates, invalid values, and datatype mismatches.

### Use Cases in DBT Pipeline

- Validate source data before transformations
- Identify and classify bad records
- Generate clean datasets for downstream models
- Implement row-level error isolation and reporting

---

## 2. Helper Macros

Helper macros extract, transform, and map column-level metadata to simplify dynamic SQL generation and enforce schema consistency.

### Use Cases in DBT Pipeline

- Dynamically cast columns based on metadata
- Export and use test metadata for transformations
- Enable reusable, parameterized SQL logic
- Support compile-time code generation using model schema

---

## 3. Metadata and Logging Table Macros

These macros manage batch IDs, logging keys, and tracking metadata related to data loads, model runs, and source lineage.

### Use Cases in DBT Pipeline

- Insert batch metadata during ETL execution
- Track model-level processing and status
- Enable audit logging and operational monitoring
- Automatically create and update logging tables

---

## How to Use

- Macros can be invoked via `dbt run-operation` or called within models using Jinja.
- Each macro is modular and can be integrated into custom workflows or DBT hooks.
- Refer to the detailed `.md` documentation files under each macro category for implementation-level details.

---

## Documentation Structure

macros/  
│  
├── column_validation/ # Column validation macros  
│ └── DBT - Column Validation Macros Documentation.md  
│  
├── helper_macros/ # Metadata and casting utilities  
│ └── DBT - Log and Metadata Handling Macros Documentation.md  
│  
└── logging_and_metadata/ # Batch logging and metadata tracking  
│ └── DBT - Log and Metadata Handling Macros Documentation.md
