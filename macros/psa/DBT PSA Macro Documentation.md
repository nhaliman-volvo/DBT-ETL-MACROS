# PSA Layer - Macro Documentation

This document outlines the macros used in the **PSA (Persistent Staging Area)** layer. Macros are grouped by functional directories for clarity and maintainability.

## Overview

The PSA layer implements [Type 2 Slowly Changing Dimension (SCD Type 2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) logic to track historical changes to data. It maintains:
- Current and historical versions of records
- Start and end dates for each version
- Hash keys for change detection
- Load timestamps and batch metadata

---

## Macro Imports

To make all macros available within your models or other macros, import this PSA initializer in your dbt files:

```jinja
{% import 'psa/init.sql' as psa %}
```

**Usage Example:**
```sql
{{ config(materialized='incremental') }}

{% import 'psa/init.sql' as psa %}

-- Use the main template to implement PSA versioning
{{ psa.main_template.change_tracking_versioning_template(
    model_name=this,
    stg_model='stg_customers',
    id_column='customer_id'
) }}
```

---

## `psa/config/`

### **get_psa_config**

**Functionality:**  
Fetches configuration settings for the PSA layer, including timestamps, metadata fields, and soft-delete logic.

**Arguments:**  
None

**Scope:** Compile-time

**Logic:**
- Defines standard column names for versioning (_START_DATE, _END_DATE)
- Defines metadata fields (_BATCH_ID, _LOAD_TIME)
- Sets time zone configuration for timestamps
- Specifies null values for open-ended records

**Usage Example:**
```sql
{% set psa_config = psa.config.get_psa_config() %}

-- Access configuration values
SELECT 
    {{ psa_config.start_date_column }} as start_date,
    {{ psa_config.end_date_column }} as end_date
FROM {{ ref('my_psa_model') }}
```

---

## `psa/system/`

### **get_system_variables**

**Functionality:**  
Provides system-wide variables like `_LOAD_TIME`, `_BATCH_ID`, and `_PROCESSED_TIME`.

**Arguments:**  
None

**Scope:** Compile-time

**Logic:**
- Generates current timestamp in standardized format
- Creates a unique batch ID for data lineage
- Captures processing time for performance monitoring

**Usage Example:**
```sql
{% set system_vars = psa.system.get_system_variables() %}

-- Add system variables to a model
SELECT 
    *,
    {{ system_vars.load_time }} as _LOAD_TIME,
    {{ system_vars.batch_id }} as _BATCH_ID
FROM {{ ref('staging_data') }}
```

---

### **get_relation_columns**

**Functionality:**  
Returns the list of columns from a relation (database object).

**Arguments:**

| Argument     | Type   | Description                        |
| ------------ | ------ | ---------------------------------- |
| `relation`   | string | The relation to get columns from   |

**Scope:** Compile-time

**Logic:**
- Uses dbt's adapter to query information schema
- Extracts column names from the specified relation
- Excludes system and metadata columns if specified

**Usage Example:**
```sql
{% set columns = psa.system.get_relation_columns(relation=ref('my_table')) %}

-- Use columns in a select statement
SELECT
    {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('my_table') }}
```

---

### **get_staging_columns**

**Functionality:**  
Returns the list of columns from a staging model.

**Arguments:**

| Argument     | Type   | Description                        |
| ------------ | ------ | ---------------------------------- |
| `model_name` | string | The staging model name             |

**Scope:** Compile-time

**Logic:**
- Gets columns from the staging model
- Filters out system and metadata columns
- Returns business columns that should be tracked for changes

**Usage Example:**
```sql
{% set stg_columns = psa.system.get_staging_columns(model_name='stg_customers') %}

-- Use in hash key generation
{{ psa.columns.generate_hash_key(columns=stg_columns, id_column='customer_id') }}
```

---

## `psa/columns/`

### **generate_hash_key**

**Functionality:**  
Generates a SHA1 hash based on all columns except the specified ID column, used for detecting row changes.

**Arguments:**

| Argument    | Type     | Description                                          |
| ----------- | -------- | ---------------------------------------------------- |
| `columns`   | list     | List of all column names in the model                |
| `id_column` | string   | Name of the primary key column to exclude from hash  |

**Scope:** Compile-time

**Logic:**
- Creates a concatenated string of all column values except the ID column
- Generates a SHA1 hash of this string
- Uses this hash to detect changes between versions of the same record

**Usage Example:**
```sql
{% set business_columns = psa.system.get_staging_columns(model_name='stg_customers') %}

SELECT
    customer_id,
    {{ psa.columns.generate_hash_key(columns=business_columns, id_column='customer_id') }} as _HASH_KEY,
    *
FROM {{ ref('stg_customers') }}
```

---

## `psa/ctes/`

### **generate_stg_model_cte**

**Functionality:**  
Creates a CTE for the staging model with appropriate transformations including hash key generation.

**Arguments:**

| Argument     | Type   | Description                      |
| ------------ | ------ | -------------------------------- |
| `model_name` | string | Name of the staging model        |
| `id_column`  | string | Primary key column               |

**Scope:** Runtime

**Logic:**
- Selects all columns from the staging model
- Adds a hash key column for change detection
- Adds system variables like load time and batch ID
- Returns a CTE that's ready for change detection processes

**Usage Example:**
```sql
WITH 
{{ psa.ctes.generate_stg_model_cte(
    model_name='stg_customers',
    id_column='customer_id'
) }}

SELECT * FROM stg_model
```

---

### **generate_existing_data_cte**

**Functionality:**  
Creates a CTE for the existing data in the PSA model, focusing on the current active records.

**Arguments:**

| Argument     | Type   | Description                      |
| ------------ | ------ | -------------------------------- |
| `model_name` | string | Name of the PSA model            |

**Scope:** Runtime

**Logic:**
- Selects data from the PSA model
- If incremental, filters for active records (WHERE _END_DATE IS NULL)
- If first run, creates an empty CTE with the right structure
- Sets up data for comparison with the staging model

**Usage Example:**
```sql
WITH 
{{ psa.ctes.generate_existing_data_cte(
    model_name=this
) }}

SELECT * FROM existing_data
```

---

### **generate_new_records_cte**

**Functionality:**  
Generates a CTE identifying new records that don't exist in the PSA model.

**Arguments:**

| Argument           | Type   | Description                         |
| ------------------ | ------ | ----------------------------------- |
| `source_model`     | string | Incoming (STG) model name           |
| `active_model`     | string | PSA model name for active records   |
| `id_column`        | string | Primary key column                  |

**Scope:** Runtime

**Logic:**
- Uses LEFT JOIN to find records in staging that don't exist in PSA
- Matches on the primary key column
- Adds SCD Type 2 metadata (_START_DATE, _END_DATE=NULL)
- Prepares new records for insertion

**Usage Example:**
```sql
WITH 
stg_model AS (...),
existing_data AS (...),
{{ psa.ctes.generate_new_records_cte(
    source_model='stg_model',
    active_model='existing_data',
    id_column='customer_id'
) }}

SELECT * FROM new_records
```

---

### **generate_updated_records_cte**

**Functionality:**  
Generates a CTE identifying updated records (same key, different hash).

**Arguments:**

| Argument           | Type   | Description                         |
| ------------------ | ------ | ----------------------------------- |
| `source_model`     | string | Incoming (STG) model name           |
| `active_model`     | string | PSA model name for active records   |
| `id_column`        | string | Primary key column                  |
| `hash_column`      | string | Name of the hash column             |

**Scope:** Runtime

**Logic:**
- Uses INNER JOIN to match records with same ID but different hash keys
- Detects changes based on hash key comparison
- Adds SCD Type 2 metadata (_START_DATE, _END_DATE=NULL)
- Prepares updated records for insertion as new versions

**Usage Example:**
```sql
WITH 
stg_model AS (...),
existing_data AS (...),
{{ psa.ctes.generate_updated_records_cte(
    source_model='stg_model',
    active_model='existing_data',
    id_column='customer_id',
    hash_column='_HASH_KEY'
) }}

SELECT * FROM updated_records
```

---

### **generate_mark_previous_records_cte**

**Functionality:**  
Generates a CTE that marks previous versions of updated records as inactive by setting their end date.

**Arguments:**

| Argument       | Type   | Description                         |
| -------------- | ------ | ----------------------------------- |
| `psa_model`    | string | Name of the PSA model               |
| `updated_cte`  | string | Name of the CTE holding updated data|
| `id_column`    | string | Primary key column                  |

**Scope:** Runtime

**Logic:**
- Identifies currently active records that have updates
- Sets _END_DATE to current timestamp
- Preserves all other fields from the original record
- Maintains historical trail of changes

**Usage Example:**
```sql
WITH 
stg_model AS (...),
existing_data AS (...),
updated_records AS (...),
{{ psa.ctes.generate_mark_previous_records_cte(
    psa_model='existing_data',
    updated_cte='updated_records',
    id_column='customer_id'
) }}

SELECT * FROM mark_previous_records
```

---

### **generate_unchanged_records_cte**

**Functionality:**  
Generates a CTE for records that haven't changed and should remain active.

**Arguments:**

| Argument           | Type   | Description                 |
| ------------------ | ------ | --------------------------- |
| `psa_model`        | string | PSA model name              |
| `new_cte`          | string | CTE for new records         |
| `updated_cte`      | string | CTE for updated records     |
| `id_column`        | string | Primary key column          |

**Scope:** Runtime

**Logic:**
- Identifies currently active records that have no updates
- Uses anti-join logic to exclude records that have been updated
- Preserves all fields from the original record
- Maintains continuity for unchanged data

**Usage Example:**
```sql
WITH 
stg_model AS (...),
existing_data AS (...),
new_records AS (...),
updated_records AS (...),
{{ psa.ctes.generate_unchanged_records_cte(
    psa_model='existing_data',
    new_cte='new_records',
    updated_cte='updated_records',
    id_column='customer_id'
) }}

SELECT * FROM unchanged_records
```

---

### **generate_final_cte**

**Functionality:**  
Final union of all record types to produce a complete historical view.

**Arguments:**

| Argument               | Type   | Description                      |
| ---------------------- | ------ | -------------------------------- |
| `mark_previous_cte`    | string | CTE for closing records          |
| `new_cte`              | string | CTE for new records              |
| `updated_cte`          | string | CTE for updated records          |
| `unchanged_cte`        | string | CTE for unchanged records        |

**Scope:** Runtime

**Logic:**
- Combines all record types with UNION ALL:
  1. Newly closed records (previously active, now with end date)
  2. New records (fresh inserts)
  3. Updated records (new versions of existing records)
  4. Unchanged records (still active)
- Creates a complete historical and current view of the data

**Usage Example:**
```sql
WITH 
stg_model AS (...),
existing_data AS (...),
new_records AS (...),
updated_records AS (...),
mark_previous_records AS (...),
unchanged_records AS (...),
{{ psa.ctes.generate_final_cte(
    mark_previous_cte='mark_previous_records',
    new_cte='new_records',
    updated_cte='updated_records',
    unchanged_cte='unchanged_records'
) }}

SELECT * FROM final_output
```

---

## `psa/`

### **change_tracking_versioning_template**

**Functionality:**  
Main template that combines all other macros to implement change tracking and versioning logic for PSA models.

**Arguments:**

| Argument     | Type   | Description                      |
| ------------ | ------ | -------------------------------- |
| `model_name` | string | Name of the PSA model            |
| `stg_model`  | string | Name of the staging model        |
| `id_column`  | string | Primary key column               |

**Scope:** Runtime