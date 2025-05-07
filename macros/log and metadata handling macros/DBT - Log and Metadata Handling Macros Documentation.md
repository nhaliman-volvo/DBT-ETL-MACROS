# DBT - Log and Metadata Handling Macros Documentation

This documentation provides a comprehensive reference for DBT macros focused on logging and metadata handling. It includes macros to add metadata columns, maintating log table and generate unique identifiers for logging.  
Use this guide to understand macro usage, dependencies, scope, and expected outputs.

## I. **add_batch_id_column**

**Functionality:**  
Adds a batch ID column to a model table and populates it by generating a SHA1 hash of concatenated filenames.

**Dependencies:**

- `ref()`
- `run_query()`

| Argument       | Type   | Description                                 |
| -------------- | ------ | ------------------------------------------- |
| `source_model` | string | Name of the model to add batch ID column to |

**Scope:** Runtime only  
Requires execution via DBT operation or pre/post hook.

**Usage:**

```jinja
{{ add_batch_id_column('load_health_data') }}
```

**Example Output:**  
Alters the specified table to add a 'batch_id' column and updates it with a SHA1 hash value:

```sql
ALTER TABLE my_model_table ADD COLUMN IF NOT EXISTS batch_id STRING;
UPDATE my_model_table SET batch_id = 'a1b2c3d4e5f6...';
```

---

## II. **add_metadata_columns**

**Functionality:**  
Generates SQL expressions for standard metadata columns to track file source and timestamps.

**Dependencies:**

- None (Uses Snowflake functions)

| Argument | Type | Description                 |
| -------- | ---- | --------------------------- |
| *None*   | –    | This macro has no arguments |

**Scope:** Compile-time (used inside models)

**Usage:**

```jinja
SELECT 
    field1,
    field2,
    {{ add_metadata_columns() }}
FROM source_table
```

**Example Output:**

```sql
METADATA$FILENAME AS _File_Name,
CURRENT_TIMESTAMP() AS _ONBOARDED_TIME,
CURRENT_TIMESTAMP() AS _INSERTED_TIME
```

---

## III. **create_logging_table_if_not_exists**

**Functionality:**  
Creates a logging table if it doesn't already exist to track model execution details.

**Dependencies:**

- `run_query()`
- `adapter.quote()`

| Argument    | Type   | Description                        |
| ----------- | ------ | ---------------------------------- |
| `log_table` | string | Full table reference for log table |

**Scope:** Runtime only

**Usage:**

```jinja
{{ create_logging_table_if_not_exists('MYDB.MYSCHEMA.DBT_LOGGING_TABLE') }}
```

**Example Output:**  
Executes a CREATE TABLE IF NOT EXISTS statement for the logging table:

```sql
CREATE TABLE IF NOT EXISTS MYDB.MYSCHEMA.DBT_LOGGING_TABLE (
    key STRING,
    source_key STRING,
    snowflake_stg STRING,
    "table" STRING,
    role STRING,
    timestamp TIMESTAMP,
    status STRING,
    db STRING,
    schema STRING,
    layer STRING,
    batch_id STRING,
    inserted_record_count INT
);
```

---

## IV. **generate_batch_id**

**Functionality:**  
Generates a unique batch ID by hashing concatenated filenames from a source model.

**Dependencies:**

- `ref()`
- `run_query()`
- `exceptions.raise_compiler_error()`

| Argument            | Type   | Description                                 |
| ------------------- | ------ | ------------------------------------------- |
| `source_model_name` | string | Name of the model to generate batch ID from |

**Scope:** Runtime

**Usage:**

```jinja
{% set batch_id = generate_batch_id('load_health_data') %}
```

**Example Output:**

```
"a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0"
```

---

## V. **generate_logging_key**

**Functionality:**  
Retrieves the current logging key from a dedicated key generator table.

**Dependencies:**

- `run_query()`
- `exceptions.raise()`

| Argument | Type | Description                 |
| -------- | ---- | --------------------------- |
| *None*   | –    | This macro has no arguments |

**Scope:** Runtime

**Usage:**

```jinja
{% set key = generate_logging_key() %}
```

**Example Output:**

```
"12345"
```

---

## VI. **generate_source_key**

**Functionality:**  
Generates a hash of the source stage name to use as an identifier.

**Dependencies:**

- `local_md5()` (DBT internal function)

| Argument       | Type   | Description                      |
| -------------- | ------ | -------------------------------- |
| `source_stage` | string | Name of the source stage to hash |

**Scope:** Compile-time

**Usage:**

```jinja
{% set source_key = generate_source_key('my_snowflake_stage') %}
```

**Example Output:**

```
"b5f1688cb7cd0b9a9c9457efb3c3050c"
```

---

## VII. **log_model_run_pending**

**Functionality:**  
Creates a log entry indicating that a model run is pending. Records metadata about the model, environment, and batch.

**Dependencies:**

- `generate_logging_key()`
- `generate_source_key()`
- `generate_batch_id()`
- `run_query()`
- `model.name`
- `target` (DBT context)
- `env_var()`

| Argument | Type | Description                 |
| -------- | ---- | --------------------------- |
| *None*   | –    | This macro has no arguments |

**Scope:** Runtime (used as pre-hook in models)

**Usage:**

```jinja
{{ config(
    pre_hook="{{ log_model_run_pending() }}"
)}}
```

**Example Output:**  
Executes an INSERT statement into the logging table:

```sql
INSERT INTO MYDB.MYSCHEMA.DBT_LOGGING_TABLE (
    key, source_key, snowflake_stg, "table",
    role, timestamp, status, db, schema, layer, batch_id, inserted_record_count
)
VALUES (
    '12345',
    'b5f1688cb7cd0b9a9c9457efb3c3050c',
    'MY_SNOWFLAKE_STAGE',
    'dim_customers',
    'TRANSFORMER_ROLE',
    CURRENT_TIMESTAMP(),
    'PENDING',
    'MYDB',
    'MYSCHEMA',
    'DIM',
    'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0',
    0
);
```

---

## VIII. **update_logging_key**

**Functionality:**  
Increments the logging key in the key generator table after a log entry is completed.

**Dependencies:**

- `run_query()`
- `target` (DBT context)

| Argument | Type | Description                 |
| -------- | ---- | --------------------------- |
| *None*   | –    | This macro has no arguments |

**Scope:** Runtime

**Usage:**

```jinja
{{ update_logging_key() }}
```

**Example Output:**  
Executes an UPDATE statement on the key generator table:

```sql
UPDATE MYDB.MYSCHEMA.KEY_ID_GENERATOR
SET "VALUE" = "VALUE" + 1
WHERE "VARIABLE" = 'KEY'
```

---

## IX. **update_logging_status**

**Functionality:**  
Updates a log entry after model execution with record count and success/failure status.

**Dependencies:**

- `generate_logging_key()`
- `run_query()`
- `adapter.quote()`
- `model.name`
- `update_logging_key()`
- `this` (DBT context)

| Argument | Type | Description                 |
| -------- | ---- | --------------------------- |
| *None*   | –    | This macro has no arguments |

**Scope:** Runtime (used as post-hook in models)

**Usage:**

```jinja
{{ config(
    post_hook="{{ update_logging_status() }}"
)}}
```

**Example Output:**  
Executes an UPDATE statement on the logging table:

```sql
UPDATE MYDB.MYSCHEMA.DBT_LOGGING_TABLE AS log
SET inserted_record_count = (
    SELECT COUNT(*) FROM dim_customers
),
status = CASE 
    WHEN EXISTS (SELECT 1 FROM dim_customers) THEN 'SUCCESS'
    ELSE 'FAILURE'
END,
timestamp = CURRENT_TIMESTAMP()
WHERE log.key = '12345' AND log."table" = 'dim_customers'
```
