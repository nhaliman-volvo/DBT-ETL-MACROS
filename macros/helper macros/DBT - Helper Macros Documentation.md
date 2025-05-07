# DBT - Helper Macros Documentation

This documentation covers a suite of helper macros to support schema enforcement and metadata-driven transformations in DBT.  
It includes tools to export, cast, and validate column-level metadata from model schema.yml and their associated tests.  These macros bridge runtime and compile-time needs for dynamic column casting, validation, and mapping.  
Use this guide to apply, extend, or integrate helper macros into your DBT workflows.

## I. **export_column_metadata**

**Functionality:**  
Extracts metadata (expected data types, null constraints, uniqueness, accepted values) from model columns and associated tests and inserts them into the `expected_dtypes` table.

**Dependencies:**

- `graph.nodes` (DBT internal context)
- `run_query()`

| Argument | Type | Description                                                       |
| -------- | ---- | ----------------------------------------------------------------- |
| *None*   | –    | This macro has no arguments; it operates on the entire DBT graph. |

**Scope:** Runtime only  
Requires execution via:

```bash
dbt run-operation export_column_metadata
```

**Usage:**

```jinja
{{ export_column_metadata() }}
```

**Example Output:**

Table: `expected_dtypes`

| model_name    | column_name | expected_dtype | is_not_null | is_unique | accepted_values      |
| ------------- | ----------- | -------------- | ----------- | --------- | -------------------- |
| stg_customers | customer_id | INT            | TRUE        | TRUE      |                      |
| stg_orders    | status      | STRING         | FALSE       | FALSE     | 'pending', 'shipped' |

---

## II. **generate_column_casts**

**Functionality:**  
Generates SQL expressions to cast each column to its expected data type using PostgreSQL-style `::TYPE` casting.

**Dependencies:**

- `get_column_test_metadata_map(model_name)`

| Argument     | Type   | Description                                      |
| ------------ | ------ | ------------------------------------------------ |
| `model_name` | string | Name of the model whose columns are being casted |

**Scope:** Compile-time (used inside models or ephemeral models)

**Usage:**

```jinja
{{ generate_column_casts('stg_orders') }}
```

**Example Output:**

```sql
$1::STRING AS order_id,
$2::STRING AS customer_id,
$3::DATE AS order_date
```

---

## III. **generate_column_casts_as_varchar**

**Functionality:**  
Returns column casts with `AS col_name` for extracting raw records (useful for logging or debugging), but all treated as generic text.

**Dependencies:**

- `get_column_test_metadata_map(model_name)`

| Argument     | Type   | Description                             |
| ------------ | ------ | --------------------------------------- |
| `model_name` | string | Name of the model to generate casts for |

**Scope:** Compile-time

**Usage:**

```jinja
{{ generate_column_casts_as_varchar('stg_orders') }}
```

**Example Output:**

```sql
$1 AS order_id,
$2 AS customer_id,
$3 AS order_date
```

---

## IV. **get_column_test_metadata_map**

**Functionality:**  
Fetches the metadata for each column in a model from the `expected_dtypes` table and returns a dictionary-like mapping.

**Dependencies:**

- `run_query()`

| Argument     | Type   | Description                                         |
| ------------ | ------ | --------------------------------------------------- |
| `model_name` | string | Name of the model whose column metadata is required |

**Scope:** Runtime only

**Usage:**

```jinja
{% set metadata = get_column_test_metadata_map('stg_customers') %}
```

**Example Output:**

```json
{
  "customer_id": {
    "expected_dtype": "INT",
    "is_not_null": true,
    "is_unique": true,
    "accepted_values": []
  },
  "status": {
    "expected_dtype": "STRING",
    "is_not_null": false,
    "is_unique": false,
    "accepted_values": ["active", "inactive"]
  }
}
```

---

## V. **get_required_columns**

**Functionality:**  
Returns a list of columns that are marked as `not null`.

**Dependencies:**

- `get_column_test_metadata_map`

| Argument     | Type   | Description       |
| ------------ | ------ | ----------------- |
| `model_name` | string | Name of the model |

**Scope:** Compile-time

**Usage:**

```jinja
{% set required_cols = get_required_columns('stg_orders') %}
```

**Example Output:**

```jinja
['order_id', 'customer_id']
```

---

## VI. **get_accepted_values_map**

**Functionality:**  
Returns a dictionary mapping each column to its list of accepted values.

**Dependencies:**

- `get_column_test_metadata_map`

| Argument     | Type   | Description       |
| ------------ | ------ | ----------------- |
| `model_name` | string | Name of the model |

**Scope:** Compile-time

**Usage:**

```jinja
{% set accepted_map = get_accepted_values_map('stg_orders') %}
```

**Example Output:**

```json
{
  "status": ["pending", "shipped", "cancelled"]
}
```

---

## VII. **get_all_columns**

**Functionality:**  
Fetches all column names for a given model from the `expected_dtypes` table.

**Dependencies:**

- `run_query()`

| Argument     | Type   | Description       |
| ------------ | ------ | ----------------- |
| `model_name` | string | Name of the model |

**Scope:** Runtime

**Usage:**

```jinja
{% set columns = get_all_columns('stg_products') %}
```

**Example Output:**

```jinja
['product_id', 'product_name', 'price']
```
