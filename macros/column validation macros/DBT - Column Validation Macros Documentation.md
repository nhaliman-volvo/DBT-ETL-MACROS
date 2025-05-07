# DBT - Error Handling Macros

This documentation provides a standardized framework for error detection and data validation in DBT ETL pipelines.
These macros classify data quality issues such as nulls, type mismatches, duplicates, and invalid values — and streamline cleanup logic.

## I. **apply_all_error_checks**

**Functionality:**  
Aggregates all classification error checks (`null`, `datatype`, `duplicate`, `accepted value`) into one unified result set.

**Dependencies:**

- `classify_null_errors`
- `classify_datatype_errors`
- `classify_duplicate_errors`
- `classify_accepted_value_errors`

| Argument      | Type         | Description                    |
| ------------- | ------------ | ------------------------------ |
| `model_ref`   | string / ref | The DBT model to validate      |
| `primary_key` | string       | Name of the primary key column |

**Scope:** Compile-time + Run-time

**Important Functions:**

- `ref()`
- `row_number()`
- Jinja templating (macros, loops, logic)
- `union all`

**Notes:**

- All dependent macros must be defined and registered.
- Error structure should be consistent across all submacros.

**Usage:**

```jinja
{{ apply_all_error_checks('stg_customers', 'customer_id') }}
```

---

## II. **classify_accepted_value_errors**

**Functionality:**  
Flags rows where column values violate the expected set of accepted values.

**Dependencies:**

- `get_accepted_values_map`

| Argument      | Type         | Description                   |
| ------------- | ------------ | ----------------------------- |
| `model_ref`   | string / ref | The model to evaluate         |
| `primary_key` | string       | Primary key used for error ID |

**Scope:** Compile-time + Run-time

**Important Functions:**

- `ref()`
- Jinja conditionals and loops to dynamically generate `IN (...)` clauses

**Notes:**

- Requires predefined accepted value mappings.
- Errors are tagged as `ACCEPTED_VALUE_ERROR`.

**Usage:**

```jinja
{{ classify_accepted_value_errors('stg_orders', 'order_id') }}
```

---

## III. **classify_datatype_errors**

**Functionality:**  
Identifies rows where values cannot be cast to expected data types.

**Dependencies:**

- `get_column_test_metadata_map`
- `validate_datatype`

| Argument      | Type         | Description             |
| ------------- | ------------ | ----------------------- |
| `model_ref`   | string / ref | DBT model reference     |
| `primary_key` | string       | Primary key column name |

**Scope:** Compile-time + Run-time

**Important Functions:**

- `ref()`
- `try_cast()` or equivalent logic in SQL dialect
- `case when` Jinja templating

**Notes:**

- Requires accurate column-type metadata.
- Error type is `DATATYPE_ERROR`.

**Usage:**

```jinja
{{ classify_datatype_errors('stg_payments', 'payment_id') }}
```

---

## IV. **classify_duplicate_errors**

**Functionality:**  
Detects duplicate records using a `row_number()` window function on the primary key.

| Argument      | Type         | Description                        |
| ------------- | ------------ | ---------------------------------- |
| `model_ref`   | string / ref | DBT model or table                 |
| `primary_key` | string       | Column used to identify duplicates |

**Scope:** Run-time only

**Important Functions:**

- `row_number()` (window function)
- `partition by`
- `ref()`

**Notes:**

- Error tagged as `DUPLICATE_ERROR` for rows with `row_number > 1`.

**Usage:**

```jinja
{{ classify_duplicate_errors('stg_products', 'product_id') }}
```

---

## V. **classify_null_errors**

**Functionality:**  
Flags rows with nulls in required columns.

**Dependencies:**

- `get_required_columns`

| Argument      | Type         | Description        |
| ------------- | ------------ | ------------------ |
| `model_ref`   | string / ref | Model to validate  |
| `primary_key` | string       | Primary key column |

**Scope:** Compile-time + Run-time

**Important Functions:**

- `ref()`
- `is null`
- Jinja templating (looping through required columns)

**Notes:**

- Returns error type `NULL_ERROR`.

**Usage:**

```jinja
{{ classify_null_errors('stg_users', 'user_id') }}
```

---

## VI. **combine_error_results**

**Functionality:**  
Merges multiple error result sets into a single output.

| Argument                | Type        | Description                  |
| ----------------------- | ----------- | ---------------------------- |
| `null_errors`           | CTE / table | Null error results           |
| `type_errors`           | CTE / table | Datatype error results       |
| `duplicate_errors`      | CTE / table | Duplicate error results      |
| `accepted_value_errors` | CTE / table | Accepted value error results |

**Scope:** Run-time

**Important Functions:**

- `union all`
- Jinja macro logic

**Notes:**

- All input tables/CTEs must have the same schema.

**Usage:**

```jinja
{{ combine_error_results('null_err', 'type_err', 'dup_err', 'accepted_err') }}
```

---

## VII. **generate_clean_data**

**Functionality:**  
Returns cleaned dataset excluding error records.

| Argument          | Type         | Description                       |
| ----------------- | ------------ | --------------------------------- |
| `model_ref`       | string / ref | Raw source model                  |
| `error_model_ref` | string / ref | Model/table containing error logs |
| `primary_key`     | string       | Primary key to filter errors      |

**Scope:** Run-time only

**Important Functions:**

- `except` or `where not in`
- `ref()`

**Notes:**

- Use after logging all validation errors.

**Usage:**

```jinja
{{ generate_clean_data('stg_products', 'error_log', 'product_id') }}
```

---

## VIII. **validate_datatype**

**Functionality:**  
Generates SQL condition for validating column data types.

| Argument        | Type   | Description                                    |
| --------------- | ------ | ---------------------------------------------- |
| `column`        | string | Column name to validate                        |
| `expected_type` | string | Target datatype (`int`, `float`, `date`, etc.) |

**Scope:** Compile-time only

**Important Functions:**

- Jinja logic to return SQL string for type validation

**Notes:**

- Used within other macros to build filters.

**Usage:**

```jinja
{{ validate_datatype('order_amount', 'float') }}
```

---

---

Place these macros inside your DBT project's `macros/error_handling/` directory.
