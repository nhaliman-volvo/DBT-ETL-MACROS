# PSA Macros Usage Guide

## Overview

This package provides modular macros for implementing Persistent Staging Area (PSA) patterns in dbt projects. The main purpose is to track changes to data over time using a standardized approach that includes versioning, validity periods, and change detection.

## Installation

1. Copy the `macros/psa` directory to your dbt project
2. Reference the macros in your models

## Main Components

The package has been modularized into several key components:

1. **Configuration** - Handles dbt model configuration
2. **System Variables** - Manages system-specific tracking variables
3. **Column Management** - Processes columns from source and target tables
4. **CTE Generation** - Creates SQL components for different data states
5. **Main Template** - Orchestrates all components

## Usage Example

Here's how to use the main template in your model:

```sql
-- in models/marts/dim_customers.sql
{{
  psa.change_tracking_versioning_template(
    stg_model_name='stg_customers'
  )
}}
```

## Individual Macros

If you need to use individual components, you can import them directly:

```sql
-- Get system variables
{% set system_vars = psa.get_system_variables() %}

-- Get column information
{% set column_info = psa.get_relation_columns(this) %}
```

## Extending the Package

To add new functionality:

1. Create additional macros in the appropriate subdirectory
2. Update `_index.sql` to import your new macros
3. Reference them in your custom implementations

## System Requirements

- dbt version 1.0.0 or higher
- A functioning `get_and_increment_system_variables()` macro in your project
- A functioning `update_system_variables()` macro in your project

## Key Features

- **Modularity**: Each component has a single responsibility
- **Reusability**: Components can be used independently
- **Maintainability**: Easy to update specific functionality
- **Type Safety**: Uses dbt's built-in adapters for database compatibility
- **Versioning**: Tracks changes to records over time
- **Schema Evolution**: Handles new columns with `on_schema_change='append_new_columns'`