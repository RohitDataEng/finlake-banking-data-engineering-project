# Data Lake Zones Explained

## Why We Use Zones

A data lake stores many types of data. Without structure, it becomes a data swamp.

Zones help separate raw, cleaned, curated, rejected, and technical processing data.

## Zone 1: Raw Zone

### Path

```text
s3://bucket-name/raw/
```

### Meaning

The raw zone is the landing area for source-system data.

### Rule

Do not modify raw files.

### Interview Explanation

Raw data is preserved so that the pipeline can be replayed, audited, and reconciled if something goes wrong.

## Zone 2: Bronze Zone

### Path

```text
s3://bucket-name/processed/bronze/
```

### Meaning

Bronze is the first lakehouse table layer.

### What changes from raw?

- File is read by Databricks/PySpark
- Data is written as Delta tables later
- Audit columns are added
- Minimal transformation is applied

## Zone 3: Silver Zone

### Path

```text
s3://bucket-name/processed/silver/
```

### Meaning

Silver is clean, validated, and standardized data.

### Examples

- Remove duplicates
- Convert strings to dates
- Validate foreign keys
- Fix inconsistent values
- Separate rejected records

## Zone 4: Gold Zone

### Path

```text
s3://bucket-name/processed/gold/
```

### Meaning

Gold is business-ready data.

### Examples

- Customer loan summary
- Branch risk summary
- Monthly collection summary
- NPA summary

## Zone 5: Rejected Zone

### Path

```text
s3://bucket-name/rejected/invalid_records/
```

### Meaning

This zone stores records that failed validation rules.

### Why It Matters

Rejected records are important because production pipelines should not silently drop bad data.

## Zone 6: Checkpoint Zone

### Path

```text
s3://bucket-name/checkpoints/
```

### Meaning

Checkpoint data helps streaming or incremental jobs track processing progress.

### Rule

Do not manually edit checkpoint files.
