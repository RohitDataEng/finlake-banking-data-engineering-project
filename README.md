# FinLake: Banking Data Lakehouse Pipeline

## Project Goal

Build an end-to-end banking data engineering pipeline using AWS S3, Databricks, PySpark, Delta Lake, Snowflake, dbt, and Airflow.

This project simulates a bank receiving raw daily files for customers, loan applications, loan accounts, EMI payments, branches, and delinquency data. The goal is to convert raw files into trusted Bronze, Silver, and Gold layers and publish business-ready reporting tables.


## Raw Source Files

| File | Business Meaning |
|---|---|
| branch_master | Bank branch reference data |
| customers | Customer master data |
| loan_applications | Customer loan application records |
| loan_accounts | Approved loans/accounts |
| emi_payments | Monthly EMI payment transactions |
| delinquency | Overdue and NPA tracking records |

## Planned Architecture

```text
Raw CSV files
    ↓
AWS S3 raw zone
    ↓
Databricks + PySpark
    ↓
Delta Lake Bronze layer
    ↓
Silver cleaning and validation
    ↓
Gold business marts
    ↓
Snowflake reporting schema
    ↓
dbt models and tests
    ↓
Airflow orchestration
```

## Intended Data Quality Scenarios

The raw files intentionally contain a few issues so the project can demonstrate real data engineering work:

- Duplicate customer record
- Duplicate payment record
- Null customer name
- Missing credit score
- Negative loan amount
- Loan linked to non-existing customer
- Payment linked to non-existing loan
- Negative delinquency values

## Target Gold Tables

- gold_customer_loan_summary
- gold_branch_risk_summary
- gold_monthly_collection_summary
- gold_npa_summary
- gold_customer_risk_score

