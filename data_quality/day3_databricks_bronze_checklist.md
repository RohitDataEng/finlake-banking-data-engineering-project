# Day 3 Databricks Bronze Checklist

## Setup

- [ ] Databricks Free Edition account created
- [ ] Notebook imported
- [ ] Catalog and schema identified
- [ ] Volume created, for example `finlake_volume`

## Upload

Upload raw files into the volume so the final paths look like:

- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/raw/branch_master/branch_master_2026_06_30.csv
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/raw/customers/customers_2026_06_30.csv
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/raw/loan_applications/loan_applications_2026_06_30.csv
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/raw/loan_accounts/loan_accounts_2026_06_30.csv
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/raw/emi_payments/emi_payments_2026_06_30.csv
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/raw/delinquency/delinquency_2026_06_30.csv

## Bronze Output

Expected output folders:

- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/processed/bronze/bronze_branch_master/
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/processed/bronze/bronze_customers/
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/processed/bronze/bronze_loan_applications/
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/processed/bronze/bronze_loan_accounts/
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/processed/bronze/bronze_emi_payments/
- [ ] /Volumes/<catalog>/<schema>/<volume>/s3/processed/bronze/bronze_delinquency/

## Audit Columns

Each Bronze table should have:

- [ ] ingestion_timestamp
- [ ] source_file_name
- [ ] load_date
- [ ] pipeline_layer
- [ ] source_system

## Expected Row Counts

| Table | Expected Rows |
|---|---:|
| bronze_branch_master | 15 |
| bronze_customers | 202 |
| bronze_loan_applications | 300 |
| bronze_loan_accounts | Around 214 |
| bronze_emi_payments | Around 1,286 |
| bronze_delinquency | Around 199 |
