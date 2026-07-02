# Day 2 Upload Validation Checklist

Use this checklist after uploading files to S3.

## Bucket Checks

- [ ] Bucket created successfully
- [ ] Bucket name is globally unique
- [ ] Public access is blocked
- [ ] Versioning is enabled
- [ ] Default encryption is enabled

## Folder / Prefix Checks

- [ ] raw/branch_master/ exists
- [ ] raw/customers/ exists
- [ ] raw/loan_applications/ exists
- [ ] raw/loan_accounts/ exists
- [ ] raw/emi_payments/ exists
- [ ] raw/delinquency/ exists
- [ ] processed/bronze/ exists
- [ ] processed/silver/ exists
- [ ] processed/gold/ exists
- [ ] rejected/invalid_records/ exists
- [ ] checkpoints/databricks/ exists
- [ ] checkpoints/airflow/ exists

## File Checks

Expected raw files:

- [ ] branch_master_2026_06_30.csv
- [ ] customers_2026_06_30.csv
- [ ] loan_applications_2026_06_30.csv
- [ ] loan_accounts_2026_06_30.csv
- [ ] emi_payments_2026_06_30.csv
- [ ] delinquency_2026_06_30.csv

## Validation Command

```bash
bash aws/verify_s3_upload.sh
```
