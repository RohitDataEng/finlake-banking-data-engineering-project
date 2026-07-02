# Source to Target Mapping - Day 1 Draft

## Bronze Layer

Bronze tables should preserve the raw source columns and add audit fields.

Common audit columns:

```text
ingestion_timestamp
source_file_name
load_date
batch_id
```

## Planned Bronze Tables

| Source File | Bronze Table |
|---|---|
| customers | bronze_customers |
| branch_master | bronze_branch_master |
| loan_applications | bronze_loan_applications |
| loan_accounts | bronze_loan_accounts |
| emi_payments | bronze_emi_payments |
| delinquency | bronze_delinquency |

## Planned Silver Tables

| Bronze Table | Silver Table | Main Purpose |
|---|---|---|
| bronze_customers | silver_customers | Clean customer master |
| bronze_branch_master | silver_branch_master | Clean branch reference |
| bronze_loan_applications | silver_loan_applications | Validated applications |
| bronze_loan_accounts | silver_loan_accounts | Validated loan accounts |
| bronze_emi_payments | silver_emi_payments | Validated EMI payments |
| bronze_delinquency | silver_delinquency | Validated delinquency records |

## Planned Gold Tables

| Gold Table | Business Use |
|---|---|
| gold_customer_loan_summary | Customer-level loan exposure and risk |
| gold_branch_risk_summary | Branch-level overdue and NPA risk |
| gold_monthly_collection_summary | Monthly EMI collection performance |
| gold_npa_summary | NPA and overdue summary |
| gold_customer_risk_score | Customer risk classification |
