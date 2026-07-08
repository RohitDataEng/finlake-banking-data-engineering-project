# Day 4 Silver Validation Rules

## Customer Rules

| Rule Code | Rule |
|---|---|
| CUST_001 | customer_id is null or blank |
| CUST_002 | duplicate customer_id |
| CUST_003 | customer_name is null or blank |
| CUST_004 | credit_score must be between 300 and 900 |
| CUST_005 | annual_income must be greater than 0 |

## Branch Rules

| Rule Code | Rule |
|---|---|
| BRANCH_001 | branch_id is null or blank |
| BRANCH_002 | duplicate branch_id |
| BRANCH_003 | is_active must be Y or N |

## Loan Application Rules

| Rule Code | Rule |
|---|---|
| APP_001 | application_id is null or blank |
| APP_002 | duplicate application_id |
| APP_003 | customer_id does not exist in silver_customers |
| APP_004 | branch_id does not exist in silver_branch_master |
| APP_005 | requested_amount must be greater than 0 |
| APP_006 | invalid application_status |

## Loan Account Rules

| Rule Code | Rule |
|---|---|
| LOAN_001 | loan_id is null or blank |
| LOAN_002 | duplicate loan_id |
| LOAN_003 | customer_id does not exist in silver_customers |
| LOAN_004 | branch_id does not exist in silver_branch_master |
| LOAN_005 | loan_amount must be greater than 0 |
| LOAN_006 | interest_rate must be greater than 0 |
| LOAN_007 | invalid loan_status |

## EMI Payment Rules

| Rule Code | Rule |
|---|---|
| EMI_001 | payment_id is null or blank |
| EMI_002 | duplicate payment_id |
| EMI_003 | loan_id does not exist in silver_loan_accounts |
| EMI_004 | emi_amount must be greater than 0 |
| EMI_005 | paid_amount must be greater than or equal to 0 |
| EMI_006 | invalid payment_status |
| EMI_007 | MISSED payments should have paid_amount = 0 |

## Delinquency Rules

| Rule Code | Rule |
|---|---|
| DLQ_001 | delinquency_id is null or blank |
| DLQ_002 | loan_id does not exist in silver_loan_accounts |
| DLQ_003 | days_past_due must be greater than or equal to 0 |
| DLQ_004 | overdue_amount must be greater than or equal to 0 |
| DLQ_005 | npa_flag must be Y or N |
| DLQ_006 | npa_flag must be Y when days_past_due >= 90 |
