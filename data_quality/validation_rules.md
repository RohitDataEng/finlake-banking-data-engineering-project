# Data Quality and Validation Rules

## Customer Rules

| Rule ID | Rule |
|---|---|
| CUST_001 | customer_id must not be null |
| CUST_002 | customer_id must be unique |
| CUST_003 | customer_name must not be null |
| CUST_004 | credit_score must be between 300 and 900 |
| CUST_005 | annual_income must be greater than 0 |

## Loan Account Rules

| Rule ID | Rule |
|---|---|
| LOAN_001 | loan_id must not be null |
| LOAN_002 | loan_id must be unique |
| LOAN_003 | customer_id must exist in silver_customers |
| LOAN_004 | branch_id must exist in branch_master |
| LOAN_005 | loan_amount must be greater than 0 |
| LOAN_006 | interest_rate must be greater than 0 |
| LOAN_007 | loan_status must be ACTIVE, CLOSED, or DEFAULTED |

## EMI Payment Rules

| Rule ID | Rule |
|---|---|
| EMI_001 | payment_id must not be null |
| EMI_002 | payment_id must be unique |
| EMI_003 | loan_id must exist in silver_loan_accounts |
| EMI_004 | emi_amount must be greater than 0 |
| EMI_005 | paid_amount must be greater than or equal to 0 |
| EMI_006 | payment_status must be PAID, LATE, PARTIAL, or MISSED |
| EMI_007 | If payment_status = MISSED, paid_amount should be 0 |

## Delinquency Rules

| Rule ID | Rule |
|---|---|
| DLQ_001 | delinquency_id must not be null |
| DLQ_002 | loan_id must exist in silver_loan_accounts |
| DLQ_003 | days_past_due must be greater than or equal to 0 |
| DLQ_004 | overdue_amount must be greater than or equal to 0 |
| DLQ_005 | npa_flag must be Y when days_past_due >= 90 |
