# Suggested AWS S3 Folder Structure

Use this structure when you create your S3 bucket.

```text
s3://finlake-banking-project/
    raw/
        branch_master/
        customers/
        loan_applications/
        loan_accounts/
        emi_payments/
        delinquency/

    processed/
        bronze/
        silver/
        gold/

    rejected/
        invalid_records/

    checkpoints/
        autoloader/
```
