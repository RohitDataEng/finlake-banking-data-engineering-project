# Suggested AWS S3 Folder Structure

Use this structure when you create your S3 bucket.

```text
s3://finlake-banking-project-rohit/
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

## Naming Advice

Use a globally unique bucket name. Example:

```text
finlake-banking-project-rohit-2026
```

Do not store secret keys, passwords, or personal data in GitHub.
