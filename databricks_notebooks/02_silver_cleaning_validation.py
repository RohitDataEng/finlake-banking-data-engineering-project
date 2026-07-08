# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Cleaning, Validation, and Rejected Records
# MAGIC
# MAGIC Bronze = raw preservation.  
# MAGIC Silver = cleaned, validated, trusted data.
# MAGIC
# MAGIC This notebook reads Day 3 Bronze managed tables and creates Silver managed tables plus a rejected-records table.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"

print("Catalog:", CATALOG)
print("Schema:", SCHEMA)

# COMMAND ----------

def table_name(layer: str, name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{layer}_{name}"

def add_silver_audit_columns(df):
    return (
        df
        .withColumn("silver_processed_timestamp", F.current_timestamp())
        .withColumn("pipeline_layer", F.lit("SILVER"))
    )

def make_rejected_df(df, source_table: str, record_id_col: str, rule_code: str, rejection_reason: str):
    return (
        df
        .withColumn("source_table", F.lit(source_table))
        .withColumn("record_id", F.col(record_id_col).cast("string"))
        .withColumn("rule_code", F.lit(rule_code))
        .withColumn("rejection_reason", F.lit(rejection_reason))
        .withColumn("rejected_timestamp", F.current_timestamp())
        .withColumn("raw_record_json", F.to_json(F.struct(*[F.col(c) for c in df.columns])))
        .select("source_table", "record_id", "rule_code", "rejection_reason", "rejected_timestamp", "raw_record_json")
    )

def save_managed_table(df, full_table_name: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    print(f"Saved {full_table_name}: {spark.table(full_table_name).count()} records")

def union_all_by_name(dfs):
    return reduce(lambda a, b: a.unionByName(b), dfs)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Bronze Tables

# COMMAND ----------

bronze_branch_master = spark.table(table_name("bronze", "branch_master"))
bronze_customers = spark.table(table_name("bronze", "customers"))
bronze_loan_applications = spark.table(table_name("bronze", "loan_applications"))
bronze_loan_accounts = spark.table(table_name("bronze", "loan_accounts"))
bronze_emi_payments = spark.table(table_name("bronze", "emi_payments"))
bronze_delinquency = spark.table(table_name("bronze", "delinquency"))

print("Bronze tables loaded successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Branch Master

# COMMAND ----------

branch_df = (
    bronze_branch_master
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("branch_name", F.trim(F.col("branch_name")))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.initcap(F.trim(F.col("state"))))
    .withColumn("region", F.initcap(F.trim(F.col("region"))))
    .withColumn("is_active", F.upper(F.trim(F.col("is_active"))))
    .withColumn("opened_date", F.to_date("opened_date"))
)

branch_window = Window.partitionBy("branch_id").orderBy(F.col("ingestion_timestamp").desc())
branch_ranked = branch_df.withColumn("rn", F.row_number().over(branch_window))

branch_rejects = [
    make_rejected_df(branch_ranked.filter(F.col("branch_id").isNull() | (F.col("branch_id") == "")), "bronze_branch_master", "branch_id", "BRANCH_001", "branch_id is null or blank"),
    make_rejected_df(branch_ranked.filter(F.col("rn") > 1), "bronze_branch_master", "branch_id", "BRANCH_002", "duplicate branch_id"),
    make_rejected_df(branch_ranked.filter(~F.col("is_active").isin("Y", "N")), "bronze_branch_master", "branch_id", "BRANCH_003", "is_active must be Y or N")
]

silver_branch_master = (
    branch_ranked
    .filter(F.col("branch_id").isNotNull() & (F.col("branch_id") != ""))
    .filter(F.col("rn") == 1)
    .filter(F.col("is_active").isin("Y", "N"))
    .drop("rn")
)
silver_branch_master = add_silver_audit_columns(silver_branch_master)
save_managed_table(silver_branch_master, table_name("silver", "branch_master"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Customers

# COMMAND ----------

customers_df = (
    bronze_customers
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.initcap(F.trim(F.col("state"))))
    .withColumn("employment_type", F.upper(F.trim(F.col("employment_type"))))
    .withColumn("dob", F.to_date("dob"))
    .withColumn("created_date", F.to_date("created_date"))
)
customer_window = Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())
customers_ranked = customers_df.withColumn("rn", F.row_number().over(customer_window))

customer_rejects = [
    make_rejected_df(customers_ranked.filter(F.col("customer_id").isNull() | (F.col("customer_id") == "")), "bronze_customers", "customer_id", "CUST_001", "customer_id is null or blank"),
    make_rejected_df(customers_ranked.filter(F.col("rn") > 1), "bronze_customers", "customer_id", "CUST_002", "duplicate customer_id"),
    make_rejected_df(customers_ranked.filter(F.col("customer_name").isNull() | (F.col("customer_name") == "")), "bronze_customers", "customer_id", "CUST_003", "customer_name is null or blank"),
    make_rejected_df(customers_ranked.filter((F.col("credit_score") < 300) | (F.col("credit_score") > 900) | F.col("credit_score").isNull()), "bronze_customers", "customer_id", "CUST_004", "credit_score must be between 300 and 900"),
    make_rejected_df(customers_ranked.filter((F.col("annual_income") <= 0) | F.col("annual_income").isNull()), "bronze_customers", "customer_id", "CUST_005", "annual_income must be greater than 0")
]

silver_customers = (
    customers_ranked
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
    .filter(F.col("rn") == 1)
    .filter(F.col("customer_name").isNotNull() & (F.col("customer_name") != ""))
    .filter(F.col("credit_score").between(300, 900))
    .filter(F.col("annual_income") > 0)
    .drop("rn")
)
silver_customers = add_silver_audit_columns(silver_customers)
save_managed_table(silver_customers, table_name("silver", "customers"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Loan Applications

# COMMAND ----------

valid_customers = spark.table(table_name("silver", "customers")).select("customer_id").distinct()
valid_branches = spark.table(table_name("silver", "branch_master")).select("branch_id").distinct()

loan_app_df = (
    bronze_loan_applications
    .withColumn("application_id", F.trim(F.col("application_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("loan_type", F.upper(F.trim(F.col("loan_type"))))
    .withColumn("application_status", F.upper(F.trim(F.col("application_status"))))
    .withColumn("application_date", F.to_date("application_date"))
)
app_window = Window.partitionBy("application_id").orderBy(F.col("ingestion_timestamp").desc())
loan_app_ranked = loan_app_df.withColumn("rn", F.row_number().over(app_window))
loan_app_with_refs = (
    loan_app_ranked
    .join(valid_customers.withColumn("customer_exists", F.lit(1)), "customer_id", "left")
    .join(valid_branches.withColumn("branch_exists", F.lit(1)), "branch_id", "left")
)
loan_app_rejects = [
    make_rejected_df(loan_app_with_refs.filter(F.col("application_id").isNull() | (F.col("application_id") == "")), "bronze_loan_applications", "application_id", "APP_001", "application_id is null or blank"),
    make_rejected_df(loan_app_with_refs.filter(F.col("rn") > 1), "bronze_loan_applications", "application_id", "APP_002", "duplicate application_id"),
    make_rejected_df(loan_app_with_refs.filter(F.col("customer_exists").isNull()), "bronze_loan_applications", "application_id", "APP_003", "customer_id does not exist in silver_customers"),
    make_rejected_df(loan_app_with_refs.filter(F.col("branch_exists").isNull()), "bronze_loan_applications", "application_id", "APP_004", "branch_id does not exist in silver_branch_master"),
    make_rejected_df(loan_app_with_refs.filter((F.col("requested_amount") <= 0) | F.col("requested_amount").isNull()), "bronze_loan_applications", "application_id", "APP_005", "requested_amount must be greater than 0"),
    make_rejected_df(loan_app_with_refs.filter(~F.col("application_status").isin("APPROVED", "REJECTED", "PENDING")), "bronze_loan_applications", "application_id", "APP_006", "invalid application_status")
]
silver_loan_applications = (
    loan_app_with_refs
    .filter(F.col("application_id").isNotNull() & (F.col("application_id") != ""))
    .filter(F.col("rn") == 1)
    .filter(F.col("customer_exists").isNotNull())
    .filter(F.col("branch_exists").isNotNull())
    .filter(F.col("requested_amount") > 0)
    .filter(F.col("application_status").isin("APPROVED", "REJECTED", "PENDING"))
    .drop("rn", "customer_exists", "branch_exists")
)
silver_loan_applications = add_silver_audit_columns(silver_loan_applications)
save_managed_table(silver_loan_applications, table_name("silver", "loan_applications"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Loan Accounts

# COMMAND ----------

loan_df = (
    bronze_loan_accounts
    .withColumn("loan_id", F.trim(F.col("loan_id")))
    .withColumn("application_id", F.trim(F.col("application_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("loan_type", F.upper(F.trim(F.col("loan_type"))))
    .withColumn("loan_status", F.upper(F.trim(F.col("loan_status"))))
    .withColumn("loan_start_date", F.to_date("loan_start_date"))
)
loan_window = Window.partitionBy("loan_id").orderBy(F.col("ingestion_timestamp").desc())
loan_ranked = loan_df.withColumn("rn", F.row_number().over(loan_window))
loan_with_refs = (
    loan_ranked
    .join(valid_customers.withColumn("customer_exists", F.lit(1)), "customer_id", "left")
    .join(valid_branches.withColumn("branch_exists", F.lit(1)), "branch_id", "left")
)
loan_rejects = [
    make_rejected_df(loan_with_refs.filter(F.col("loan_id").isNull() | (F.col("loan_id") == "")), "bronze_loan_accounts", "loan_id", "LOAN_001", "loan_id is null or blank"),
    make_rejected_df(loan_with_refs.filter(F.col("rn") > 1), "bronze_loan_accounts", "loan_id", "LOAN_002", "duplicate loan_id"),
    make_rejected_df(loan_with_refs.filter(F.col("customer_exists").isNull()), "bronze_loan_accounts", "loan_id", "LOAN_003", "customer_id does not exist in silver_customers"),
    make_rejected_df(loan_with_refs.filter(F.col("branch_exists").isNull()), "bronze_loan_accounts", "loan_id", "LOAN_004", "branch_id does not exist in silver_branch_master"),
    make_rejected_df(loan_with_refs.filter((F.col("loan_amount") <= 0) | F.col("loan_amount").isNull()), "bronze_loan_accounts", "loan_id", "LOAN_005", "loan_amount must be greater than 0"),
    make_rejected_df(loan_with_refs.filter((F.col("interest_rate") <= 0) | F.col("interest_rate").isNull()), "bronze_loan_accounts", "loan_id", "LOAN_006", "interest_rate must be greater than 0"),
    make_rejected_df(loan_with_refs.filter(~F.col("loan_status").isin("ACTIVE", "CLOSED", "DEFAULTED")), "bronze_loan_accounts", "loan_id", "LOAN_007", "invalid loan_status")
]
silver_loan_accounts = (
    loan_with_refs
    .filter(F.col("loan_id").isNotNull() & (F.col("loan_id") != ""))
    .filter(F.col("rn") == 1)
    .filter(F.col("customer_exists").isNotNull())
    .filter(F.col("branch_exists").isNotNull())
    .filter(F.col("loan_amount") > 0)
    .filter(F.col("interest_rate") > 0)
    .filter(F.col("loan_status").isin("ACTIVE", "CLOSED", "DEFAULTED"))
    .drop("rn", "customer_exists", "branch_exists")
)
silver_loan_accounts = add_silver_audit_columns(silver_loan_accounts)
save_managed_table(silver_loan_accounts, table_name("silver", "loan_accounts"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver EMI Payments

# COMMAND ----------

valid_loans = spark.table(table_name("silver", "loan_accounts")).select("loan_id").distinct()
payments_df = (
    bronze_emi_payments
    .withColumn("payment_id", F.trim(F.col("payment_id")))
    .withColumn("loan_id", F.trim(F.col("loan_id")))
    .withColumn("payment_status", F.upper(F.trim(F.col("payment_status"))))
    .withColumn("payment_mode", F.upper(F.trim(F.col("payment_mode"))))
    .withColumn("emi_due_date", F.to_date("emi_due_date"))
    .withColumn("emi_paid_date", F.to_date("emi_paid_date"))
)
payment_window = Window.partitionBy("payment_id").orderBy(F.col("ingestion_timestamp").desc())
payments_ranked = payments_df.withColumn("rn", F.row_number().over(payment_window))
payments_with_refs = payments_ranked.join(valid_loans.withColumn("loan_exists", F.lit(1)), "loan_id", "left")
payment_rejects = [
    make_rejected_df(payments_with_refs.filter(F.col("payment_id").isNull() | (F.col("payment_id") == "")), "bronze_emi_payments", "payment_id", "EMI_001", "payment_id is null or blank"),
    make_rejected_df(payments_with_refs.filter(F.col("rn") > 1), "bronze_emi_payments", "payment_id", "EMI_002", "duplicate payment_id"),
    make_rejected_df(payments_with_refs.filter(F.col("loan_exists").isNull()), "bronze_emi_payments", "payment_id", "EMI_003", "loan_id does not exist in silver_loan_accounts"),
    make_rejected_df(payments_with_refs.filter((F.col("emi_amount") <= 0) | F.col("emi_amount").isNull()), "bronze_emi_payments", "payment_id", "EMI_004", "emi_amount must be greater than 0"),
    make_rejected_df(payments_with_refs.filter((F.col("paid_amount") < 0) | F.col("paid_amount").isNull()), "bronze_emi_payments", "payment_id", "EMI_005", "paid_amount must be greater than or equal to 0"),
    make_rejected_df(payments_with_refs.filter(~F.col("payment_status").isin("PAID", "LATE", "PARTIAL", "MISSED")), "bronze_emi_payments", "payment_id", "EMI_006", "invalid payment_status"),
    make_rejected_df(payments_with_refs.filter((F.col("payment_status") == "MISSED") & (F.col("paid_amount") != 0)), "bronze_emi_payments", "payment_id", "EMI_007", "MISSED payments should have paid_amount = 0")
]
silver_emi_payments = (
    payments_with_refs
    .filter(F.col("payment_id").isNotNull() & (F.col("payment_id") != ""))
    .filter(F.col("rn") == 1)
    .filter(F.col("loan_exists").isNotNull())
    .filter(F.col("emi_amount") > 0)
    .filter(F.col("paid_amount") >= 0)
    .filter(F.col("payment_status").isin("PAID", "LATE", "PARTIAL", "MISSED"))
    .filter(~((F.col("payment_status") == "MISSED") & (F.col("paid_amount") != 0)))
    .drop("rn", "loan_exists")
)
silver_emi_payments = add_silver_audit_columns(silver_emi_payments)
save_managed_table(silver_emi_payments, table_name("silver", "emi_payments"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Delinquency

# COMMAND ----------

delinq_df = (
    bronze_delinquency
    .withColumn("delinquency_id", F.trim(F.col("delinquency_id")))
    .withColumn("loan_id", F.trim(F.col("loan_id")))
    .withColumn("npa_flag", F.upper(F.trim(F.col("npa_flag"))))
    .withColumn("report_date", F.to_date("report_date"))
)
delinq_with_refs = delinq_df.join(valid_loans.withColumn("loan_exists", F.lit(1)), "loan_id", "left")
delinq_rejects = [
    make_rejected_df(delinq_with_refs.filter(F.col("delinquency_id").isNull() | (F.col("delinquency_id") == "")), "bronze_delinquency", "delinquency_id", "DLQ_001", "delinquency_id is null or blank"),
    make_rejected_df(delinq_with_refs.filter(F.col("loan_exists").isNull()), "bronze_delinquency", "delinquency_id", "DLQ_002", "loan_id does not exist in silver_loan_accounts"),
    make_rejected_df(delinq_with_refs.filter((F.col("days_past_due") < 0) | F.col("days_past_due").isNull()), "bronze_delinquency", "delinquency_id", "DLQ_003", "days_past_due must be greater than or equal to 0"),
    make_rejected_df(delinq_with_refs.filter((F.col("overdue_amount") < 0) | F.col("overdue_amount").isNull()), "bronze_delinquency", "delinquency_id", "DLQ_004", "overdue_amount must be greater than or equal to 0"),
    make_rejected_df(delinq_with_refs.filter(~F.col("npa_flag").isin("Y", "N")), "bronze_delinquency", "delinquency_id", "DLQ_005", "npa_flag must be Y or N"),
    make_rejected_df(delinq_with_refs.filter((F.col("days_past_due") >= 90) & (F.col("npa_flag") != "Y")), "bronze_delinquency", "delinquency_id", "DLQ_006", "npa_flag must be Y when days_past_due >= 90")
]
silver_delinquency = (
    delinq_with_refs
    .filter(F.col("delinquency_id").isNotNull() & (F.col("delinquency_id") != ""))
    .filter(F.col("loan_exists").isNotNull())
    .filter(F.col("days_past_due") >= 0)
    .filter(F.col("overdue_amount") >= 0)
    .filter(F.col("npa_flag").isin("Y", "N"))
    .filter(~((F.col("days_past_due") >= 90) & (F.col("npa_flag") != "Y")))
    .drop("loan_exists")
)
silver_delinquency = add_silver_audit_columns(silver_delinquency)
save_managed_table(silver_delinquency, table_name("silver", "delinquency"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Consolidated Rejected Records Table

# COMMAND ----------

all_reject_dfs = branch_rejects + customer_rejects + loan_app_rejects + loan_rejects + payment_rejects + delinq_rejects
non_empty_rejects = [df for df in all_reject_dfs if df.count() > 0]

if non_empty_rejects:
    rejected_records = union_all_by_name(non_empty_rejects)
else:
    rejected_records = spark.createDataFrame(
        [],
        "source_table string, record_id string, rule_code string, rejection_reason string, rejected_timestamp timestamp, raw_record_json string"
    )

save_managed_table(rejected_records, f"{CATALOG}.{SCHEMA}.silver_rejected_records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Row Count Summary

# COMMAND ----------

silver_tables = [
    "silver_branch_master",
    "silver_customers",
    "silver_loan_applications",
    "silver_loan_accounts",
    "silver_emi_payments",
    "silver_delinquency",
    "silver_rejected_records"
]

summary_rows = []
for t in silver_tables:
    full_name = f"{CATALOG}.{SCHEMA}.{t}"
    summary_rows.append((t, spark.table(full_name).count()))
summary_df = spark.createDataFrame(summary_rows, ["table_name", "row_count"])
display(summary_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Rejected Records Summary by Rule

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SCHEMA}.silver_rejected_records")
    .groupBy("source_table", "rule_code", "rejection_reason")
    .count()
    .orderBy("source_table", "rule_code")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preview Silver Tables

# COMMAND ----------

display(spark.table(table_name("silver", "customers")).limit(10))

# COMMAND ----------

display(spark.table(table_name("silver", "loan_accounts")).limit(10))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.silver_rejected_records").limit(20))
