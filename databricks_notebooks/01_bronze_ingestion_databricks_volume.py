# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 - Proper Databricks Bronze Ingestion
# MAGIC
# MAGIC This notebook uses Databricks + PySpark.
# MAGIC
# MAGIC Source data comes `s3/` folder after uploading it into a Databricks Unity Catalog volume.
# MAGIC
# MAGIC folder:
# MAGIC
# MAGIC ```text
# MAGIC s3/raw/customers/customers_2026_06_30.csv
# MAGIC ```
# MAGIC
# MAGIC Databricks volume path:
# MAGIC
# MAGIC ```text
# MAGIC /Volumes/<catalog>/<schema>/<volume>/s3/raw/customers/customers_2026_06_30.csv
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set Catalog, Schema, and Volume
# MAGIC
# MAGIC Change these values if your Databricks workspace uses different names.
# MAGIC
# MAGIC In many learning workspaces, you can use an existing catalog and schema from Catalog Explorer.
# MAGIC
# MAGIC Recommended volume name:
# MAGIC
# MAGIC ```text
# MAGIC finlake_volume
# MAGIC ```

# COMMAND ----------

CATALOG = "workspace"       # Change if needed
SCHEMA = "default"          # Change if needed
VOLUME = "finlake_volume"   # Change if needed

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BASE_PATH = f"{VOLUME_ROOT}/s3"

RAW_PATH = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/processed/bronze"

print("VOLUME_ROOT:", VOLUME_ROOT)
print("RAW_PATH:", RAW_PATH)
print("BRONZE_PATH:", BRONZE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Volume If Allowed
# MAGIC
# MAGIC If this fails due to permission, create the volume manually from Catalog Explorer and rerun the notebook.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check Uploaded Raw Files
# MAGIC
# MAGIC Before running ingestion, upload files into the raw folders.
# MAGIC
# MAGIC Expected folders:
# MAGIC
# MAGIC ```text
# MAGIC s3/raw/branch_master/
# MAGIC s3/raw/customers/
# MAGIC s3/raw/loan_applications/
# MAGIC s3/raw/loan_accounts/
# MAGIC s3/raw/emi_payments/
# MAGIC s3/raw/delinquency/
# MAGIC ```

# COMMAND ----------

raw_folders = [
    "branch_master",
    "customers",
    "loan_applications",
    "loan_accounts",
    "emi_payments",
    "delinquency",
]

for folder in raw_folders:
    folder_path = f"{RAW_PATH}/{folder}/"
    print(f"Checking {folder_path}")
    try:
        display(dbutils.fs.ls(folder_path))
    except Exception as e:
        print(f"Could not list {folder_path}. Upload files there first. Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define Schemas
# MAGIC
# MAGIC We define schemas manually because production pipelines should not depend blindly on inferred types.

# COMMAND ----------

branch_schema = StructType([
    StructField("branch_id", StringType(), True),
    StructField("branch_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
    StructField("opened_date", StringType(), True),
    StructField("is_active", StringType(), True),
])

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("annual_income", IntegerType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("created_date", StringType(), True),
])

loan_applications_schema = StructType([
    StructField("application_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("loan_type", StringType(), True),
    StructField("requested_amount", IntegerType(), True),
    StructField("application_date", StringType(), True),
    StructField("application_status", StringType(), True),
    StructField("credit_score_at_application", IntegerType(), True),
    StructField("decision_reason", StringType(), True),
])

loan_accounts_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("application_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("loan_type", StringType(), True),
    StructField("loan_amount", IntegerType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("loan_start_date", StringType(), True),
    StructField("loan_status", StringType(), True),
])

emi_payments_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("emi_due_date", StringType(), True),
    StructField("emi_paid_date", StringType(), True),
    StructField("emi_amount", IntegerType(), True),
    StructField("paid_amount", IntegerType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_mode", StringType(), True),
])

delinquency_schema = StructType([
    StructField("delinquency_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("days_past_due", IntegerType(), True),
    StructField("overdue_amount", IntegerType(), True),
    StructField("npa_flag", StringType(), True),
    StructField("report_date", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze Loader Function
# MAGIC
# MAGIC This function reads raw CSV files, adds audit columns, writes Delta output, and registers a Databricks table.

# COMMAND ----------

def load_csv_to_bronze(source_name: str, input_folder: str, schema: StructType, table_name: str):
    input_path = f"{RAW_PATH}/{input_folder}/"
    output_path = f"{BRONZE_PATH}/{table_name}/"
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    print("=" * 80)
    print(f"Source: {source_name}")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Table: {full_table_name}")

    raw_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(input_path)
    )

    bronze_df = (
        raw_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("load_date", F.current_date())
        .withColumn("pipeline_layer", F.lit("BRONZE"))
        .withColumn("source_system", F.lit(source_name))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING DELTA
        LOCATION '{output_path}'
    """)

    count_value = bronze_df.count()
    print(f"Loaded records: {count_value}")

    return count_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load All Bronze Tables

# COMMAND ----------

load_results = {}

load_results["bronze_branch_master"] = load_csv_to_bronze(
    "BRANCH_MASTER", "branch_master", branch_schema, "bronze_branch_master"
)

load_results["bronze_customers"] = load_csv_to_bronze(
    "CUSTOMERS", "customers", customers_schema, "bronze_customers"
)

load_results["bronze_loan_applications"] = load_csv_to_bronze(
    "LOAN_APPLICATIONS", "loan_applications", loan_applications_schema, "bronze_loan_applications"
)

load_results["bronze_loan_accounts"] = load_csv_to_bronze(
    "LOAN_ACCOUNTS", "loan_accounts", loan_accounts_schema, "bronze_loan_accounts"
)

load_results["bronze_emi_payments"] = load_csv_to_bronze(
    "EMI_PAYMENTS", "emi_payments", emi_payments_schema, "bronze_emi_payments"
)

load_results["bronze_delinquency"] = load_csv_to_bronze(
    "DELINQUENCY", "delinquency", delinquency_schema, "bronze_delinquency"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Row Count Summary

# COMMAND ----------

summary_df = spark.createDataFrame(
    [(table, count) for table, count in load_results.items()],
    ["table_name", "row_count"]
)

display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Preview Bronze Tables

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers").limit(10))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_loan_accounts").limit(10))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_emi_payments").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Check Audit Columns

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers")
    .select("customer_id", "customer_name", "ingestion_timestamp", "source_file_name", "load_date", "pipeline_layer", "source_system")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Bronze Learning Note
# MAGIC
# MAGIC Bronze is not the cleaning layer.
# MAGIC
# MAGIC Bad records remain here intentionally:
# MAGIC
# MAGIC - Duplicate customers
# MAGIC - Duplicate payments
# MAGIC - Missing names
# MAGIC - Negative loan amounts
# MAGIC - Orphan keys
# MAGIC
# MAGIC These will be handled in Silver.
