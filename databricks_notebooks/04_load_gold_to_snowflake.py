# Databricks notebook source
# MAGIC %md
# MAGIC # Load Gold Tables into Snowflake
# MAGIC
# MAGIC ## Goal
# MAGIC
# MAGIC Load Databricks Gold tables into Snowflake analytics tables.
# MAGIC
# MAGIC Databricks Gold tables created on Day 5:
# MAGIC
# MAGIC - gold_customer_loan_summary
# MAGIC - gold_branch_performance_summary
# MAGIC - gold_monthly_collection_summary
# MAGIC - gold_loan_default_risk_summary
# MAGIC - gold_portfolio_kpis
# MAGIC
# MAGIC Snowflake target tables:
# MAGIC
# MAGIC - FINLAKE_ANALYTICS.GOLD.GOLD_CUSTOMER_LOAN_SUMMARY
# MAGIC - FINLAKE_ANALYTICS.GOLD.GOLD_BRANCH_PERFORMANCE_SUMMARY
# MAGIC - FINLAKE_ANALYTICS.GOLD.GOLD_MONTHLY_COLLECTION_SUMMARY
# MAGIC - FINLAKE_ANALYTICS.GOLD.GOLD_LOAN_DEFAULT_RISK_SUMMARY
# MAGIC - FINLAKE_ANALYTICS.GOLD.GOLD_PORTFOLIO_KPIS

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Databricks Configuration

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"

GOLD_TABLES = [
    "gold_customer_loan_summary",
    "gold_branch_performance_summary",
    "gold_monthly_collection_summary",
    "gold_loan_default_risk_summary",
    "gold_portfolio_kpis"
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Snowflake Configuration
# MAGIC
# MAGIC Use Databricks secrets if available. Do not hardcode passwords in GitHub.
# MAGIC
# MAGIC First create a secret scope/key, or replace this block temporarily while testing and remove credentials before committing.

# COMMAND ----------

# Recommended: store password in Databricks Secret Scope.
# Example secret reference:
# SNOWFLAKE_PASSWORD = dbutils.secrets.get(scope="finlake", key="snowflake_password")

SNOWFLAKE_USER = "USERNAME"
SNOWFLAKE_PASSWORD = dbutils.secrets.get(scope="scopename", key="keyname")
SNOWFLAKE_ACCOUNT = "account_id"  # example: abc12345.ap-south-1.aws
SNOWFLAKE_WAREHOUSE = "FINLAKE_WH"
SNOWFLAKE_DATABASE = "FINLAKE_ANALYTICS"
SNOWFLAKE_SCHEMA = "GOLD"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"  # use a project-specific role if you created one

sf_options = {
    "host": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
    "sfUser": SNOWFLAKE_USER,
    "sfPassword": SNOWFLAKE_PASSWORD,
    "sfDatabase": SNOWFLAKE_DATABASE,
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfWarehouse": SNOWFLAKE_WAREHOUSE,
    "sfRole": SNOWFLAKE_ROLE
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Create Snowflake Database and Schema
# MAGIC
# MAGIC Run this SQL directly in Snowflake before running the load section.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE DATABASE IF NOT EXISTS FINLAKE_ANALYTICS;
# MAGIC CREATE SCHEMA IF NOT EXISTS FINLAKE_ANALYTICS.GOLD;
# MAGIC CREATE WAREHOUSE IF NOT EXISTS FINLAKE_WH
# MAGIC   WAREHOUSE_SIZE = 'XSMALL'
# MAGIC   AUTO_SUSPEND = 60
# MAGIC   AUTO_RESUME = TRUE;
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Helper Functions

# COMMAND ----------

def databricks_table(table_name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table_name}"

def snowflake_table_name(table_name: str) -> str:
    return table_name.upper()

def load_table_to_snowflake(table_name: str):
    source_table = databricks_table(table_name)
    target_table = snowflake_table_name(table_name)

    print("=" * 80)
    print(f"Reading Databricks table: {source_table}")
    print(f"Writing Snowflake table: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_table}")

    df = spark.table(source_table)
    row_count = df.count()
    print(f"Source row count: {row_count}")

    (
        df.write
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", target_table)
        .mode("overwrite")
        .save()
    )

    print(f"Loaded {row_count} rows into Snowflake table {target_table}")
    return (table_name, target_table, row_count)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Verify Gold Tables in Databricks Before Loading

# COMMAND ----------

summary_rows = []
for table_name in GOLD_TABLES:
    full_name = databricks_table(table_name)
    summary_rows.append((full_name, spark.table(full_name).count()))

display(spark.createDataFrame(summary_rows, ["databricks_table", "row_count"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Load Gold Tables to Snowflake

# COMMAND ----------

load_results = []

for table_name in GOLD_TABLES:
    result = load_table_to_snowflake(table_name)
    load_results.append(result)

load_results_df = spark.createDataFrame(load_results, ["databricks_table", "snowflake_table", "rows_loaded"])
display(load_results_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Optional Read-Back Check from Snowflake
# MAGIC
# MAGIC This reads row counts back from Snowflake to prove the load worked.

# COMMAND ----------

readback_rows = []

for table_name in GOLD_TABLES:
    target_table = snowflake_table_name(table_name)
    query = f"SELECT COUNT(*) AS ROW_COUNT FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_table}"

    count_df = (
        spark.read
        .format("snowflake")
        .options(**sf_options)
        .option("query", query)
        .load()
    )

    row_count = count_df.collect()[0]["ROW_COUNT"]
    readback_rows.append((target_table, int(row_count)))

readback_df = spark.createDataFrame(readback_rows, ["snowflake_table", "row_count"])
display(readback_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Complete
# MAGIC
# MAGIC You loaded Databricks Gold tables into Snowflake and verified row counts.
