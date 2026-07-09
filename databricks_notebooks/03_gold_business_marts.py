# Databricks notebook source
# MAGIC %md
# MAGIC #Gold Layer: Business Analytics Marts
# MAGIC 
# MAGIC Gold layer = business-ready reporting tables built from clean Silver tables.

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
CATALOG = "workspace"
SCHEMA = "default"

# COMMAND ----------
def full_table(table_name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table_name}"

def save_gold_table(df, table_name: str):
    full_name = full_table(table_name)
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_name))
    print(f"Saved {full_name}: {spark.table(full_name).count()} rows")

def add_gold_audit_columns(df):
    return (df
        .withColumn("gold_processed_timestamp", F.current_timestamp())
        .withColumn("pipeline_layer", F.lit("GOLD")))

# COMMAND ----------
# Read Silver tables
silver_branch_master = spark.table(full_table("silver_branch_master"))
silver_customers = spark.table(full_table("silver_customers"))
silver_loan_applications = spark.table(full_table("silver_loan_applications"))
silver_loan_accounts = spark.table(full_table("silver_loan_accounts"))
silver_emi_payments = spark.table(full_table("silver_emi_payments"))
silver_delinquency = spark.table(full_table("silver_delinquency"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Gold Customer Loan Summary
# MAGIC Grain: one row per customer.

# COMMAND ----------
loan_customer_agg = (
    silver_loan_accounts.groupBy("customer_id")
    .agg(
        F.countDistinct("loan_id").alias("total_loans"),
        F.sum("loan_amount").alias("total_loan_amount"),
        F.avg("interest_rate").alias("avg_interest_rate"),
        F.sum(F.when(F.col("loan_status") == "ACTIVE", 1).otherwise(0)).alias("active_loan_count"),
        F.sum(F.when(F.col("loan_status") == "CLOSED", 1).otherwise(0)).alias("closed_loan_count"),
        F.sum(F.when(F.col("loan_status") == "DEFAULTED", 1).otherwise(0)).alias("defaulted_loan_count")
    )
)

payment_customer_agg = (
    silver_emi_payments.alias("p")
    .join(silver_loan_accounts.select("loan_id", "customer_id").alias("l"), "loan_id", "inner")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("payment_id").alias("total_emi_records"),
        F.sum("emi_amount").alias("total_emi_due"),
        F.sum("paid_amount").alias("total_amount_paid"),
        F.sum(F.when(F.col("payment_status") == "PAID", 1).otherwise(0)).alias("paid_emi_count"),
        F.sum(F.when(F.col("payment_status") == "LATE", 1).otherwise(0)).alias("late_emi_count"),
        F.sum(F.when(F.col("payment_status") == "MISSED", 1).otherwise(0)).alias("missed_emi_count"),
        F.sum(F.when(F.col("payment_status") == "PARTIAL", 1).otherwise(0)).alias("partial_emi_count")
    )
)

delinq_customer_agg = (
    silver_delinquency.alias("d")
    .join(silver_loan_accounts.select("loan_id", "customer_id").alias("l"), "loan_id", "inner")
    .groupBy("customer_id")
    .agg(
        F.max("days_past_due").alias("max_days_past_due"),
        F.sum("overdue_amount").alias("total_overdue_amount"),
        F.max(F.when(F.col("npa_flag") == "Y", 1).otherwise(0)).alias("has_npa_flag")
    )
)

gold_customer_loan_summary = (
    silver_customers.alias("c")
    .join(loan_customer_agg.alias("la"), "customer_id", "left")
    .join(payment_customer_agg.alias("pa"), "customer_id", "left")
    .join(delinq_customer_agg.alias("da"), "customer_id", "left")
    .fillna(0, subset=[
        "total_loans", "total_loan_amount", "avg_interest_rate", "active_loan_count",
        "closed_loan_count", "defaulted_loan_count", "total_emi_records", "total_emi_due",
        "total_amount_paid", "paid_emi_count", "late_emi_count", "missed_emi_count",
        "partial_emi_count", "max_days_past_due", "total_overdue_amount", "has_npa_flag"
    ])
    .withColumn("collection_rate", F.when(F.col("total_emi_due") > 0, F.round((F.col("total_amount_paid") / F.col("total_emi_due")) * 100, 2)).otherwise(F.lit(0)))
    .withColumn("risk_category", F.when((F.col("has_npa_flag") == 1) | (F.col("max_days_past_due") >= 90), "HIGH")
        .when((F.col("missed_emi_count") > 0) | (F.col("max_days_past_due") >= 30), "MEDIUM")
        .otherwise("LOW"))
    .select("customer_id", "customer_name", "gender", "city", "state", "employment_type", "annual_income", "credit_score",
            "total_loans", "total_loan_amount", "avg_interest_rate", "active_loan_count", "closed_loan_count", "defaulted_loan_count",
            "total_emi_records", "total_emi_due", "total_amount_paid", "collection_rate", "paid_emi_count", "late_emi_count",
            "missed_emi_count", "partial_emi_count", "max_days_past_due", "total_overdue_amount", "has_npa_flag", "risk_category")
)

save_gold_table(add_gold_audit_columns(gold_customer_loan_summary), "gold_customer_loan_summary")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Gold Branch Performance Summary
# MAGIC Grain: one row per branch.

# COMMAND ----------
branch_loan_agg = (
    silver_loan_accounts.groupBy("branch_id")
    .agg(
        F.countDistinct("loan_id").alias("total_loans"),
        F.countDistinct("customer_id").alias("total_borrowers"),
        F.sum("loan_amount").alias("total_disbursed_amount"),
        F.avg("interest_rate").alias("avg_interest_rate"),
        F.sum(F.when(F.col("loan_status") == "ACTIVE", 1).otherwise(0)).alias("active_loans"),
        F.sum(F.when(F.col("loan_status") == "CLOSED", 1).otherwise(0)).alias("closed_loans"),
        F.sum(F.when(F.col("loan_status") == "DEFAULTED", 1).otherwise(0)).alias("defaulted_loans")
    )
)

branch_payment_agg = (
    silver_emi_payments.alias("p")
    .join(silver_loan_accounts.select("loan_id", "branch_id").alias("l"), "loan_id", "inner")
    .groupBy("branch_id")
    .agg(
        F.sum("emi_amount").alias("total_emi_due"),
        F.sum("paid_amount").alias("total_collected_amount"),
        F.sum(F.when(F.col("payment_status") == "MISSED", 1).otherwise(0)).alias("missed_emi_count"),
        F.sum(F.when(F.col("payment_status") == "LATE", 1).otherwise(0)).alias("late_emi_count")
    )
)

branch_delinq_agg = (
    silver_delinquency.alias("d")
    .join(silver_loan_accounts.select("loan_id", "branch_id").alias("l"), "loan_id", "inner")
    .groupBy("branch_id")
    .agg(
        F.sum("overdue_amount").alias("total_overdue_amount"),
        F.max("days_past_due").alias("max_days_past_due"),
        F.sum(F.when(F.col("npa_flag") == "Y", 1).otherwise(0)).alias("npa_records")
    )
)

gold_branch_performance_summary = (
    silver_branch_master.alias("b")
    .join(branch_loan_agg.alias("la"), "branch_id", "left")
    .join(branch_payment_agg.alias("pa"), "branch_id", "left")
    .join(branch_delinq_agg.alias("da"), "branch_id", "left")
    .fillna(0, subset=["total_loans", "total_borrowers", "total_disbursed_amount", "avg_interest_rate", "active_loans", "closed_loans", "defaulted_loans", "total_emi_due", "total_collected_amount", "missed_emi_count", "late_emi_count", "total_overdue_amount", "max_days_past_due", "npa_records"])
    .withColumn("collection_rate", F.when(F.col("total_emi_due") > 0, F.round((F.col("total_collected_amount") / F.col("total_emi_due")) * 100, 2)).otherwise(F.lit(0)))
    .withColumn("default_rate", F.when(F.col("total_loans") > 0, F.round((F.col("defaulted_loans") / F.col("total_loans")) * 100, 2)).otherwise(F.lit(0)))
    .select("branch_id", "branch_name", "city", "state", "region", "is_active", "total_loans", "total_borrowers", "total_disbursed_amount", "avg_interest_rate", "active_loans", "closed_loans", "defaulted_loans", "default_rate", "total_emi_due", "total_collected_amount", "collection_rate", "missed_emi_count", "late_emi_count", "total_overdue_amount", "max_days_past_due", "npa_records")
)

save_gold_table(add_gold_audit_columns(gold_branch_performance_summary), "gold_branch_performance_summary")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Gold Monthly Collection Summary
# MAGIC Grain: one row per EMI due month and loan type.

# COMMAND ----------
gold_monthly_collection_summary = (
    silver_emi_payments.alias("p")
    .join(silver_loan_accounts.select("loan_id", "loan_type", "branch_id").alias("l"), "loan_id", "inner")
    .withColumn("emi_due_month", F.date_format("emi_due_date", "yyyy-MM"))
    .groupBy("emi_due_month", "loan_type")
    .agg(
        F.countDistinct("payment_id").alias("total_emi_records"),
        F.countDistinct("loan_id").alias("total_loans"),
        F.sum("emi_amount").alias("total_emi_due"),
        F.sum("paid_amount").alias("total_amount_paid"),
        F.sum(F.when(F.col("payment_status") == "PAID", 1).otherwise(0)).alias("paid_count"),
        F.sum(F.when(F.col("payment_status") == "LATE", 1).otherwise(0)).alias("late_count"),
        F.sum(F.when(F.col("payment_status") == "PARTIAL", 1).otherwise(0)).alias("partial_count"),
        F.sum(F.when(F.col("payment_status") == "MISSED", 1).otherwise(0)).alias("missed_count")
    )
    .withColumn("collection_rate", F.when(F.col("total_emi_due") > 0, F.round((F.col("total_amount_paid") / F.col("total_emi_due")) * 100, 2)).otherwise(F.lit(0)))
    .withColumn("missed_rate", F.when(F.col("total_emi_records") > 0, F.round((F.col("missed_count") / F.col("total_emi_records")) * 100, 2)).otherwise(F.lit(0)))
    .orderBy("emi_due_month", "loan_type")
)

save_gold_table(add_gold_audit_columns(gold_monthly_collection_summary), "gold_monthly_collection_summary")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Gold Loan Default Risk Summary
# MAGIC Grain: one row per loan.

# COMMAND ----------
payment_loan_agg = (
    silver_emi_payments.groupBy("loan_id")
    .agg(
        F.countDistinct("payment_id").alias("total_emi_records"),
        F.sum("emi_amount").alias("total_emi_due"),
        F.sum("paid_amount").alias("total_amount_paid"),
        F.sum(F.when(F.col("payment_status") == "PAID", 1).otherwise(0)).alias("paid_count"),
        F.sum(F.when(F.col("payment_status") == "LATE", 1).otherwise(0)).alias("late_count"),
        F.sum(F.when(F.col("payment_status") == "PARTIAL", 1).otherwise(0)).alias("partial_count"),
        F.sum(F.when(F.col("payment_status") == "MISSED", 1).otherwise(0)).alias("missed_count")
    )
)

delinq_loan_agg = (
    silver_delinquency.groupBy("loan_id")
    .agg(
        F.max("days_past_due").alias("max_days_past_due"),
        F.sum("overdue_amount").alias("total_overdue_amount"),
        F.max(F.when(F.col("npa_flag") == "Y", 1).otherwise(0)).alias("has_npa_flag")
    )
)

gold_loan_default_risk_summary = (
    silver_loan_accounts.alias("l")
    .join(silver_customers.select("customer_id", "customer_name", "credit_score", "annual_income").alias("c"), "customer_id", "left")
    .join(silver_branch_master.select("branch_id", "branch_name", "city", "state", "region").alias("b"), "branch_id", "left")
    .join(payment_loan_agg.alias("p"), "loan_id", "left")
    .join(delinq_loan_agg.alias("d"), "loan_id", "left")
    .fillna(0, subset=["total_emi_records", "total_emi_due", "total_amount_paid", "paid_count", "late_count", "partial_count", "missed_count", "max_days_past_due", "total_overdue_amount", "has_npa_flag"])
    .withColumn("collection_rate", F.when(F.col("total_emi_due") > 0, F.round((F.col("total_amount_paid") / F.col("total_emi_due")) * 100, 2)).otherwise(F.lit(0)))
    .withColumn("risk_score", F.when((F.col("has_npa_flag") == 1) | (F.col("max_days_past_due") >= 90), 100)
        .when((F.col("missed_count") >= 2) | (F.col("max_days_past_due") >= 60), 80)
        .when((F.col("missed_count") == 1) | (F.col("late_count") >= 2) | (F.col("max_days_past_due") >= 30), 60)
        .when((F.col("late_count") == 1) | (F.col("partial_count") >= 1), 40)
        .otherwise(20))
    .withColumn("risk_category", F.when(F.col("risk_score") >= 90, "CRITICAL")
        .when(F.col("risk_score") >= 70, "HIGH")
        .when(F.col("risk_score") >= 50, "MEDIUM")
        .otherwise("LOW"))
    .select("loan_id", "application_id", "customer_id", "customer_name", "credit_score", "annual_income", "branch_id", "branch_name", "city", "state", "region", "loan_type", "loan_amount", "interest_rate", "loan_start_date", "loan_status", "total_emi_records", "total_emi_due", "total_amount_paid", "collection_rate", "paid_count", "late_count", "partial_count", "missed_count", "max_days_past_due", "total_overdue_amount", "has_npa_flag", "risk_score", "risk_category")
)

save_gold_table(add_gold_audit_columns(gold_loan_default_risk_summary), "gold_loan_default_risk_summary")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Gold Portfolio KPIs
# MAGIC Grain: one row for executive summary.

# COMMAND ----------
gold_portfolio_kpis = (
    spark.table(full_table("gold_loan_default_risk_summary"))
    .agg(
        F.countDistinct("loan_id").alias("total_loans"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum("loan_amount").alias("total_loan_portfolio"),
        F.sum("total_emi_due").alias("total_emi_due"),
        F.sum("total_amount_paid").alias("total_amount_collected"),
        F.sum("total_overdue_amount").alias("total_overdue_amount"),
        F.sum(F.when(F.col("loan_status") == "ACTIVE", 1).otherwise(0)).alias("active_loans"),
        F.sum(F.when(F.col("loan_status") == "CLOSED", 1).otherwise(0)).alias("closed_loans"),
        F.sum(F.when(F.col("loan_status") == "DEFAULTED", 1).otherwise(0)).alias("defaulted_loans"),
        F.sum(F.when(F.col("risk_category").isin("HIGH", "CRITICAL"), 1).otherwise(0)).alias("high_risk_loans"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score")
    )
    .withColumn("collection_rate", F.when(F.col("total_emi_due") > 0, F.round((F.col("total_amount_collected") / F.col("total_emi_due")) * 100, 2)).otherwise(F.lit(0)))
    .withColumn("default_rate", F.when(F.col("total_loans") > 0, F.round((F.col("defaulted_loans") / F.col("total_loans")) * 100, 2)).otherwise(F.lit(0)))
    .withColumn("high_risk_loan_rate", F.when(F.col("total_loans") > 0, F.round((F.col("high_risk_loans") / F.col("total_loans")) * 100, 2)).otherwise(F.lit(0)))
    .withColumn("kpi_date", F.current_date())
)

save_gold_table(add_gold_audit_columns(gold_portfolio_kpis), "gold_portfolio_kpis")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Gold Row Count Summary

# COMMAND ----------
gold_tables = [
    "gold_customer_loan_summary",
    "gold_branch_performance_summary",
    "gold_monthly_collection_summary",
    "gold_loan_default_risk_summary",
    "gold_portfolio_kpis"
]
summary_rows = [(t, spark.table(full_table(t)).count()) for t in gold_tables]
gold_summary_df = spark.createDataFrame(summary_rows, ["table_name", "row_count"])
display(gold_summary_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Preview Gold Tables

# COMMAND ----------
display(spark.table(full_table("gold_customer_loan_summary")).select("customer_id", "customer_name", "total_loans", "total_loan_amount", "collection_rate", "max_days_past_due", "total_overdue_amount", "risk_category").orderBy(F.col("total_loan_amount").desc()).limit(10))

# COMMAND ----------
display(spark.table(full_table("gold_branch_performance_summary")).select("branch_id", "branch_name", "city", "region", "total_loans", "total_disbursed_amount", "collection_rate", "default_rate", "total_overdue_amount").orderBy(F.col("total_disbursed_amount").desc()))

# COMMAND ----------
display(spark.table(full_table("gold_monthly_collection_summary")).orderBy("emi_due_month", "loan_type"))

# COMMAND ----------
display(spark.table(full_table("gold_loan_default_risk_summary")).select("loan_id", "customer_id", "customer_name", "loan_type", "loan_amount", "collection_rate", "missed_count", "max_days_past_due", "total_overdue_amount", "risk_score", "risk_category").orderBy(F.col("risk_score").desc(), F.col("total_overdue_amount").desc()).limit(20))

# COMMAND ----------
display(spark.table(full_table("gold_portfolio_kpis")))
