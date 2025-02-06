# COMMAND ----------
# %md
# # ETL Process for Guardian Insurance Data
# This notebook performs an ETL process on insurance data using PySpark. It integrates data from multiple tables, performs transformations, and writes the results to a Delta table.

# COMMAND ----------
# %python
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

# COMMAND ----------
# %md
# ## Configure Logging

# COMMAND ----------
# %python
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
# %md
# ## Enable Delta Lake Optimizations

# COMMAND ----------
# %python
# Enable Delta Lake optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoCompact", "true")

# COMMAND ----------
# %md
# ## Load Data from Unity Catalog Tables

# COMMAND ----------
# %python
try:
    # Load data from Unity Catalog tables with schema enforcement
    policy_df = spark.table("genai_demo.guardian.policy").select(
        "policy_id", "customer_id", "policy_type", "policy_status", 
        F.col("policy_start_date").cast(DateType()), 
        F.col("policy_end_date").cast(DateType()), 
        "policy_term", 
        F.col("policy_premium").cast(DoubleType()), 
        F.col("total_premium_paid").cast(DoubleType()), 
        "renewal_status", "policy_addons"
    )

    claims_df = spark.table("genai_demo.guardian.claims").select(
        "Claim_ID", "Policy_ID", 
        F.col("Claim_Date").cast(DateType()), 
        "Claim_Type", "Claim_Status", 
        F.col("Claim_Amount").cast(DoubleType()), 
        F.col("Claim_Payout").cast(DoubleType())
    )

    demographics_df = spark.table("genai_demo.guardian.demographics").select(
        "Customer_ID", "Customer_Name", "Email", "Phone_Number", 
        "Address", "City", "State", "Postal_Code", 
        F.col("Date_of_Birth").cast(DateType()), 
        "Gender", "Marital_Status", "Occupation", "Income_Level", "Customer_Segment"
    )

    scores_df = spark.table("genai_demo.guardian.scores").select(
        "Customer_ID", "Credit_Score", "Fraud_Score", "Customer_Risk_Score"
    )

    aiml_insights_df = spark.table("genai_demo.guardian.aiml_insights").select(
        "Customer_ID", "Churn_Probability", "Next_Best_Offer", 
        "Claims_Fraud_Probability", "Revenue_Potential"
    )

# COMMAND ----------
# %md
# ## Data Integration

# COMMAND ----------
# %python
    # Data Integration
    demo_policy_df = demographics_df.join(policy_df, demographics_df.Customer_ID == policy_df.customer_id, "inner")
    demo_policy_claims_df = demo_policy_df.join(claims_df, demo_policy_df.policy_id == claims_df.Policy_ID, "inner")

# COMMAND ----------
# %md
# ## Data Aggregation

# COMMAND ----------
# %python
    # Data Aggregation
    aggregated_df = demo_policy_claims_df.groupBy("Customer_ID").agg(
        F.count("Claim_ID").alias("Total_Claims"),
        F.count("policy_id").alias("Policy_Count"),
        F.max("Claim_Date").alias("Recent_Claim_Date"),
        F.avg("Claim_Amount").alias("Average_Claim_Amount")
    )

# COMMAND ----------
# %md
# ## Custom Calculations

# COMMAND ----------
# %python
    # Custom Calculations
    final_df = demo_policy_claims_df.join(aggregated_df, "Customer_ID", "inner")
    final_df = final_df.withColumn("Age", F.datediff(F.current_date(), F.col("Date_of_Birth")) / 365) \
        .withColumn("Claim_To_Premium_Ratio", F.when(F.col("total_premium_paid") != 0, F.col("Claim_Amount") / F.col("total_premium_paid")).otherwise(0)) \
        .withColumn("Claims_Per_Policy", F.when(F.col("Policy_Count") != 0, F.col("Total_Claims") / F.col("Policy_Count")).otherwise(0)) \
        .withColumn("Retention_Rate", F.lit(0.85)) \
        .withColumn("Cross_Sell_Opportunities", F.lit("Multi-Policy Discount, Home Coverage Add-on")) \
        .withColumn("Upsell_Potential", F.lit("Premium Vehicle Coverage"))

# COMMAND ----------
# %md
# ## Comprehensive Data Joining

# COMMAND ----------
# %python
    # Comprehensive Data Joining
    customer_360_df = final_df.join(scores_df, "Customer_ID", "inner") \
        .join(aiml_insights_df, "Customer_ID", "inner")

# COMMAND ----------
# %md
# ## Output Generation to Delta Table

# COMMAND ----------
# %python
    # Output Generation to Delta Table
    customer_360_df.write.format("delta").mode("overwrite").saveAsTable("genai_demo.guardian.customer_360")

    logger.info("ETL process completed successfully and data written to Delta table.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")
    raise
