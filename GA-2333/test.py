
# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 ETL Process
# MAGIC This notebook performs an ETL process to create a comprehensive view of customer data by integrating various data sources.

# COMMAND ----------

import pyspark.sql.functions as F
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

try:
    # Step 1: Data Source Configuration
    logger.info("Loading data from Unity Catalog tables...")
    claims_df = spark.table("genai_demo.guardian.claims")
    demographics_df = spark.table("genai_demo.guardian.demographics")
    policy_df = spark.table("genai_demo.guardian.policy")
    scores_df = spark.table("genai_demo.guardian.scores")
    aiml_insights_df = spark.table("genai_demo.guardian.aiml_insights")

# COMMAND ----------

    # Step 2: Data Selection and Filtering
    logger.info("Selecting relevant fields from demographics and claims data...")
    selected_demographics_df = demographics_df.select(
        "Customer_ID", "Customer_Name", "Email", "Phone_Number", "Address", "City", "State", "Postal_Code",
        "Date_of_Birth", "Gender", "Marital_Status", "Occupation", "Income_Level", "Customer_Segment"
    )
    selected_claims_df = claims_df.select(
        "Claim_ID", "Policy_ID", "Claim_Date", "Claim_Type", "Claim_Status", "Claim_Amount", "Claim_Payout"
    )

# COMMAND ----------

    # Step 3: Data Integration
    logger.info("Joining demographics and policy data on Customer_ID...")
    joined_df = selected_demographics_df.join(policy_df, "Customer_ID").join(selected_claims_df, "Policy_ID")

# COMMAND ----------

    # Step 4: Data Aggregation
    logger.info("Aggregating claims data to calculate metrics...")
    aggregated_df = joined_df.groupBy("Customer_ID").agg(
        F.count("Claim_ID").alias("Total_Claims"),
        F.countDistinct("Policy_ID").alias("Policy_Count"),
        F.max("Claim_Date").alias("Recent_Claim_Date"),
        F.avg("Claim_Amount").alias("Average_Claim_Amount"),
        F.first("Date_of_Birth").alias("Date_of_Birth"),  # Ensure Date_of_Birth is available for Age calculation
        F.first("Total_Premium_Paid").alias("Total_Premium_Paid")  # Ensure Total_Premium_Paid is available
    )

# COMMAND ----------

    # Step 5: Custom Calculations
    logger.info("Performing custom calculations...")
    final_df = aggregated_df.withColumn("Age", F.datediff(F.current_date(), F.col("Date_of_Birth")) / 365) \
        .withColumn("Claim_To_Premium_Ratio", F.when(F.col("Total_Premium_Paid") != 0, F.col("Average_Claim_Amount") / F.col("Total_Premium_Paid")).otherwise(0)) \
        .withColumn("Claims_Per_Policy", F.when(F.col("Policy_Count") != 0, F.col("Total_Claims") / F.col("Policy_Count")).otherwise(0)) \
        .withColumn("Retention_Rate", F.lit(0.85)) \
        .withColumn("Cross_Sell_Opportunities", F.lit("Multi-Policy Discount, Home Coverage Add-on")) \
        .withColumn("Upsell_Potential", F.lit("Premium Vehicle Coverage"))

# COMMAND ----------

    # Step 6: Comprehensive Data Joining
    logger.info("Integrating data from all sources...")
    customer_360_df = final_df.join(scores_df, "Customer_ID").join(aiml_insights_df, "Customer_ID")

# COMMAND ----------

    # Step 7: Output Data
    logger.info("Writing the consolidated view of customer data to Delta format...")
    customer_360_df.write.format("delta").mode("overwrite").save("/mnt/dataeconomy-8ixr/62446/output/Customer_360")

    logger.info("ETL process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")
    raise
