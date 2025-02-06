# Databricks notebook source
# COMMAND ----------
# MAGIC %python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType

# COMMAND ----------
# MAGIC %python
# Define schemas for data consistency
policy_schema = StructType([
    StructField("policy_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("policy_type", StringType(), True),
    StructField("policy_status", StringType(), True),
    StructField("policy_start_date", DateType(), True),
    StructField("policy_end_date", DateType(), True),
    StructField("policy_term", IntegerType(), True),
    StructField("policy_premium", DoubleType(), True),
    StructField("total_premium_paid", DoubleType(), True),
    StructField("renewal_status", StringType(), True),
    StructField("policy_addons", StringType(), True)
])

claims_schema = StructType([
    StructField("Claim_ID", StringType(), True),
    StructField("Policy_ID", StringType(), True),
    StructField("Claim_Date", DateType(), True),
    StructField("Claim_Type", StringType(), True),
    StructField("Claim_Status", StringType(), True),
    StructField("Claim_Amount", DoubleType(), True),
    StructField("Claim_Payout", DoubleType(), True)
])

demographics_schema = StructType([
    StructField("Customer_ID", StringType(), True),
    StructField("Customer_Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone_Number", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal_Code", StringType(), True),
    StructField("Date_of_Birth", DateType(), True),
    StructField("Gender", StringType(), True),
    StructField("Marital_Status", StringType(), True),
    StructField("Occupation", StringType(), True),
    StructField("Income_Level", StringType(), True),
    StructField("Customer_Segment", StringType(), True)
])

# COMMAND ----------
# MAGIC %python
# Load data with schema enforcement
def load_data_with_schema():
    try:
        logger.info("Loading data from Unity Catalog tables with schema enforcement...")
        policy_df = spark.read.schema(policy_schema).table("genai_demo.guardian.policy")
        claims_df = spark.read.schema(claims_schema).table("genai_demo.guardian.claims")
        demographics_df = spark.read.schema(demographics_schema).table("genai_demo.guardian.demographics")
        scores_df = spark.table("genai_demo.guardian.scores")
        aiml_insights_df = spark.table("genai_demo.guardian.aiml_insights")
        return policy_df, claims_df, demographics_df, scores_df, aiml_insights_df
    except Exception as e:
        logger.error(f"Error loading data with schema: {e}")
        raise

# COMMAND ----------
# MAGIC %python
# Main function with schema enforcement
def main_with_schema():
    try:
        policy_df, claims_df, demographics_df, scores_df, aiml_insights_df = load_data_with_schema()
        selected_demographics_df, selected_claims_df, selected_policy_df = select_and_filter_data(policy_df, claims_df, demographics_df)
        integrated_df = integrate_data(selected_demographics_df, selected_claims_df, selected_policy_df)
        aggregated_df = aggregate_data(integrated_df)
        final_df = calculate_custom_metrics(aggregated_df)
        final_dataset = consolidate_data(final_df, scores_df, aiml_insights_df)
        write_output(final_dataset)
        logger.info("ETL process completed successfully with schema enforcement.")
    except Exception as e:
        logger.error(f"ETL process failed with schema enforcement: {e}")

# COMMAND ----------
# MAGIC %python
if __name__ == "__main__":
    main_with_schema()
