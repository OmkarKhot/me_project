
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, months_between, current_date, to_date, count, sum as _sum, avg, max, datediff
from pyspark.sql.types import IntegerType

def run_banking_churn_pipeline_enhanced(customers_path, subscriptions_path, transactions_path, output_path=None):
    spark = SparkSession.builder.appName("Banking Churn Feature Engineering Enhanced").getOrCreate()

    # Load CSVs
    customers = spark.read.option("header", True).option("inferSchema", True).csv(customers_path)
    subscriptions = spark.read.option("header", True).option("inferSchema", True).csv(subscriptions_path)
    transactions = spark.read.option("header", True).option("inferSchema", True).csv(transactions_path)

    # Preprocess: parse dates
    customers = customers.withColumn("signup_date", to_date("signup_date"))
    subscriptions = subscriptions.withColumn("start_date", to_date("start_date"))                                  .withColumn("end_date", when(col("end_date").isNull(), current_date()).otherwise(to_date("end_date")))
    transactions = transactions.withColumn("txn_date", to_date("txn_date"))

    # Join customers and subscriptions
    joined = customers.join(subscriptions, on="customer_id", how="left")

    # Tenure calculation
    joined = joined.withColumn("tenure_months", months_between(current_date(), col("start_date")).cast(IntegerType()))

    # Join transactions: aggregate stats per customer
    txn_agg = transactions.groupBy("customer_id").agg(
        count("*").alias("txn_count"),
        _sum("txn_amount").alias("total_txn_amount"),
        avg("txn_amount").alias("avg_txn_amount"),
        max("txn_date").alias("last_txn_date")
    )

    # High-value transaction count (txn_amount > 10000)
    high_value_txns = transactions.filter(col("txn_amount") > 10000)                                   .groupBy("customer_id").count().withColumnRenamed("count", "high_value_txn_count")

    full_data = joined.join(txn_agg, on="customer_id", how="left")                       .join(high_value_txns, on="customer_id", how="left")                       .fillna({"high_value_txn_count": 0})

    # Days since last transaction
    full_data = full_data.withColumn("days_since_last_txn", datediff(current_date(), col("last_txn_date")))

    # Monthly average spend
    full_data = full_data.withColumn("monthly_avg_spend", col("total_txn_amount") / (col("tenure_months") + 1))

    # Tenure segmentation
    full_data = full_data.withColumn(
        "tenure_segment",
        when(col("tenure_months") < 12, "New")
        .when(col("tenure_months") < 36, "Mid")
        .otherwise("Long")
    )

    # Churn logic: inactive or no transaction or no recent transaction (e.g., last txn > 90 days ago)
    full_data = full_data.withColumn("is_churned",
                    when((col("is_active") == False) |
                         (col("txn_count").isNull()) |
                         (col("txn_count") < 1) |
                         (col("days_since_last_txn") > 90), True).otherwise(False))

    # Final features
    result = full_data.select(
        "customer_id", "age", "gender", "plan_type", "monthly_cost", "tenure_months", "tenure_segment",
        "txn_count", "total_txn_amount", "avg_txn_amount", "monthly_avg_spend", "days_since_last_txn",
        "high_value_txn_count", "is_churned"
    )

    if output_path:
        result.write.mode("overwrite").option("header", True).csv(output_path)
    else:
        result.show()

    spark.stop()
    return result
