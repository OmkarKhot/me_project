
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, months_between, current_date, to_date, count, sum as _sum, avg
from pyspark.sql.types import IntegerType

def run_banking_churn_pipeline(customers_path, subscriptions_path, transactions_path, output_path=None):
    spark = SparkSession.builder.appName("Banking Churn Feature Engineering").getOrCreate()

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
        avg("txn_amount").alias("avg_txn_amount")
    )

    full_data = joined.join(txn_agg, on="customer_id", how="left")

    # Churn logic: inactive or no transaction in last 3 months
    full_data = full_data.withColumn("is_churned",
                    when((col("is_active") == False) |
                         (col("txn_count").isNull()) |
                         (col("txn_count") < 1), True).otherwise(False))

    # Final features
    result = full_data.select(
        "customer_id", "age", "gender", "plan_type", "monthly_cost",
        "tenure_months", "txn_count", "total_txn_amount", "avg_txn_amount", "is_churned"
    )

    if output_path:
        result.write.mode("overwrite").option("header", True).csv(output_path)
    else:
        result.show()

    spark.stop()
    return result
