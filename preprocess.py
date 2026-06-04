from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, desc, sum as _sum

spark = SparkSession.builder.appName("EcommercePipeline").getOrCreate()

# Read data
df = spark.read.csv("/opt/airflow/data/ecommerce.csv", header=True, inferSchema=True)

# ----------------------------
# Data Cleaning
# ----------------------------
df = df.dropna(subset=["Order_ID", "Price", "Quantity"])

df = df.fillna({
    "Customer_City": "Unknown",
    "Product_Category": "Unknown",
    "Payment_Method": "Unknown"
})

# ----------------------------
# Transformations
# ----------------------------
df = df.withColumn("order_date", to_date(col("Order_Date"), "yyyy-MM-dd"))
df = df.withColumn("year", year(col("order_date")))
df = df.withColumn("month", month(col("order_date")))

# ----------------------------
# Feature Engineering
# ----------------------------
df = df.withColumn("revenue", col("Price") * col("Quantity"))

# ----------------------------
# Final Selection
# ----------------------------
df = df.select(
    "Order_ID",
    "order_date",
    "Product_Name",
    "Product_Category",
    "revenue",
    "Customer_City",
    "year",
    "month"
)

# ----------------------------
# Config
# ----------------------------
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ----------------------------
# MAIN OUTPUT (Processed Data)
# ----------------------------
df.write.mode("overwrite").partitionBy("year").parquet("/opt/airflow/output/processed/")

# =========================================================
# 🔥 AGGREGATIONS (Analytics Layer)
# =========================================================

# Category Revenue
category_revenue = df.groupBy("Product_Category") \
    .agg(_sum("revenue").alias("total_revenue"))

category_revenue.write.mode("overwrite") \
    .parquet("/opt/airflow/output/analytics/category_revenue/")

# Monthly Sales Trend
monthly_sales = df.groupBy("year", "month") \
    .agg(_sum("revenue").alias("monthly_revenue")) \
    .orderBy("year", "month")

monthly_sales.write.mode("overwrite") \
    .parquet("/opt/airflow/output/analytics/monthly_sales/")

# Top Products
top_products = df.groupBy("Product_Name") \
    .agg(_sum("revenue").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

top_products.write.mode("overwrite") \
    .parquet("/opt/airflow/output/analytics/top_products/")

# City-wise Sales
city_sales = df.groupBy("Customer_City") \
    .agg(_sum("revenue").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

city_sales.write.mode("overwrite") \
    .parquet("/opt/airflow/output/analytics/city_sales/")

