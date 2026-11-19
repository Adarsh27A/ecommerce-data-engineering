# Databricks notebook source
silver_path = (
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/silver"
)

silver_dfs = {
    name: spark.read.parquet(f"{silver_path}/{name}")
    for name in [
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
    ]
}

print("Silver datasets loaded successfully.")

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Monthly revenue
gold_monthly_revenue = (
    silver_dfs["orders"]
    .filter(F.col("status").isin("Completed", "Shipped"))
    .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("order_month")
    .agg(
        F.sum("total_amount").alias("monthly_revenue"),
        F.countDistinct("order_id").alias("order_count")
    )
)

# 2. Sales by category
gold_category_sales = (
    silver_dfs["order_items"]
    .join(silver_dfs["products"], "product_id")
    .join(silver_dfs["orders"], "order_id")
    .filter(F.col("status").isin("Completed", "Shipped"))
    .groupBy("category")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum(F.col("quantity") * F.col("unit_price"))
         .alias("sales_revenue")
    )
)

# 3. Top 10 products by units sold
gold_top_products = (
    silver_dfs["order_items"]
    .join(silver_dfs["products"], "product_id")
    .join(silver_dfs["orders"], "order_id")
    .filter(F.col("status").isin("Completed", "Shipped"))
    .groupBy("product_id", "product_name")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum(F.col("quantity") * F.col("unit_price"))
         .alias("sales_revenue")
    )
    .orderBy(F.desc("units_sold"))
    .limit(10)
)

# 4. Payment analysis
gold_payment_analysis = (
    silver_dfs["payments"]
    .groupBy("payment_status")
    .agg(
        F.count("payment_id").alias("payment_count"),
        F.sum("amount").alias("total_payment_amount")
    )
)

# Save all Gold outputs
gold_path = (
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/gold"
)

gold_outputs = {
    "monthly_revenue": gold_monthly_revenue,
    "category_sales": gold_category_sales,
    "top_products": gold_top_products,
    "payment_analysis": gold_payment_analysis,
}

for name, df in gold_outputs.items():
    (
        df.write
        .mode("overwrite")
        .parquet(f"{gold_path}/{name}")
    )
    print(f"Saved: {name}")

print("Gold analytics pipeline completed.")

# COMMAND ----------

gold_path = "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/gold"

gold_dfs = {
    name: spark.read.parquet(f"{gold_path}/{name}")
    for name in [
        "monthly_revenue",
        "category_sales",
        "top_products",
        "payment_analysis"
    ]
}

print("All Gold datasets loaded successfully.")

# COMMAND ----------

for name, df in gold_dfs.items():
    print(f"\n{'='*50}")
    print(f"DATASET: {name}")
    print(f"{'='*50}")

    print("Schema:")
    df.printSchema()

    print("Records:")
    df.show(10, truncate=False)

    print(f"Total rows: {df.count()}")

# COMMAND ----------

#
silver_dfs["products"].filter(
    F.col("product_id") == 1
).show(truncate=False)

# Inspect its order-item records
silver_dfs["order_items"].filter(
    F.col("product_id") == 1
).show(20, truncate=False)



# COMMAND ----------

(
    silver_dfs["order_items"]
    .filter(F.col("product_id") == 1)
    .join(silver_dfs["orders"], "order_id")
    .filter(F.col("status").isin("Completed", "Shipped"))
    .agg(
        F.count("*").alias("order_item_records"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum(
            F.col("quantity") * F.col("unit_price")
        ).alias("total_revenue")
    )
    .show()
)

# COMMAND ----------

gold_dfs["monthly_revenue"].orderBy(
    "order_month"
).show(30, truncate=False)