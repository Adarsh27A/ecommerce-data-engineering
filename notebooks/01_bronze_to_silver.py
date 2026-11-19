# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

display(dbutils.fs.ls(
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/"
))

# COMMAND ----------

customers_path = (
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/"
    "customers/customers.csv"
)

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(customers_path)
)

display(customers_df.limit(10))

# COMMAND ----------

raw_path = "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net"

tables = {
    "customers": f"{raw_path}/customers/customers.csv",
    "products": f"{raw_path}/products/products.csv",
    "orders": f"{raw_path}/orders/orders.csv",
    "order_items": f"{raw_path}/order_items/order_items.csv",
    "payments": f"{raw_path}/payments/payments.csv",
}

dfs = {
    name: spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    for name, path in tables.items()
}

for name, df in dfs.items():
    print(f"\n--- {name} ---")
    print(f"Columns: {df.columns}")
    df.printSchema()
    display(df.limit(5))

# COMMAND ----------

expected_counts = {
    "customers": 10_000,
    "products": 1_000,
    "orders": 50_000,
    "order_items": 124_509,
    "payments": 50_000,
}

for name, df in dfs.items():
    actual = df.count()
    expected = expected_counts[name]
    status = "PASS" if actual == expected else "CHECK"
    print(f"{name}: actual={actual:,}, expected={expected:,} [{status}]")

# COMMAND ----------

from pyspark.sql import functions as F

for name, df in dfs.items():
    null_checks = df.select([
        F.count(
            F.when(F.col(c).isNull(), 1)
        ).alias(c)
        for c in df.columns
    ])
    print(f"\nNull counts — {name}")
    display(null_checks)

# COMMAND ----------

relationships = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("payments", "order_id", "orders", "order_id"),
]

for child_name, child_key, parent_name, parent_key in relationships:
    child = dfs[child_name].alias("child")
    parent = dfs[parent_name].select(parent_key).distinct().alias("parent")

    orphans = child.join(
        parent,
        F.col(f"child.{child_key}") == F.col(f"parent.{parent_key}"),
        "left_anti"
    )

    print(
        f"{child_name}.{child_key} -> {parent_name}.{parent_key}: "
        f"{orphans.count()} orphan rows"
    )

# COMMAND ----------

for name, df in dfs.items():
    print(f"\n--- {name} ---")
    df.printSchema()

# COMMAND ----------

for name, df in dfs.items():
    print(f"\n--- {name} ---")
    df.printSchema()

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType, DecimalType
)

schemas = {
    "customers": StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]),

    "products": StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DecimalType(12, 2), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
    ]),

    "orders": StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("total_amount", DecimalType(12, 2), True),
        StructField("status", StringType(), True),
    ]),

    "order_items": StructType([
        StructField("order_item_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(12, 2), True),
    ]),

    "payments": StructType([
        StructField("payment_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("payment_date", TimestampType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
    ])
}

print("Defined schemas for:", list(schemas.keys()))

# COMMAND ----------

from pyspark.sql import functions as F

dfs = {
    name: (
        spark.read
        .option("header", True)
        .schema(schemas[name])
        .csv(path)
    )
    for name, path in tables.items()
}

for name, df in dfs.items():
    print(f"\n--- {name} ---")
    df.printSchema()
    print(f"Rows: {df.count():,}")

# COMMAND ----------

from pyspark.sql import functions as F

dfs["orders"] = dfs["orders"].withColumn(
    "status", F.initcap(F.trim(F.col("status")))
)

dfs["payments"] = (
    dfs["payments"]
    .withColumn("payment_status", F.upper(F.trim(F.col("payment_status"))))
    .withColumn("payment_method", F.initcap(F.trim(F.col("payment_method"))))
)

dfs["products"] = (
    dfs["products"]
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("product_name", F.trim(F.col("product_name")))
)

dfs["customers"] = (
    dfs["customers"]
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("first_name", F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
)

print("Text standardization complete.")

# COMMAND ----------

from pyspark.sql import functions as F

checks = {
    "products_invalid_price": dfs["products"].filter(
        F.col("price").isNull() | (F.col("price") < 0)
    ),
    "products_invalid_stock": dfs["products"].filter(
        F.col("stock_quantity").isNull() | (F.col("stock_quantity") < 0)
    ),
    "orders_invalid_total": dfs["orders"].filter(
        F.col("total_amount").isNull() | (F.col("total_amount") < 0)
    ),
    "order_items_invalid_quantity": dfs["order_items"].filter(
        F.col("quantity").isNull() | (F.col("quantity") <= 0)
    ),
    "order_items_invalid_price": dfs["order_items"].filter(
        F.col("unit_price").isNull() | (F.col("unit_price") < 0)
    ),
    "payments_invalid_amount": dfs["payments"].filter(
        F.col("amount").isNull() | (F.col("amount") < 0)
    ),
}

for name, bad_rows in checks.items():
    print(f"{name}: {bad_rows.count()} suspicious rows")

# COMMAND ----------

from pyspark.sql import functions as F

relationships = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("payments", "order_id", "orders", "order_id"),
]

for child_name, child_key, parent_name, parent_key in relationships:
    child = dfs[child_name].alias("child")
    parent = dfs[parent_name].select(parent_key).distinct().alias("parent")

    orphans = child.join(
        parent,
        F.col(f"child.{child_key}") == F.col(f"parent.{parent_key}"),
        "left_anti"
    )

    print(f"{child_name}.{child_key} -> {parent_name}.{parent_key}: "
          f"{orphans.count()} orphan rows")

# COMMAND ----------

silver_path = (
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/silver"
)

for name, df in dfs.items():
    (
        df.write
        .mode("overwrite")
        .parquet(f"{silver_path}/{name}")
    )
    print(f"Written silver dataset: {name}")

# COMMAND ----------

silver_customers_path = (
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/"
    "silver/customers"
)

silver_customers_df = spark.read.parquet(silver_customers_path)

display(silver_customers_df.limit(5))

# COMMAND ----------

silver_path = (
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/silver"
)

silver_dfs = {
    name: spark.read.parquet(f"{silver_path}/{name}")
    for name in ["customers", "products", "orders", "order_items", "payments"]
}

print("Silver datasets loaded successfully:")
for name in silver_dfs:
    print(name)

# COMMAND ----------

from pyspark.sql import functions as F

orders = silver_dfs["orders"]

gold_monthly_revenue = (
    orders
    .filter(F.col("status").isin("Completed", "Shipped"))
    .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("order_month")
    .agg(
        F.sum("total_amount").alias("monthly_revenue"),
        F.countDistinct("order_id").alias("order_count")
    )
    .orderBy("order_month")
)

display(gold_monthly_revenue)

# COMMAND ----------

# from pyspark.sql import functions as F

gold_category_sales = (
    silver_dfs["order_items"]
    .join(silver_dfs["products"], "product_id", "inner")
    .join(silver_dfs["orders"], "order_id", "inner")
    .filter(F.col("status").isin("Completed", "Shipped"))
    .groupBy("category")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum(
            F.col("quantity") * F.col("unit_price")
        ).alias("sales_revenue")
    )
    .orderBy(F.desc("sales_revenue"))
)

display(gold_category_sales)

# COMMAND ----------

gold_path = (
    "abfss://raw-data@ecomdedatalake2026.dfs.core.windows.net/gold"
)

(
    gold_monthly_revenue.write
    .mode("overwrite")
    .parquet(f"{gold_path}/monthly_revenue")
)

(
    gold_category_sales.write
    .mode("overwrite")
    .parquet(f"{gold_path}/category_sales")
)

print("Both Gold datasets saved successfully.")

# COMMAND ----------

from pyspark.sql import functions as F

gold_top_products = (
    silver_dfs["order_items"]
    .join(silver_dfs["products"], "product_id", "inner")
    .join(silver_dfs["orders"], "order_id", "inner")
    .filter(F.col("status").isin("Completed", "Shipped"))
    .groupBy("product_id", "product_name")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum(
            F.col("quantity") * F.col("unit_price")
        ).alias("sales_revenue")
    )
    .orderBy(F.desc("units_sold"))
    .limit(10)
)

display(gold_top_products)

# COMMAND ----------

(
    gold_top_products.write
    .mode("overwrite")
    .parquet(f"{gold_path}/top_products")
)

print("Gold top-products dataset saved successfully.")

# COMMAND ----------

gold_monthly_revenue.createOrReplaceTempView("gold_monthly_revenue")
gold_category_sales.createOrReplaceTempView("gold_category_sales")
gold_top_products.createOrReplaceTempView("gold_top_products")

print("Gold temporary views created.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_monthly_revenue
# MAGIC ORDER BY order_month;

# COMMAND ----------

from pyspark.sql import functions as F

gold_payment_analysis = (
    silver_dfs["payments"]
    .groupBy("payment_status")
    .agg(
        F.count("payment_id").alias("payment_count"),
        F.sum("amount").alias("total_payment_amount")
    )
    .orderBy(F.desc("payment_count"))
)

display(gold_payment_analysis)

# COMMAND ----------

(
    gold_payment_analysis.write
    .mode("overwrite")
    .parquet(f"{gold_path}/payment_analysis")
)

print("Gold payment analysis saved.")

# COMMAND ----------

gold_datasets = {
    "gold_monthly_revenue": gold_monthly_revenue,
    "gold_category_sales": gold_category_sales,
    "gold_top_products": gold_top_products,
    "gold_payment_analysis": gold_payment_analysis,
}

for name, df in gold_datasets.items():
    df.createOrReplaceTempView(name)

print("All four Gold SQL views registered.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_payment_analysis
# MAGIC ORDER BY payment_count DESC;