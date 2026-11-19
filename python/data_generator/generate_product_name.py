
# -------------------------------
import random
import mysql.connector
from datetime import date, timedelta

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="pass",
    database="ecommerce_db"
)

cursor = connection.cursor()
# -------------------------------

product_names = [
    "Laptop", "Wireless Mouse", "Mechanical Keyboard",
    "Monitor", "USB Cable", "Headphones",
    "Webcam", "Smartphone", "Tablet", "Power Bank",
    "Office Chair", "Desk Lamp", "Notebook",
    "Water Bottle", "Backpack", "Running Shoes",
    "Coffee Maker", "Blender", "Smart Watch", "Speaker"
]

categories = [
    "Electronics", "Accessories", "Furniture",
    "Stationery", "Home", "Fashion", "Sports"
]

TARGET_PRODUCTS = 1000
BATCH_SIZE = 500

cursor.execute("SELECT COUNT(*) FROM products")
existing_products = cursor.fetchone()[0]

products_to_create = max(0, TARGET_PRODUCTS - existing_products)

print(f"Existing products: {existing_products}")
print(f"Products to generate: {products_to_create}")

for start in range(0, products_to_create, BATCH_SIZE):
    batch = []

    for _ in range(min(BATCH_SIZE, products_to_create - start)):
        product_name = random.choice(product_names)
        category = random.choice(categories)
        price = round(random.uniform(100, 100000), 2)
        stock_quantity = random.randint(0, 500)

        batch.append((
            product_name,
            category,
            price,
            stock_quantity
        ))

    cursor.executemany(
        """
        INSERT INTO products
            (product_name, category, price, stock_quantity)
        VALUES (%s, %s, %s, %s)
        """,
        batch
    )

    connection.commit()
    print(f"Inserted {len(batch)} products in this batch")

print("Product generation completed!")
