
import random
import mysql.connector

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="pass",
    database="ecommerce_db"
)

cursor = connection.cursor()

TARGET_ITEMS = 120_000
BATCH_SIZE = 1_000

# Check existing records so rerunning won't duplicate up to the target
cursor.execute("SELECT COUNT(*) FROM order_items")
existing_items = cursor.fetchone()[0]

items_to_create = max(0, TARGET_ITEMS - existing_items)

print(f"Existing order items: {existing_items}")
print(f"Items to generate: {items_to_create}")

# Load valid order IDs and product prices
cursor.execute("SELECT order_id FROM orders")
order_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT product_id, price FROM products")
products = cursor.fetchall()

if not order_ids or not products:
    raise ValueError("Orders and products must exist before generating order items.")

try:
    for start in range(0, items_to_create, BATCH_SIZE):
        batch = []

        for _ in range(min(BATCH_SIZE, items_to_create - start)):
            order_id = random.choice(order_ids)
            product_id, unit_price = random.choice(products)
            quantity = random.randint(1, 5)

            batch.append((
                order_id,
                product_id,
                quantity,
                unit_price
            ))

        cursor.executemany(
            """
            INSERT INTO order_items
                (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
            """,
            batch
        )

        connection.commit()
        print(f"Inserted {len(batch)} items")

    print("Order-item generation completed!")

except Exception:
    connection.rollback()
    raise

finally:
    cursor.close()
    connection.close()
