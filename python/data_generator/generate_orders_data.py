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
# Generate orders
# -------------------------------

from datetime import datetime, timedelta

TARGET_ORDERS = 50_000
BATCH_SIZE = 1000

cursor.execute("SELECT COUNT(*) FROM orders")
existing_orders = cursor.fetchone()[0]

orders_to_create = max(0, TARGET_ORDERS - existing_orders)

print(f"Existing orders: {existing_orders}")
print(f"Orders to generate: {orders_to_create}")

# Get valid customer IDs
cursor.execute("SELECT customer_id FROM customers")
customer_ids = [row[0] for row in cursor.fetchall()]

statuses = [
    "Pending",
    "Processing",
    "Shipped",
    "Delivered",
    "Cancelled"
]

for start in range(0, orders_to_create, BATCH_SIZE):
    batch = []

    for _ in range(min(BATCH_SIZE, orders_to_create - start)):
        customer_id = random.choice(customer_ids)

        order_date = datetime.now() - timedelta(
            days=random.randint(0, 730)
        )

        total_amount = round(random.uniform(200, 150000), 2)
        status = random.choice(statuses)

        batch.append((
            customer_id,
            order_date,
            total_amount,
            status
        ))

    cursor.executemany(
        """
        INSERT INTO orders
            (customer_id, order_date, total_amount, status)
        VALUES (%s, %s, %s, %s)
        """,
        batch
    )

    connection.commit()
    print(f"Inserted {len(batch)} orders in this batch")

print("Order generation completed!")
