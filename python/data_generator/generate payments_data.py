
import random
import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="pass",
    database="ecommerce_db"
)

cursor = connection.cursor()

TARGET_PAYMENTS = 50_000
BATCH_SIZE = 1_000

payment_methods = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet"]
payment_statuses = ["Completed", "Pending", "Failed", "Refunded"]

cursor.execute("SELECT COUNT(*) FROM payments")
existing_payments = cursor.fetchone()[0]

payments_to_create = max(0, TARGET_PAYMENTS - existing_payments)

print(f"Existing payments: {existing_payments}")
print(f"Payments to generate: {payments_to_create}")

# Select orders that do not yet have a payment
cursor.execute("""
    SELECT o.order_id, o.total_amount
    FROM orders o
    LEFT JOIN payments p ON o.order_id = p.order_id
    WHERE p.order_id IS NULL
    ORDER BY o.order_id
""")

orders = cursor.fetchall()

if len(orders) < payments_to_create:
    raise ValueError(
        f"Need {payments_to_create} orders without payments, "
        f"but found only {len(orders)}."
    )

try:
    for start in range(0, payments_to_create, BATCH_SIZE):
        batch = []

        for order_id, total_amount in orders[start:start + BATCH_SIZE]:
            payment_method = random.choice(payment_methods)
            payment_status = random.choice(payment_statuses)

            # Keep payment amount aligned with the order total.
            amount = total_amount

            batch.append((
                order_id,
                amount,
                payment_method,
                payment_status
            ))

        cursor.executemany("""
            INSERT INTO payments
                (order_id, amount, payment_method, payment_status)
            VALUES (%s, %s, %s, %s)
        """, batch)

        connection.commit()
        print(f"Inserted {len(batch)} payments")

    print("Payment generation completed!")

except Exception:
    connection.rollback()
    raise

finally:
    cursor.close()
    connection.close()
