
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

# Sample data for generating customers
first_names = [
    "Aarav", "Aditya", "Arjun", "Ishaan", "Rahul",
    "Rohan", "Vikram", "Priya", "Ananya", "Diya",
    "Neha", "Sneha", "Kavya", "Meera", "Aditi"
]

last_names = [
    "Sharma", "Patel", "Kumar", "Singh", "Verma",
    "Gupta", "Reddy", "Nair", "Joshi", "Mehta"
]

cities = [
    "Mumbai", "Pune", "Delhi", "Bengaluru", "Hyderabad",
    "Chennai", "Kolkata", "Jaipur", "Ahmedabad", "Nashik"
]

TARGET_CUSTOMERS = 10_000

# Check how many customers already exist
cursor.execute("SELECT COUNT(*) FROM customers")
existing_count = cursor.fetchone()[0]

rows_to_create = max(0, TARGET_CUSTOMERS - existing_count)

print(f"Existing customers: {existing_count}")
print(f"Customers to generate: {rows_to_create}")

# Generate customers in batches
batch_size = 1000

for start in range(0, rows_to_create, batch_size):
    batch = []

    for _ in range(min(batch_size, rows_to_create - start)):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        city = random.choice(cities)

        # Generate a unique email using a random identifier
        email = f"customer_{random.getrandbits(64):016x}@example.com"

        created_at = date.today() - timedelta(
            days=random.randint(0, 365 * 5)
        )

        batch.append((
            first_name,
            last_name,
            email,
            city,
            created_at
        ))

    cursor.executemany(
        """
        INSERT INTO customers
            (first_name, last_name, email, city, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        batch
    )

    connection.commit()
    print(f"Inserted {len(batch)} customers in this batch")

print("Customer generation completed!")

cursor.close()
connection.close()
