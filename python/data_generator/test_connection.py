
import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="pass",
    database="ecommerce_db"
)

if connection.is_connected():
    print("Successfully connected to ecommerce_db!")

cursor= connection.cursor()

tables=['customers','orders','order_items','payments','products']

for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
    count=cursor.fetchone()[0]
    print(f"{table} : {count} rows")

    cursor.close
    connection.close
