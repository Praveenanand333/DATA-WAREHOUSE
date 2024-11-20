import pandas as pd
import mysql.connector
from mysql.connector import Error

# Connect to the operational database (MySQL)
try:
    source_conn = mysql.connector.connect(
        host="localhost",       # Your MySQL host (e.g., "localhost")
        user="your_username",   # Your MySQL username
        password="your_password", # Your MySQL password
        database="data_warehouse"  # Your operational database name
    )
    
    if source_conn.is_connected():
        print("Connected to the operational database")

    # Extract data from the operational database
    item_table = pd.read_sql("SELECT * FROM Item", source_conn)
    date_table = pd.read_sql("SELECT * FROM Date", source_conn)
    location_table = pd.read_sql("SELECT * FROM Location", source_conn)
    customer_table = pd.read_sql("SELECT * FROM Customer", source_conn)
    sales_table = pd.read_sql("SELECT * FROM Sales", source_conn)

except Error as e:
    print(f"Error: {e}")
finally:
    if source_conn.is_connected():
        source_conn.close()
        print("Source connection closed")

# Transformation Functions
def clean_item_table(df):
    # Remove duplicate rows based on ItemID
    df = df.drop_duplicates(subset="ItemID")
    # Fill missing prices with the average price
    df["Price"] = df["Price"].fillna(df["Price"].mean())
    return df

def clean_date_table(df):
    # Remove rows with missing years
    df = df.dropna(subset=["Year"])
    return df

def clean_location_table(df):
    # Fill missing countries with "Unknown"
    df["Country"] = df["Country"].fillna("Unknown")
    return df

def clean_customer_table(df):
    # Remove rows with invalid ages (e.g., negative values)
    df = df[df["Age"] > 0]
    return df

def clean_sales_table(df, valid_item_ids, valid_date_ids, valid_location_ids, valid_customer_ids):
    # Remove rows with invalid foreign keys
    df = df[df["ItemID"].isin(valid_item_ids)]
    df = df[df["DateID"].isin(valid_date_ids)]
    df = df[df["LocationID"].isin(valid_location_ids)]
    df = df[df["CustomerID"].isin(valid_customer_ids)]
    return df

# Clean dimension tables
item_table_cleaned = clean_item_table(item_table)
date_table_cleaned = clean_date_table(date_table)
location_table_cleaned = clean_location_table(location_table)
customer_table_cleaned = clean_customer_table(customer_table)

# Clean fact table
sales_table_cleaned = clean_sales_table(
    sales_table,
    valid_item_ids=item_table_cleaned["ItemID"],
    valid_date_ids=date_table_cleaned["DateID"],
    valid_location_ids=location_table_cleaned["LocationID"],
    valid_customer_ids=customer_table_cleaned["CustomerID"],
)

# Connect to the target data warehouse database (MySQL)
try:
    target_conn = mysql.connector.connect(
        host="localhost",       # Your MySQL host (e.g., "localhost")
        user="your_username",   # Your MySQL username
        password="your_password", # Your MySQL password
        database="data_warehouse_cleaned"  # Your cleaned data warehouse database name
    )

    if target_conn.is_connected():
        print("Connected to the target data warehouse")

    # Create a cursor for executing SQL queries
    cursor = target_conn.cursor()

    # Load cleaned data into the data warehouse (MySQL)
    # Ensure that the table already exists in MySQL, or use CREATE TABLE if necessary
    item_table_cleaned.to_sql("Item", target_conn, if_exists="replace", index=False)
    date_table_cleaned.to_sql("Date", target_conn, if_exists="replace", index=False)
    location_table_cleaned.to_sql("Location", target_conn, if_exists="replace", index=False)
    customer_table_cleaned.to_sql("Customer", target_conn, if_exists="replace", index=False)
    sales_table_cleaned.to_sql("Sales", target_conn, if_exists="replace", index=False)

except Error as e:
    print(f"Error: {e}")
finally:
    if target_conn.is_connected():
        target_conn.close()
        print("Target connection closed")

print("ETL process completed. Cleaned data saved to the data warehouse.")
