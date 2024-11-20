import pandas as pd
import mysql.connector
from mysql.connector import Error

# Connect to the operational database (MySQL)
try:
    source_conn = mysql.connector.connect(
        host="localhost",       # Your MySQL host (e.g., "localhost")
        user="root",   # Your MySQL username
        password="", # Your MySQL password
        database="operational_db"  # Your operational database name
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
    
    # Remove duplicate SaleID values to avoid primary key violations
    df = df.drop_duplicates(subset="SaleID")
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
        user="root",   # Your MySQL username
        password="", # Your MySQL password
        database="data_warehouse"  # Your cleaned data warehouse database name
    )

    if target_conn.is_connected():
        print("Connected to the target data warehouse")

    # Create a cursor for executing SQL queries
    cursor = target_conn.cursor()

    # Create tables in the target data warehouse with proper key constraints

    # Item Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Item (
            ItemID VARCHAR(10) PRIMARY KEY,
            ItemName VARCHAR(255),
            Category VARCHAR(50),
            Price FLOAT
        );
    """)

    # Date Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Date (
            DateID VARCHAR(10) PRIMARY KEY,
            Year INT,
            Month INT,
            Day INT,
            Weekday VARCHAR(10)
        );
    """)

    # Location Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Location (
            LocationID VARCHAR(10) PRIMARY KEY,
            City VARCHAR(255),
            State VARCHAR(255),
            Country VARCHAR(50)
        );
    """)

    # Customer Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Customer (
            CustomerID VARCHAR(10) PRIMARY KEY,
            CustomerName VARCHAR(255),
            Gender VARCHAR(10),
            Age INT
        );
    """)

    # Sales Table (Fact Table) with foreign keys to enforce referential integrity
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Sales (
            SaleID VARCHAR(10) PRIMARY KEY,
            DateID VARCHAR(10),
            ItemID VARCHAR(10),
            LocationID VARCHAR(10),
            CustomerID VARCHAR(10),
            Quantity INT,
            TotalPrice FLOAT,
            FOREIGN KEY (DateID) REFERENCES Date(DateID),
            FOREIGN KEY (ItemID) REFERENCES Item(ItemID),
            FOREIGN KEY (LocationID) REFERENCES Location(LocationID),
            FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
        );
    """)

    # Insert cleaned data into the target data warehouse
    for _, row in item_table_cleaned.iterrows():
        cursor.execute("INSERT INTO Item (ItemID, ItemName, Category, Price) VALUES (%s, %s, %s, %s)",
                       (row["ItemID"], row["ItemName"], row["Category"], row["Price"]))

    for _, row in date_table_cleaned.iterrows():
        cursor.execute("INSERT INTO Date (DateID, Year, Month, Day, Weekday) VALUES (%s, %s, %s, %s, %s)",
                       (row["DateID"], row["Year"], row["Month"], row["Day"], row["Weekday"]))

    for _, row in location_table_cleaned.iterrows():
        cursor.execute("INSERT INTO Location (LocationID, City, State, Country) VALUES (%s, %s, %s, %s)",
                       (row["LocationID"], row["City"], row["State"], row["Country"]))

    for _, row in customer_table_cleaned.iterrows():
        cursor.execute("INSERT INTO Customer (CustomerID, CustomerName, Gender, Age) VALUES (%s, %s, %s, %s)",
                       (row["CustomerID"], row["CustomerName"], row["Gender"], row["Age"]))

    for _, row in sales_table_cleaned.iterrows():
        cursor.execute("""
            INSERT INTO Sales (SaleID, DateID, ItemID, LocationID, CustomerID, Quantity, TotalPrice)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row["SaleID"], row["DateID"], row["ItemID"], row["LocationID"], row["CustomerID"], row["Quantity"], row["TotalPrice"]))

    # Commit the transaction
    target_conn.commit()

except Error as e:
    print(f"Error: {e}")
finally:
    if target_conn.is_connected():
        target_conn.close()
        print("Target connection closed")

print("ETL process completed. Cleaned data saved to the data warehouse.")
