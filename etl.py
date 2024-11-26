import pandas as pd
import mysql.connector
from mysql.connector import Error


try:
    source_conn = mysql.connector.connect(
        host="localhost",       
        user="root",   
        password="", 
        database="operational_db"  
    )
    
    if source_conn.is_connected():
        print("Connected to the operational database")

    
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


def clean_item_table(df):
    
    df = df.drop_duplicates(subset="ItemID")
    
    df["Price"] = df["Price"].fillna(df["Price"].mean())
    return df

def clean_date_table(df):
    
    df = df.dropna(subset=["Year"])
    return df

def clean_location_table(df):
    
    df["Country"] = df["Country"].fillna("Unknown")
    return df

def clean_customer_table(df):
    
    df = df[df["Age"] > 0]
    return df

def clean_sales_table(df, valid_item_ids, valid_date_ids, valid_location_ids, valid_customer_ids):
    
    df = df[df["ItemID"].isin(valid_item_ids)]
    df = df[df["DateID"].isin(valid_date_ids)]
    df = df[df["LocationID"].isin(valid_location_ids)]
    df = df[df["CustomerID"].isin(valid_customer_ids)]
    
    
    df = df.drop_duplicates(subset="SaleID")
    return df


item_table_cleaned = clean_item_table(item_table)
date_table_cleaned = clean_date_table(date_table)
location_table_cleaned = clean_location_table(location_table)
customer_table_cleaned = clean_customer_table(customer_table)


sales_table_cleaned = clean_sales_table(
    sales_table,
    valid_item_ids=item_table_cleaned["ItemID"],
    valid_date_ids=date_table_cleaned["DateID"],
    valid_location_ids=location_table_cleaned["LocationID"],
    valid_customer_ids=customer_table_cleaned["CustomerID"],
)


try:
    target_conn = mysql.connector.connect(
        host="localhost",       
        user="root",   
        password="", 
        database="data_warehouse"  
    )

    if target_conn.is_connected():
        print("Connected to the target data warehouse")

    
    cursor = target_conn.cursor()

    

    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Item (
            ItemID VARCHAR(10) PRIMARY KEY,
            ItemName VARCHAR(255),
            Category VARCHAR(50),
            Price FLOAT
        );
    """)

    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Date (
            DateID VARCHAR(10) PRIMARY KEY,
            Year INT,
            Month INT,
            Day INT,
            Weekday VARCHAR(10)
        );
    """)

    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Location (
            LocationID VARCHAR(10) PRIMARY KEY,
            City VARCHAR(255),
            State VARCHAR(255),
            Country VARCHAR(50)
        );
    """)

    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Customer (
            CustomerID VARCHAR(10) PRIMARY KEY,
            CustomerName VARCHAR(255),
            Gender VARCHAR(10),
            Age INT
        );
    """)

    
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

    
    target_conn.commit()

except Error as e:
    print(f"Error: {e}")
finally:
    if target_conn.is_connected():
        target_conn.close()
        print("Target connection closed")

print("ETL process completed. Cleaned data saved to the data warehouse.")
