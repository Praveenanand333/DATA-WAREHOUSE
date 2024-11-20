import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import mysql.connector

fake = Faker()

# Parameters
num_items = 500
num_dates = 365
num_locations = 100
num_customers = 1000
num_sales = 100000

# Connect to MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="your_username",     # Replace with your MySQL username
    password="your_password", # Replace with your MySQL password
    database="operational_db" # Replace with your database name
)
cursor = conn.cursor()

# Drop tables if they exist (to avoid duplicates)
cursor.execute("DROP TABLE IF EXISTS Sales, Item, Date, Location, Customer;")

# Generate Dimension Tables
# Item Table with intentional duplicates and missing values
item_ids = [f"ITEM{i:05d}" for i in range(1, num_items + 1)]
item_names = [fake.word().capitalize() for _ in range(num_items)]
categories = [random.choice(["Electronics", "Clothing", "Grocery", "Furniture", "Books"]) for _ in range(num_items)]
prices = [round(random.uniform(10, 500), 2) for _ in range(num_items)]
# Introduce faults
item_ids[0] = item_ids[1]  # Duplicate ID
prices[5] = None  # Missing price
item_table = pd.DataFrame({"ItemID": item_ids, "ItemName": item_names, "Category": categories, "Price": prices})

# Create and insert data into Item table
cursor.execute("""
CREATE TABLE Item (
    ItemID VARCHAR(10) PRIMARY KEY,
    ItemName VARCHAR(255),
    Category VARCHAR(50),
    Price FLOAT
);
""")
for _, row in item_table.iterrows():
    cursor.execute("INSERT INTO Item (ItemID, ItemName, Category, Price) VALUES (%s, %s, %s, %s)",
                   tuple(row))

# Date Table with missing values
start_date = datetime(2023, 1, 1)
date_ids = [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range(num_dates)]
date_values = [start_date + timedelta(days=i) for i in range(num_dates)]
years = [date.year for date in date_values]
months = [date.month for date in date_values]
days = [date.day for date in date_values]
weekdays = [date.strftime("%A") for date in date_values]
# Introduce faults
years[10] = None  # Missing year
date_table = pd.DataFrame({"DateID": date_ids, "Year": years, "Month": months, "Day": days, "Weekday": weekdays})

# Create and insert data into Date table
cursor.execute("""
CREATE TABLE Date (
    DateID VARCHAR(10) PRIMARY KEY,
    Year INT,
    Month INT,
    Day INT,
    Weekday VARCHAR(10)
);
""")
for _, row in date_table.iterrows():
    cursor.execute("INSERT INTO Date (DateID, Year, Month, Day, Weekday) VALUES (%s, %s, %s, %s, %s)",
                   tuple(row))

# Location Table with invalid values
location_ids = [f"LOC{i:03d}" for i in range(1, num_locations + 1)]
cities = [fake.city() for _ in range(num_locations)]
states = [fake.state() for _ in range(num_locations)]
countries = ["USA"] * (num_locations - 5) + [None] * 5  # Missing country values
location_table = pd.DataFrame({"LocationID": location_ids, "City": cities, "State": states, "Country": countries})

# Create and insert data into Location table
cursor.execute("""
CREATE TABLE Location (
    LocationID VARCHAR(10) PRIMARY KEY,
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(50)
);
""")
for _, row in location_table.iterrows():
    cursor.execute("INSERT INTO Location (LocationID, City, State, Country) VALUES (%s, %s, %s, %s)",
                   tuple(row))

# Customer Table with inconsistent data
customer_ids = [f"CUST{i:05d}" for i in range(1, num_customers + 1)]
customer_names = [fake.name() for _ in range(num_customers)]
genders = [random.choice(["Male", "Female", "Unknown"]) for _ in range(num_customers)]
ages = [random.randint(18, 70) for _ in range(num_customers)]
# Introduce faults
ages[2] = -5  # Invalid age
customer_table = pd.DataFrame({"CustomerID": customer_ids, "CustomerName": customer_names, "Gender": genders, "Age": ages})

# Create and insert data into Customer table
cursor.execute("""
CREATE TABLE Customer (
    CustomerID VARCHAR(10) PRIMARY KEY,
    CustomerName VARCHAR(255),
    Gender VARCHAR(10),
    Age INT
);
""")
for _, row in customer_table.iterrows():
    cursor.execute("INSERT INTO Customer (CustomerID, CustomerName, Gender, Age) VALUES (%s, %s, %s, %s)",
                   tuple(row))

# Generate Fact Table with foreign key mismatches
sales_data = {
    "SaleID": [f"SALE{i:07d}" for i in range(1, num_sales + 1)],
    "DateID": random.choices(date_ids, k=num_sales),
    "ItemID": random.choices(item_ids + ["INVALID"], k=num_sales),  # Add some invalid foreign keys
    "LocationID": random.choices(location_ids, k=num_sales),
    "CustomerID": random.choices(customer_ids, k=num_sales),
    "Quantity": [random.randint(1, 20) for _ in range(num_sales)],
    "TotalPrice": [round(random.uniform(20, 1000), 2) for _ in range(num_sales)],
}
sales_table = pd.DataFrame(sales_data)

# Create and insert data into Sales table
cursor.execute("""
CREATE TABLE Sales (
    SaleID VARCHAR(10) PRIMARY KEY,
    DateID VARCHAR(10),
    ItemID VARCHAR(10),
    LocationID VARCHAR(10),
    CustomerID VARCHAR(10),
    Quantity INT,
    TotalPrice FLOAT
);
""")
for _, row in sales_table.iterrows():
    cursor.execute("INSERT INTO Sales (SaleID, DateID, ItemID, LocationID, CustomerID, Quantity, TotalPrice) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                   tuple(row))

# Commit and close the MySQL connection
conn.commit()
cursor.close()
conn.close()

print("Data generation completed with faults and saved to MySQL database.")
