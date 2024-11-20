import mysql.connector
import pandas as pd

# Connect to the data warehouse
def connect_to_warehouse():
    try:
        conn = mysql.connector.connect(
            host="localhost",       # Your MySQL host (e.g., "localhost")
            user="root",   # Your MySQL username
            password="",  # Your MySQL password
            database="data_warehouse"  # Your cleaned data warehouse name
        )
        if conn.is_connected():
            print("Connected to the data warehouse")
        return conn
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None


# OLAP Operations (Slicing, Dicing, Drill-down, Roll-up)

# 1. Slicing (Example: Fetch sales for a specific year and month)
def slice_data(conn, year, month):
    query = """
        SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Date d ON s.DateID = d.DateID
        WHERE d.Year = %s AND d.Month = %s;
    """
    return pd.read_sql(query, conn, params=(year, month))

# 2. Dicing (Example: Fetch sales for a specific year, month, and item category)
def dice_data(conn, year, month, category):
    query = """
        SELECT i.ItemName, i.Category, s.Quantity, s.TotalPrice, d.Year, d.Month
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Date d ON s.DateID = d.DateID
        WHERE d.Year = %s AND d.Month = %s AND i.Category = %s;
    """
    return pd.read_sql(query, conn, params=(year, month, category))

# 3. Drill-down (Example: Fetch sales for a specific item across different locations)
def drill_down_data(conn, item_name):
    query = """
        SELECT l.City, l.State, s.Quantity, s.TotalPrice
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Location l ON s.LocationID = l.LocationID
        WHERE i.ItemName = %s;
    """
    return pd.read_sql(query, conn, params=(item_name,))

# 4. Roll-up (Example: Fetch sales for a specific month and roll-up by year)
def roll_up_data(conn, year, month):
    query = """
        SELECT d.Year, SUM(s.Quantity) AS TotalQuantity, SUM(s.TotalPrice) AS TotalSales
        FROM Sales s
        JOIN Date d ON s.DateID = d.DateID
        WHERE d.Year = %s AND d.Month = %s
        GROUP BY d.Year;
    """
    return pd.read_sql(query, conn, params=(year, month))


# Main OLAP server function
def olap_server():
    conn = connect_to_warehouse()
    
    if conn is None:
        return

    # Example 1: Slicing - Sales data for 2023, month 5
    print("Slicing: Sales data for May 2023")
    slice_result = slice_data(conn, 2023, 5)
    print(slice_result)
    
    # Example 2: Dicing - Sales data for May 2023, item category 'Electronics'
    print("\nDicing: Sales data for May 2023, Electronics category")
    dice_result = dice_data(conn, 2023, 5, 'Electronics')
    print(dice_result)
    
    # Example 3: Drill-down - Sales data for item 'Laptop' across different locations
    print("\nDrill-down: Sales data for Laptop in different locations")
    drill_down_result = drill_down_data(conn, 'Laptop')
    print(drill_down_result)
    
    # Example 4: Roll-up - Total sales for May 2023 rolled up by year
    print("\nRoll-up: Total sales for May 2023 rolled up by year")
    roll_up_result = roll_up_data(conn, 2023, 5)
    print(roll_up_result)

    # Close the connection
    if conn.is_connected():
        conn.close()
        print("Connection closed")


# Run the OLAP server
if __name__ == "__main__":
    olap_server()
