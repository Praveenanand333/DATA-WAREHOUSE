import mysql.connector
import pandas as pd


def connect_to_warehouse():
    try:
        conn = mysql.connector.connect(
            host="localhost",       
            user="root",   
            password="",  
            database="data_warehouse"  
        )
        if conn.is_connected():
            print("Connected to the data warehouse")
        return conn
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None





def slice_data(conn, year, month):
    query = """
        SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Date d ON s.DateID = d.DateID
        WHERE d.Year = %s AND d.Month = %s;
    """
    return pd.read_sql(query, conn, params=(year, month))


def dice_data(conn, year, month, category):
    query = """
        SELECT i.ItemName, i.Category, s.Quantity, s.TotalPrice, d.Year, d.Month
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Date d ON s.DateID = d.DateID
        WHERE d.Year = %s AND d.Month = %s AND i.Category = %s;
    """
    return pd.read_sql(query, conn, params=(year, month, category))


def drill_down_data(conn, item_name):
    query = """
        SELECT l.City, l.State, s.Quantity, s.TotalPrice
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Location l ON s.LocationID = l.LocationID
        WHERE i.ItemName = %s;
    """
    return pd.read_sql(query, conn, params=(item_name,))


def roll_up_data(conn, year, month):
    query = """
        SELECT d.Year, SUM(s.Quantity) AS TotalQuantity, SUM(s.TotalPrice) AS TotalSales
        FROM Sales s
        JOIN Date d ON s.DateID = d.DateID
        WHERE d.Year = %s AND d.Month = %s
        GROUP BY d.Year;
    """
    return pd.read_sql(query, conn, params=(year, month))



def olap_server():
    conn = connect_to_warehouse()
    
    if conn is None:
        return

    
    print("Slicing: Sales data for May 2023")
    slice_result = slice_data(conn, 2023, 5)
    print(slice_result)
    
    
    print("\nDicing: Sales data for May 2023, Electronics category")
    dice_result = dice_data(conn, 2023, 5, 'Electronics')
    print(dice_result)
    
    
    print("\nDrill-down: Sales data for Laptop in different locations")
    drill_down_result = drill_down_data(conn, 'Laptop')
    print(drill_down_result)
    
    
    print("\nRoll-up: Total sales for May 2023 rolled up by year")
    roll_up_result = roll_up_data(conn, 2023, 5)
    print(roll_up_result)

    
    if conn.is_connected():
        conn.close()
        print("Connection closed")



if __name__ == "__main__":
    olap_server()
