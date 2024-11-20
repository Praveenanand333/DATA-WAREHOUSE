from flask import Flask, jsonify, request
import mysql.connector
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Connect to the data warehouse
def connect_to_warehouse():
    try:
        conn = mysql.connector.connect(
            host="localhost",       # Your MySQL host (e.g., "localhost")
            user="root",            # Your MySQL username
            password="",           # Your MySQL password
            database="data_warehouse"  # Your cleaned data warehouse name
        )
        if conn.is_connected():
            print("Connected to the data warehouse")
        return conn
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None


# OLAP Operations
def all_data(conn):
    query = """
        SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month, d.Day,l.City, l.State
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Date d ON s.DateID = d.DateID
        JOIN Location l ON s.LocationID = l.LocationID;
    """
    return pd.read_sql(query, conn)
# 1. Slicing: Sales data for a specific year and month
def slice_data(conn, year, month, day, city, state, country, slicedimension):
    if slicedimension == "city":
        query = """
            SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
            FROM Sales s
            JOIN Item i ON s.ItemID = i.ItemID
            JOIN Date d ON s.DateID = d.DateID
            JOIN Location l ON s.LocationID = l.LocationID
            WHERE l.City = %s;
        """
        return pd.read_sql(query, conn, params=(city,))
    
    elif slicedimension == "state":
        query = """
            SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
            FROM Sales s
            JOIN Item i ON s.ItemID = i.ItemID
            JOIN Date d ON s.DateID = d.DateID
            JOIN Location l ON s.LocationID = l.LocationID
            WHERE l.State = %s;
        """
        return pd.read_sql(query, conn, params=(state,))
    
    elif slicedimension == "country":
        query = """
            SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State, l.Country
            FROM Sales s
            JOIN Item i ON s.ItemID = i.ItemID
            JOIN Date d ON s.DateID = d.DateID
            JOIN Location l ON s.LocationID = l.LocationID
            WHERE l.Country = %s;
        """
        return pd.read_sql(query, conn, params=(country,))
    
    elif slicedimension == "year":
        query = """
            SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
            FROM Sales s
            JOIN Item i ON s.ItemID = i.ItemID
            JOIN Date d ON s.DateID = d.DateID
            JOIN Location l ON s.LocationID = l.LocationID
            WHERE d.Year = %s;
        """
        return pd.read_sql(query, conn, params=(year,))
    
    elif slicedimension == "month":
        query = """
            SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
            FROM Sales s
            JOIN Item i ON s.ItemID = i.ItemID
            JOIN Date d ON s.DateID = d.DateID
            JOIN Location l ON s.LocationID = l.LocationID
            WHERE d.Month = %s AND d.Year = %s;
        """
        return pd.read_sql(query, conn, params=(month, year))
    
    elif slicedimension == "day":
        query = """
            SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month, d.Day, l.City, l.State
            FROM Sales s
            JOIN Item i ON s.ItemID = i.ItemID
            JOIN Date d ON s.DateID = d.DateID
            JOIN Location l ON s.LocationID = l.LocationID
            WHERE d.Day = %s AND d.Month = %s AND d.Year = %s;
        """
        return pd.read_sql(query, conn, params=(day, month, year))
    
    else:
        return None
    

# 2. Dicing: Sales data based on selected dimension (Sales, Time, Location)
def dice_data(conn, year, month, day, city, state, country, dicedimension1, dicedimension2):
    if dicedimension1 == "city":
        if dicedimension2 == "year":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.City = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(city, year))

        elif dicedimension2 == "month":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.City = %s AND d.Month = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(city, month, year))

        elif dicedimension2 == "day":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, d.Day, l.City, l.State
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.City = %s AND d.Day = %s AND d.Month = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(city, day, month, year))

    elif dicedimension1 == "state":
        if dicedimension2 == "year":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.State = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(state, year))

        elif dicedimension2 == "month":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.State = %s AND d.Month = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(state, month, year))

        elif dicedimension2 == "day":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, d.Day, l.City, l.State
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.State = %s AND d.Day = %s AND d.Month = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(state, day, month, year))

    elif dicedimension1 == "country":
        if dicedimension2 == "year":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State, l.Country
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.Country = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(country, year))

        elif dicedimension2 == "month":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, l.City, l.State, l.Country
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.Country = %s AND d.Month = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(country, month, year))

        elif dicedimension2 == "day":
            query = """
                SELECT i.ItemName, s.Quantity, s.TotalPrice, d.Year, d.Month,d.Day, d.Day, l.City, l.State, l.Country
                FROM Sales s
                JOIN Item i ON s.ItemID = i.ItemID
                JOIN Date d ON s.DateID = d.DateID
                JOIN Location l ON s.LocationID = l.LocationID
                WHERE l.Country = %s AND d.Day = %s AND d.Month = %s AND d.Year = %s;
            """
            return pd.read_sql(query, conn, params=(country, day, month, year))

    else:
        return None
 # Return an empty DataFrame for invalid dimension

# 3. Roll-up: Aggregated sales by year
def roll_up_data(conn, year, month):
    query = """
        SELECT d.Year, SUM(s.Quantity) AS TotalQuantity, SUM(s.TotalPrice) AS TotalSales
        FROM Sales s
        JOIN Date d ON s.DateID = d.DateID
        WHERE d.Year = %s AND d.Month = %s
        GROUP BY d.Year;
    """
    return pd.read_sql(query, conn, params=(year, month))

# 4. Drill-down: Sales for an item across locations
def drill_down_data(conn, item_name):
    query = """
        SELECT l.City, l.State, s.Quantity, s.TotalPrice
        FROM Sales s
        JOIN Item i ON s.ItemID = i.ItemID
        JOIN Location l ON s.LocationID = l.LocationID
        WHERE i.ItemName = %s;
    """
    return pd.read_sql(query, conn, params=(item_name,))


# API Endpoints

# Endpoint to fetch 3D sales data (can be used for slicing, dicing, drill-down)
@app.route('/fetch_sales_data', methods=['GET'])
def fetch_sales_data():
    year = request.args.get('year', default=2023, type=int)
    month = request.args.get('month', default=5, type=int)
    day = request.args.get('day',default=1,type=int)
    city = request.args.get('city', default=None, type=str)
    state = request.args.get('state', default=None, type=str)
    country = request.args.get('country', default=None, type=str)
    operation = request.args.get('operation', default="slice", type=str)
    slicedimension = request.args.get('slicedimension', default=None, type=str)
    dicedimension1 = request.args.get('dicedimension1', default=None, type=str)
    dicedimension2 = request.args.get('dicedimension2', default=None, type=str)
    conn = connect_to_warehouse()

    if conn is None:
        return jsonify({"error": "Failed to connect to database"}), 500

    if operation == "og":
        sales_data=all_data(conn);
    elif operation == "slice":
        sales_data = slice_data(conn, year, month,day,city,state,country,slicedimension)
    elif operation == "dice":
        sales_data = dice_data(conn, year, month,day,city,state,country,dicedimension1,dicedimension2 )
    elif operation == "rollup":
        sales_data = roll_up_data(conn, year, month)
    elif operation == "drilldown":
        sales_data = drill_down_data(conn, item_name)
    else:
        return jsonify({"error": "Invalid operation"}), 400

    conn.close()
    return jsonify(sales_data.to_dict(orient="records"))

if __name__ == "__main__":
    app.run(debug=True)
