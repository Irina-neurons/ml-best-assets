import pyodbc

# Use the DSN (Data Source Name) defined in your odbc.ini
conn_string = "DSN=PostgreSQL"

try:
    # Establish the connection
    conn = pyodbc.connect(conn_string)
    print("Connection successful!")

    # Create a cursor and test a query
    cursor = conn.cursor()
    cursor.execute("SELECT version();")  # Query to check PostgreSQL version
    version = cursor.fetchone()
    print(f"PostgreSQL version: {version[0]}")

    # Close the connection
    conn.close()
except Exception as e:
    print(f"Error connecting to the database: {e}")
