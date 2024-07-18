import pandas as pd
import sqlite3

spreadsheet_0_path = 'data/shipping_data_0.csv'
spreadsheet_1_path = 'data/shipping_data_1.csv'
spreadsheet_2_path = 'data/shipping_data_2.csv'

db_path = 'shipment_database.db'

spreadsheet_0 = pd.read_csv(spreadsheet_0_path)
spreadsheet_1 = pd.read_csv(spreadsheet_1_path)
spreadsheet_2 = pd.read_csv(spreadsheet_2_path)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def create_tables():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS table_0 (
        origin_warehouse TEXT,
        destination_store TEXT,
        product TEXT,
        on_time INTEGER,
        product_quantity INTEGER,
        driver_identifier TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS table_1 (
        shipment_identifier TEXT,
        product TEXT,
        on_time INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS table_2 (
        shipment_identifier TEXT,
        origin_warehouse TEXT,
        destination_store TEXT,
        driver_identifier TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS combined_table (
        shipment_identifier TEXT,
        product TEXT,
        on_time INTEGER,
        origin_warehouse TEXT,
        destination_store TEXT,
        driver_identifier TEXT
    )
    ''')

# Insert data from spreadsheet 0
def insert_spreadsheet_0():
    for _, row in spreadsheet_0.iterrows():
        cursor.execute('''
        INSERT INTO table_0 (origin_warehouse,  
                            destination_store,
                            product,
                            on_time,
                            product_quantity,
                            driver_identifier)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (row['origin_warehouse'], row['destination_store'], row['product'],
              row['on_time'], row['product_quantity'], row['driver_identifier']))

# Insert data from spreadsheets 1
def insert_spreadsheet_1():
    for _, row in spreadsheet_1.iterrows():
        cursor.execute('''
        INSERT INTO table_1 (shipment_identifier,
        product,
        on_time)
        VALUES (?, ?, ?)
        ''', (row['shipment_identifier'], row['product'], row['on_time']))

# Insert data from spreadsheets 2
def insert_spreadsheet_2():
    for _, row in spreadsheet_2.iterrows():
        cursor.execute('''
        INSERT INTO table_2 (shipment_identifier,
        origin_warehouse,
        destination_store,
        driver_identifier)
        VALUES (?, ?, ?, ?)
        ''', (row['shipment_identifier'], row['origin_warehouse'], row['destination_store'], row['driver_identifier']))

# Combine data from tables 1 and 2 and insert into combined_table
def combine_and_insert_data():
    query = '''
    INSERT INTO combined_table (shipment_identifier,
        product,
        on_time,
        origin_warehouse,
        destination_store,
        driver_identifier)
    FROM table_1
    INNER JOIN table_2 
    ON table_1.shipment_identifier = table_2.shipment_identifier
    '''
    cursor.execute(query)

# Create tables
create_tables()

# Insert data
insert_spreadsheet_0()
insert_spreadsheet_1()
insert_spreadsheet_2()

# Combine data and insert into combined_table
combine_and_insert_data()

# Commit and close the connection
conn.commit()
conn.close()

print("Database filled.")
