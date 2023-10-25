import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def to_database(data):
    conn_string = 'postgresql://postgres:0000@127.0.0.1/taskmanager'
    db = create_engine(conn_string)
    conn = db.connect()
    conn1 = psycopg2.connect(
        database = "taskmanager",
        user = "postgres",
        password = "0000",
        host = "127.0.0.1",
        port = "5432"
    )
    conn1.autocommit = True
    cursor = conn1.cursor() 
    
    # drop table if it already exists 
    cursor.execute('drop table if exists metamtica_order')

    sql = '''CREATE TABLE IF NOT EXISTS metamtica_order (OrderID varchar(40),ProductID varchar(40),ProductName varchar(40),
    Quantity integer,UnitPrice decimal,OrderDate date,TotalCost decimal,Month smallint);'''

    cursor.execute(sql)

    data.to_sql('metamtica_order', conn, if_exists = 'replace')

    # fetching all rows 
    sql1='''select * from metamtica_order;'''
    cursor.execute(sql1) 
    for i in cursor.fetchall(): 
        print(i) 
    
    conn1.commit() 
    conn1.close() 





mathematica_df = pd.read_csv("C:/Users/akhil/study/python_workspace/data_etl_project/mathematica.csv", index_col="OrderID")
mathematica_df['OrderDate'] = pd.to_datetime(mathematica_df['OrderDate'], errors='coerce')

#Remove duplicates
mathematica_df.drop_duplicates(inplace=True)

#Create TotalCost
mathematica_df['TotalCost'] = mathematica_df['Quantity'] * mathematica_df['UnitPrice']

#Create Month column
mathematica_df['Month'] = mathematica_df['OrderDate'].dt.month
print(mathematica_df)

#Product groupby Quantity
product_sum_df = mathematica_df.groupby(by="ProductID")['Quantity'].sum()
# print(type(product_sum_df))

#UnitPrice average
product_mean_df = mathematica_df.groupby(by="ProductID")['UnitPrice'].mean()

# mathematica_df.to_csv("transformed_data.csv")
# product_sum_df.to_csv("Product_sum.csv")
# product_mean_df.to_csv("Product_mean.csv")
to_database(mathematica_df)