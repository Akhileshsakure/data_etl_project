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
    print("\nInserted into database successfully:\n")
    for i in cursor.fetchall(): 
        print(i) 
    
    conn1.commit() 
    conn1.close() 
