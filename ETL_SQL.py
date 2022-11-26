import sqlite3

try:
    sqliteConnection = sqlite3.connect('S30 ETL Assignment.db')
    sqlite_select_query = '''SELECT c.customer_id as Customer,
    c.age, i.item_name as Item,sum(o.quantity) as Quantity from customers c
    left join sales s
    on c.customer_id=s.customer_id
    left join orders o
    on o.sales_id=s.sales_id
    left join items i
    on i.item_id=o.item_id
    where quantity is not null
    and age between 18 and 35
    group by c.customer_id, c.age, i.item_name
    order by age desc;'''

    cursor = sqliteConnection.cursor()
    print("Successfully Connected to SQLite")
    cursor.execute(sqlite_select_query)
    print("List of tables\n")
    print(cursor.fetchall())
    sqliteConnection.commit()
    print("SQLite table created")

    cursor.close()

except sqlite3.Error as error:
    print("Error while creating a sqlite table", error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print("sqlite connection is closed")