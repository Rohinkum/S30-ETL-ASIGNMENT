import sqlite3
import pandas as pd
conn = sqlite3.connect('S30 ETL Assignment.db')
print ("Opened database successfully");

customer = pd.read_sql('select * from customers;',conn)
sales = pd.read_sql('select * from sales;',conn)
orders = pd.read_sql('select * from orders;',conn)
items = pd.read_sql('select * from items;',conn)

Df = customer.merge(sales, on = 'customer_id', how='left')
Df1 = Df.merge(orders, on = 'sales_id', how='left')
Df2 = Df1.merge(items , on=  'item_id', how = 'left')
Df3 = Df2[Df2['quantity'].notnull()].[(Df2['age'] <= 18) & (Df2['age'] >=35)].groupby(['customer_id','age','item_name']).agg({'quantity': ['sum']}).sort_values('age', ascending = False)
Df3.head()