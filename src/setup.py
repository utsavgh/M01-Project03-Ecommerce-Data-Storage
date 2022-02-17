import csv
import database as db

PW = "root" # IMPORTANT! Put your MySQL Terminal password here.
ROOT = "root"
DB = "ecommerce_record" # This is the name of the database we will create in the next step - call it whatever you like.
LOCALHOST = "localhost"
connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB 
db.create_switch_database(connection, DB, DB)


RELATIVE_CONFIG_PATH = '../config/'
USER = 'users'
ITEM = 'items'
ORDER = 'orders'

# Create the tables through python code here
# if you have created the table in UI, then no need to define the table structure

users_table="""
create table users(
user_id varchar(40) primary key,
user_name varchar(40) not null,
user_email varchar(40) not null,
user_password varchar(40) not null,
user_address varchar(40) not null,
is_vendor tinyint(1) default 0
)
"""

item_table="""
create table items(
product_id varchar(5) primary key,
product_name varchar(40) not null,
product_price float(50) not null,
product_description varchar(200) not null,
vendor_id varchar(40) not null,
emi_available varchar(40) not null,
FOREIGN KEY (vendor_id) REFERENCES users(user_id)
)
"""

order_table="""
create table orders(
order_id int primary key,
customer_id varchar(40) not null,
vendor_id varchar(40) not null,
total_value float(50) not null,
order_quantity int not null,
reward_point int not null,
FOREIGN KEY (vendor_id) REFERENCES users(user_id),
FOREIGN KEY (customer_id) REFERENCES users(user_id)
)
"""

# --------------------------------------------------------#
# Customer_leaderboard table DDL is present in main.py    #
# --------------------------------------------------------#

db.create_insert_query(connection, users_table)
db.create_insert_query(connection, item_table)
db.create_insert_query(connection, order_table)



# If you are using python to create the tables, call the relevant query to complete the creation
print("Inserting to users table")
with open(RELATIVE_CONFIG_PATH+USER+'.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    sql='''insert into users (user_id, user_name, user_email, user_password, user_address, is_vendor) values (%s, %s, %s, %s, %s, %s)'''
    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    db.insert_many_data(connection, sql, val)
print("\nInserted to users table")



print("Inserting to items table")
with open(RELATIVE_CONFIG_PATH+ITEM+'.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    sql='''insert into items (product_id, product_name, product_price, product_description, vendor_id, emi_available) values (%s, %s, %s, %s, %s, %s)'''
    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    db.insert_many_data(connection, sql, val)
print("\nInserted to items table")


print("Inserting to orders table")
with open(RELATIVE_CONFIG_PATH+ORDER+'.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    sql='''insert into orders (order_id, customer_id, vendor_id, total_value, order_quantity, reward_point) values (%s, %s, %s, %s, %s, %s)'''
    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    db.insert_many_data(connection, sql, val)
print("\nInserted to orders table")