import mysql.connector
import random
import time
import datetime

# Global methods to push interact with the Database

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
	# Implement the logic to create the server connection
    connection=None
    try:
        connection=mysql.connector.connect(host=host_name, user=user_name, password=user_password)
        print("MySQL Database connection successful")
    except Exception as err:
        print("Unable to establish connection: ", err)

    return connection


# This method will create the database
def create_switch_database(connection, db_name, switch_db):
    # For database creatio nuse this method
    # If you have created your databse using UI, no need to implement anything
    mycursor = connection.cursor()
    # current_db="SELECT DATABASE()"
    try:
        sql_delete_db=f'DROP DATABASE IF EXISTS {db_name}'    # For re-running the program 
        sql_create_db=f'CREATE DATABASE {db_name}'
        sql_use_db=f'USE {switch_db}'
        mycursor.execute(sql_delete_db)
        mycursor.execute(sql_create_db)
        mycursor.execute(sql_use_db)
        # mycursor.execute(current_db)
        # res=mycursor.fetchall()
        # print("\nCurrent Database selected: ", res)
        print(f'Created DB {db_name} successfully')
    except Exception as err:
        print("Unable to create DB: ", err)


# This method will establish the connection with the newly created DB 
def create_db_connection(host_name, user_name, user_password, db_name):
    connection=None
    try:
        connection=mysql.connector.connect(host=host_name, user=user_name, password=user_password, database=db_name)
        print("MySQL Database connection successful")
    except Exception as err:
        print("Unable to establish connection: ", err)

    return connection

# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, query):
	# This method will perform creation of the table 
	# this can also be used to perform single data point insertion in the desired table
    # print("Create Statement: ")
    # print("\n")
    # print(query)
    mycursor = connection.cursor()
    try:
        mycursor.execute(query)
        connection.commit()
        print("Query successful")
    except Exception as err:
        print("Unable to create tables: ", err)
    
# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table 
    res=None
    mycursor = connection.cursor()
    try:
        mycursor.execute(query)
        res=mycursor.fetchall()
        return res
    except Exception as err:
        print("Error querying table: ", err)

# performing the execute many query over the table, 
# this method will help us to inert multiple records using a single instance
def insert_many_data(connection, sql, val):
    # to perform multiple insert operation in teh database
    mycursor = connection.cursor()
    try:
        mycursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Exception as err:
        print("Unable to insert many: ", err)