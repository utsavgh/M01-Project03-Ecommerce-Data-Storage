from calendar import c
import database as db
import setup  # Calling setup file in main file
# Driver code
if __name__ == "__main__":

    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = "root" # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerce_record" # This is the name of the database we will create in the next step - call it whatever you like.
    LOCALHOST = "localhost"
    # LOCALHOST = "127.0.0.1" # Local loopback
    # connection = db.create_server_connection(LOCALHOST, ROOT, PW)
    
    # creating the schema in the DB 
    connection = db.create_db_connection(LOCALHOST, ROOT, PW, DB)

    query_2_b="""
    insert into orders values 
    (101, '8', '1', 40848, 1, 200),
    (102, '13', '2', 88289, 2, 100),
    (103, '11', '3', 33659, 3, 300),
    (104, '7', '4', 42345, 4, 100),
    (105, '9', '5', 8971, 5, 200)
    """

    # ------------------------------------------------------------- #
    # Max, min, avg values may differ depending on the above inputs #
    # ------------------------------------------------------------- #


    query_2_c="""
    select * from orders;
    """

    query_3_a_max="""
    select * from orders where total_value = (select max(total_value) from orders);
    """

    # Alternate way to get the same
    query_3_a_max_alternate="""
    select * from orders order by total_value desc limit 1;
    """

    query_3_a_min="""
    select * from orders where total_value = (select min(total_value) from orders);
    """

    # Alternate way to get the same
    query_3_a_min_alternate="""
    select * from orders order by total_value limit 1;
    """

    query_3_b="""
    select * from orders where total_value > (select avg(total_value) from orders);
    """

    query_3_c="""
    create table cutomer_leaderboard(
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
    
    customer_leaderboard_table="""
    create table customer_leaderboard(
    customer_id varchar(40) primary key,
    total_value float(50) not null,
    customer_name varchar(40) not null,
    customer_email varchar(40) not null,
    FOREIGN KEY (customer_id) REFERENCES users(user_id)
    )
    """

    customer_leaderboard_table_insert="""
    insert into customer_leaderboard (customer_id, total_value, customer_name, customer_email)
    SELECT u.user_id, max(o.total_value), u.user_name, u.user_email
    from orders o, users u
    where o.customer_id = u.user_id and o.order_quantity >0
    GROUP BY user_id;
    """

    customer_leaderboard_table_select="""
    select * from customer_leaderboard;
    """
    
    print("\n----------------E-commerce data storage Solution ------------------")
    print("\n----------------Solution - Problem 2.b ----------------------------")
    print("\nInserting the additional data points in the orders table: ")
    db.create_insert_query(connection, query_2_b)
    print('\n------------------ Solution - Problem 2.b is completed. -----------')



    print('\n-------------------- Solution - Problem 2.c -----------------------')
    print('\nHere is the details of all orders inserted in the table: ')
    order_details=db.select_query(connection, query_2_c)
    for i in order_details:
        print(i)
    print('\n------------------ Solution - Problem 2.c is completed. -----------')



    print('\n-------------------- Solution - Problem 3.a -----------------------')
    print('Order with minimum value is: ')
    min_order=db.select_query(connection, query_3_a_min)
    print(min_order)
    print('Order with maximum value is: ')
    max_order=db.select_query(connection, query_3_a_max)
    print(max_order)
    print('\n------------------ Solution - Problem 3.a is completed. -----------')


    print('\n-------------------- Solution - Problem 3.b -----------------------')
    print('\nAll the order details with value greater than average order value: ')
    greater_than_avg=db.select_query(connection, query_3_b)
    for i in greater_than_avg:
        print(i)
    print('\n------------------ Solution - Problem 3.b is completed. -----------')
    print('\n-------------------- Solution - Problem 3.c -----------------------')
    db.create_insert_query(connection, customer_leaderboard_table)
    print('\nData fetch query is being created: ')
    print('\nInitiating the data insertion in customer_leaderboard table: ')
    db.create_insert_query(connection, customer_leaderboard_table_insert)
    print('\nData is inserted in the table.')
    print('\nResult of highest ordered purchase from customer_leaderboard table')
    select_customer_leaderboard=db.select_query(connection, customer_leaderboard_table_select)
    for i in select_customer_leaderboard:
        print(i)
    print('\n------------------ Solution - Problem 3.c is completed. -----------')


    # Start implementing your task as mentioned in the problem statement 
    # Implement all the test cases and test them by running this file


