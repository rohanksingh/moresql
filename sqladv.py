import mysql.connector
from mysql.connector import Error
import pandas as pd 

def create_server_connection(host_name, user_name , user_password):
    connection =None
    try: 
        connection= mysql.connector.connect(
            host= host_name,
            user= user_name,
            passwd=user_password
        )
        print("MYSQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

pw = "sUmitra#12" # IMPORTANT! Put your MySQL Terminal password here.
db = "sales" # This is the name of the database we will create in the next step - call it whatever you like.

connection = create_server_connection("localhost", "root", pw) 

# Creating a new database 

# def create_database(connection, query):
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         print("Database created successfully")
#     except Error as err:
#         print(f"Error: '{err}'")

# create_database_query = "CREATE DATABASE sales"
# create_database(connection, create_database_query)

# Modifying server connection 

def create_db_connection(host_name, user_name, user_password, db_name):
    connection= None
    try: 
        connection= mysql.connector.connect(
            host= host_name,
            user= user_name,
            passwd= user_password,
            database= db_name
        )
        print("MYSQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
        
    return connection 


# connection = create_db_connection("localhost", "root", "sUmitra#12", "sales")


# Define query execution function 

def execute_query(connection, query):
    cursor= connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
        
# Create sales data 

# create_table_sales = """
# create table sales(
#     sale_id int primary key,
#     sale_date date,
#     customer_id int ,
#     product_id int,
#     quantity int,
#     price decimal(10,2)
# );
# """

# # db_name="sales"
# connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
# execute_query(connection, create_table_sales) # Execute our defined query


# sales_input= """

# insert into sales 
#  values
# (1, '2024-01-01', 101, 201, 2,19.99),
# (2, '2024-01-03', 102, 202, 1,29.99),
# (3, '2024-01-05', 101, 203, 3, 9.99),
# (4, '2024-01-07', 103, 202, 5,4.99),
# (5, '2024-01-10', 104, 202, 2,29.99),
# (6, '2024-01-12', 105, 205, 1,49.99),
# (7, '2024-01-15', 101, 201, 4,19.99),
# (8, '2024-01-17', 106, 206, 3,14.99),
# (9, '2024-01-20', 102, 207, 2,24.99),
# (10, '2024-01-22', 107, 208, 1,39.99);
# """
# connection = create_db_connection("localhost", "root", pw, db)
# execute_query(connection, sales_input)


# Creating a new database Products

# def create_database(connection, query):
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         print("Database created successfully")
#     except Error as err:
#         print(f"Error: '{err}'")

# create_database_query = "CREATE DATABASE products"
# create_database(connection, create_database_query)

# Create products data 

create_table_products = """
create table products(
    product_id int primary key,
    product_name varchar(255) not null,
    category varchar(100) ,
    price decimal(10,2),
    stock_quantity int
   
);
"""

db="sales"
connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_products) # Execute our defined query

# Insert product data into the 'products' table

products = """
INSERT INTO products (product_id, product_name, category, price, stock_quantity)
VALUES
(201, 'Wireless Headphones', 'Electronics', 19.99, 150),
(202, 'Bluetooth Speaker', 'Electronics', 29.99, 200),
(203, 'Phone Charger', 'Accessories', 9.99, 300),
(205, 'Smartwatch', 'Electronics', 49.99, 100),
(206, 'Laptop Stand', 'Office Supplies', 14.99, 250),
(207, 'Gaming Mouse', 'Gaming', 24.99, 175),
(208, '4K Monitor', 'Electronics', 39.99, 80)
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, products)

# Create products data 

create_table_employees = """
create table employees(
    employee_id int primary key auto_increment,
    first_name varchar(100) not null,
    last_name varchar(100) not null,
    role_id int,
    email varchar(255) unique not null,
    phone_number varchar(20),
    salary decimal(10,2),
    hire_date date not null,
    created_at timestamp default current_timestamp,
    department varchar(100)
    
);
"""

db="sales"
connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_employees) # Execute our defined query

employees = """
INSERT INTO employees (first_name, last_name, role_id, email, phone_number, salary, hire_date, department)
VALUES 
('John', 'Doe', 1, 'john.doe@example.com', '1234567890', 50000, '2023-01-01', 'Sales'),
('Jane', 'Smith', 2, 'jane.smith@example.com', '0987654321', 60000, '2022-05-10', 'Management'),
('David', 'Brown', 1, 'david.brown@example.com', '2345678901', 55000, '2023-02-15', 'Sales'),
('Emily', 'Clark', 3, 'emily.clark@example.com', '3456789012', 48000, '2023-03-01', 'Accounting'),
('Michael', 'Johnson', 2, 'michael.johnson@example.com', '4567890123', 62000, '2023-04-12', 'Management'),
('Sarah', 'Davis', 1, 'sarah.davis@example.com', '5678901234', 47000, '2023-05-20', 'Sales'),
('Robert', 'Lee', 3, 'robert.lee@example.com', '6789012345', 51000, '2023-06-10', 'Accounting'),
('Linda', 'Garcia', 1, 'linda.garcia@example.com', '7890123456', 45000, '2023-07-30', 'Sales'),
('James', 'Martinez', 2, 'james.martinez@example.com', '8901234567', 63000, '2023-08-05', 'Management'),
('Laura', 'Rodriguez', 1, 'laura.rodriguez@example.com', '9012345678', 54000, '2023-09-14', 'Sales');


"""
connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, employees)

# Defining data reading function 

def read_query(connection, query):
    cursor= connection.cursor()
    result= None
    try:
        cursor.execute(query)
        result= cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

# Reading data from database 

q1= """
select * from sales;
"""
connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

for result in results:
    print(result)
    
    
# Common Table Expressions (CTEs)
# CTEs are temporary named result sets that exist within the scope of a single SQL statement. 
# They simplify complex queries by breaking them into smaller, more manageable parts.
   
q2= """

with sales_summary as (
    select 
    product_id, sum(quantity) as total_quantity,
    sum(price*quantity) as total_revenue
    from sales
    group by product_id
)  

select p.product_name, s.total_quantity, s.total_revenue
from products p 
join sales_summary s on p.product_id= s.product_id
order by s.total_revenue desc
limit 10;

"""
# connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q2)

for result in results:
    print(result)
    
q3= """

select * from sales where sale_date between  '2024-01-01' and '2024-01-10';

"""

results= read_query(connection, q3)

for result in results:
    print(result)
    
# Formatting poutput into list:

# from_db= []

# for results in results:
#     result= list(result)
#     from_db.append(result)
    
# print(from_db)

# Formatting output into pandas dataframe 

from_db= []

for result in results:
    result= list(result)
    from_db.append(result)
    
columns= ["sale_id", "sale_date", "customer_id", "product_id", "quantity", "price"]
df= pd.DataFrame(from_db, columns=columns)

df.head()

  
# Subqueries

# Subqueries are nested queries within a larger SQL statement. 
# They can be used in various parts of a query, such as SELECT, FROM, WHERE, and HAVING clauses.

q4= """

select first_name, last_name, salary from employees where salary> (select avg(salary)
from employees where department= 'Sales')
order by salary desc;
"""
# connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q4)

for result in results:
    print(result)
    
# Self Joins

# Self joins are used when a table needs to be joined with itself, 
# typically to compare rows within the same table or to establish hierarchical relationships.

q5= """
 select e.first_name as employee, 
 m.first_name as manager 
 from employees e
 left join employees m on e.role_id= m.role_id
 order by e.first_name;

"""
results = read_query(connection, q5)

for result in results:
    print(result)

# Window Functions

# Window functions perform calculations across a set of table rows that are 
# related to the current row, allowing for complex analytical queries.

q6= """
select first_name, last_name, department, salary, 
avg(salary) over (partition by department) as dept_avg_salary,
salary - avg(salary) over (partition by department) as salary_diff_from_avg

from employees
order by department, salary desc;
"""

results= read_query(connection, q6)
for result in results:
    print(result)
    
# Unions
# UNION combines the result sets of two or more SELECT statements,
# removing duplicate rows by default. UNION ALL retains all rows, including duplicates.

q7= """
select product_name, 'In Stock' as status from 
products 
where stock_quantity > 80

union 

select product_name, 'Out of Stock' as status
from products
where stock_quantity = 0

order by product_name;
"""
results= read_query(connection, q7)
for result in results:
    print(result)
    
# Date Manipulation

# SQL provides various functions to work with dates, 
# allowing for complex date-based calculations and filtering.

# Create order data 

create_table_orders = """
create table orders(
    order_id int primary key,
    order_date Date,
    delivery_date Date   
);
"""

db="sales"
connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_orders) # Execute our defined query

# Insert orders data into the 'orders' table

orders = """
INSERT INTO orders (order_id, order_date, delivery_date)
VALUES
(1, '2024-01-05', '2024-01-12'),
(2, '2024-01-10', '2024-01-16'),
(3, '2024-01-15', '2024-01-22'),
(4, '2024-01-20', '2024-01-25'),
(5, '2024-01-25', '2024-01-30'),
(6, '2024-02-01', '2024-02-08'),
(7, '2024-02-10', '2024-02-14'),
(8, '2024-02-15', '2024-02-22'),
(9, '2024-02-20', '2024-02-25'),
(10, '2024-03-01', '2024-03-07');
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, orders)

q8= """
 select order_id, order_date, delivery_date,
 datediff(delivery_date, order_date) as days_to_delivery,
 date_add(order_date, interval 7 day) as expected_delivery,
 case
    when delivery_date <= date_add(order_date, interval 7 day) then 'On Time'
    else 'Delayed'
    end as delivery_status
 from orders
 where year(order_date)= year(curdate())
 order by order_date;
"""
results = read_query(connection, q8)

for result in results:
    print(result)
    
# Pivoting Techniques

# Pivoting transforms rows into columns, 
# useful for creating summary reports or transforming data for analysis.

q9= """
 select product_id, 
 sum(case when day(sale_date)=1 then price*quantity  else 0 end) as day1,
 sum(case when day(sale_date)=2 then price*quantity else 0 end) as day2,
 sum(case when day(sale_date)=3 then price*quantity else 0 end) as day3
 
 from sales
 where year(sale_date) = year(curdate())
 group by product_id;
 order_by product_id;
"""

results= read_query(connection, q9)

for result in results:
    print(result)
    
# Unpivoting Techniques

# Unpivoting converts columns into rows, 
# useful for normalizing data or preparing it for analysis.

# Data Modeling and Table Relationships

# Data modeling involves designing the structure of a database, 
# including tables and their relationships. Common relationship 
# types include one-to-one, one-to-many, and many-to-many.

# -- one to many realtionship example 
create_table_departments="""
create table departments (
    department_id int primary key,
    department_name varchar(100) not null
);
"""

create_table_employeesnew="""
create table employeesnew (
employee_id int primary key ,
employee_name varchar(100) Not Null,
department_id int,
foreign key (department_id) references departments(department_id));
"""

db="sales"
connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_departments) # Execute our defined query


db="sales"
connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_employeesnew) # Execute our defined query


# -- Many to Many relationship example 
create_table_students="""

create table students (
    
    student_id int primary key,
    student_name varchar(100) Not Null    
);
"""

create_table_courses="""
create table courses (
    course_id int primary key,
    course_name varchar(100) not null
);

"""

create_table_enrollments="""
create table enrollments (
    
    student_id int, 
    course_id int,
    enrollment_date Date ,
    primary key (student_id, course_id),
    foreign key (student_id) references students(student_id),
    foreign key (course_id) references courses (course_id)
);

"""
db="sales"
connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_students) # Execute our defined query

# db="sales"
# connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_courses) # Execute our defined query

# db="sales"
# connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_table_enrollments) # Execute our defined query
