use sales_db;


--  CI/CD pipeline example for MySQL using the earlier orders, customer, and region_sales_summary tables.

-- STEP 1 : sql Scripts

create table orders (
order_id int primary key,
customer_id int,
order_date Date,
amount decimal(10,2)
);

create table customer (
customer_id int primary key ,
customer_name varchar(100),
region varchar(50)
);

insert into orders values (1, 101, '2023-12-01', 150.00),
						(2, 102, '2023-12-02' , 200.00),
                        (3, 103, '2023-12-05' , 300.00),
						(4, 104, '2023-12-07' , 210.00),
                        (5, 105, '2023-12-08' , 310.00),
						(6, 106, '2023-12-09' , 400.00),
						(7, 107, '2023-12-11' , 410.00),
                        (8, 108, '2023-12-13' , 510.00);
                        
                        
insert into customer values (101, 'Alice', 'North'),
							(102, 'Bob' , 'South'),
                            (103, 'Rohan', 'East'),
							(104, 'Singh' , 'West'),
							(105, 'Sob' , 'South'),
                            (106, 'ber', 'East'),
							(107, 'roz' , 'West'),
							(108, 'ros' , 'North');
                            
create table region_sales (
region varchar(50),
total_sales decimal (10, 2)
);

insert into region_sales (region, total_sales)
select 
c.region,
sum(o.amount) as total_sales
from 
orders o 
join customer c on o.customer_id = c.customer_id
where 
c.region in ('North' , 'South')
group by 
c.region;


-- Create target table 

create table region_sales_summary (
region varchar(50),
total_sales decimal (10, 2)
);


