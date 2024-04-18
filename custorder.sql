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


-- PL/SQL Script for ETL
-- The script below extracts data from the source tables, 
-- transforms it by filtering for orders in January 2024, and loads the summarized data into the region_sales_summary table.


-- ETL Procedure 


BEGIN;

-- Step 1: Clear the target table 

DELETE from region_sales_summary;

-- Perform the ETL process 
insert into region_sales_summary (region, total_sales)
select 
c.region,
sum(o.amount) as total_sales
from orders o 
join customer c on o.customer_id = c.customer_id
where 
o.order_date between ('2023-01-01')
				 and ('2023-12-31')
                 
group by 
c.region;

-- Commit the transaction
commit;

-- -------------------------

Drop procedure if exists etl_region_sales;

-- Create a Stored Procedure

delimiter $$

create procedure etl_region_sales()

begin

declare exit handler for sqlexception
begin

rollback;
select 'Error occured during etl process.';
end;

start transaction;

delete from region_sales_summary;

insert into region_sales_summary(region, total_sales)
select 
c.region,
sum(o.amount) as total_sales
from 
orders o 
join 
customer c on o.customer_id = c.customer_id
where 
o.order_date between '2023-01-01' and '2023-12-31'
group by 
c.region;

-- commit the transaction if successful 
commit;

-- output success message 
select 'ETL pROCESS COMPLETE SUCCESSFULLY.';

END$$

delimiter ;


begin
dbms_scheduler.create_job(
job_name => 'ETL_region_sales_job',
job_type => 'PLSQL_block',
job_action => 'Begin
				delete from region_sales_summary;
                insert into region_sales_summary (region, total_sales)
                select c.region, sum(o.amount) 
                from orders o  
                join customer c on o.customers_id =c.customer_id
                where o.order_date between sysdate -1 and sysdate group by c.region;
                commit;
			end;',
start_date  => systimestamp, 
repeat_interval  => 'Freq=daily; byhour=0; byminute=0; bysecond=0',
enabled          => true
);
end $$

-- Step 2 Test Script 
-- Test data Integrity

select 
case 
when count(*) >0 then 'Test Passed : Data exists in region_sales_summary.'
else 'Test Failed: No data found in region_sales_summary.'
end as test_resulr
from region_sales_summary;

-- verify total sales calculation for a specific region (example for 'North')

select 
case 
when (select total_sales from region_sales_summary where region= 'North') = (
select sum(o.amount)
from orders o  join customer c on o.customer_id = c.customer_id
where c.region = 'East' and o.order_date between '2023-01-01' and '2023-12-31') then 'Test Passed: Totsl sales match for region North.'
else 'Test Failed: Total sales do not match for region North.'
end as test_result;







show databases;



GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' IDENTIFIED BY 'sUmitra#12';
FLUSH PRIVILEGES;

FLUSH PRIVILEGES;

SHOW GRANTS FOR 'root'@'127.0.0.1';




                            
                            
                            