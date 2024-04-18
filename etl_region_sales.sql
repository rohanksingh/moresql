use sales_db;


--  CI/CD pipeline example for MySQL using the earlier orders, customer, and region_sales_summary tables.



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

