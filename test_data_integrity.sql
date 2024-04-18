use sales_db;


--  CI/CD pipeline example for MySQL using the earlier orders, customer, and region_sales_summary tables.


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
















                            
                            
                            