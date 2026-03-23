CREATE DATABASE superstore_analysis;
Use superstore_analysis;

CREATE TABLE superstore (
	row_id Int, 
    order_id VARCHAR(50),
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code INT,
    region VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(200),
    sales FLOAT,
    quantity INT,
    discount FLOAT,
    profit FLOAT
);

select * from superstore;

select * from superstore
limit 10;

describe superstore;

select distinct region from superstore;
select distinct category from superstore;
select distinct segment from superstore;

SELECT 
    COUNT(*) - COUNT(order_id) AS missing_order_id,
    COUNT(*) - COUNT(sales) AS missing_sales,
    COUNT(*) - COUNT(profit) AS missing_profit
FROM superstore;

select order_id, COUNT(*)
from superstore
GROUP BY order_id
HAVING Count(*)>1;

SELECT 
    MIN(sales) AS min_sales,
    MAX(sales) AS max_sales,
    AVG(sales) AS avg_sales
FROM superstore;

SELECT *
FROM superstore
ORDER BY sales DESC
LIMIT 10;

SELECT 
    MIN(order_date) AS start_date,
    MAX(order_date) AS end_date
FROM superstore;

SELECT region, COUNT(*) AS total_orders
FROM superstore
GROUP BY region;

SELECT region, ROUND(SUM(sales),2) AS total_sales
FROM superstore
GROUP BY region;

SELECT SUM(sales) AS total_sales FROM superstore;

SELECT SUM(profit) AS total_profit FROM superstore;

SELECT COUNT(DISTINCT order_id) AS total_orders FROM superstore;

SELECT AVG(discount) AS avg_discount FROM superstore;

-- Sales By region --
SELECT region, SUM(sales) AS total_sales
FROM superstore
GROUP BY region
ORDER BY total_sales DESC;

-- Profit by Category --
SELECT category, SUM(profit) AS total_profit
FROM superstore
GROUP BY category;

-- Sub-category Loss Analysis --
SELECT sub_category, SUM(profit) AS total_profit
FROM superstore
GROUP BY sub_category
HAVING SUM(profit) < 0
ORDER BY total_profit;

-- Monthly Sales Trend --
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY year, month
ORDER BY year, month;

-- Top 5 Customers --
SELECT customer_name, SUM(sales) AS total_sales
FROM superstore
GROUP BY customer_name
ORDER BY total_sales DESC
LIMIT 5;

-- Profit Margin --
SELECT 
    SUM(profit)/SUM(sales) AS profit_margin
FROM superstore;

-- Discount Impact --
SELECT 
    discount,
    AVG(profit) AS avg_profit
FROM superstore
GROUP BY discount
ORDER BY discount;

-- Top State by Profit --
SELECT state, SUM(profit) AS total_profit
FROM superstore
GROUP BY state
ORDER BY total_profit DESC
LIMIT 5;

-- Lowest Profit By Region --
SELECT region, MIN(profit) AS lowest_profit
FROM superstore
GROUP BY region
ORDER BY lowest_profit DESC;

-- Category Wise Highest Sales,Lowest Profit --
SELECT category, MAX(sales) AS highest_sales, MIN(profit) AS low_profit
FROM superstore 
GROUP BY category
ORDER BY category DESC;

-- Top 3 loss-making sub-categories --
SELECT sub_category, SUM(profit) AS total_profit
FROM superstore
GROUP BY sub_category
HAVING SUM(profit) < 0
ORDER BY total_profit
LIMIT 3;

-- Top Month having highest total sales --
SELECT 
    YEAR(order_date) AS year,
    MONTHNAME(order_date) AS month,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 1;

-- Rank Customer BY Sales --
with rnk_cust as(
	select customer_name, sum(sales) as total_sales,
    dense_rank() over(order by sum(sales) desc) as rnk
    from superstore
    group by customer_name
)
select * from rnk_cust
where rnk <= 5;

-- Sales ranking customers within each region --
select region ,customer_name, sum(sales) as total_sales,
	dense_rank() over (partition by region order by sum(sales) desc) as rnk
    from superstore
    group by region, customer_name;
    
-- Top 3 Sales ranking customers within each region --
with rnk_cust as(
    select region ,customer_name, sum(sales) as total_sales,
	dense_rank() over (partition by region order by sum(sales) desc) as rnk
    from superstore
    group by region, customer_name
)
select * from rnk_cust
where rnk <= 3;

-- running total --
SELECT 
    order_date,
    SUM(sales) AS daily_sales,
    SUM(SUM(sales)) OVER (ORDER BY order_date) AS running_total
FROM superstore
GROUP BY order_date
ORDER BY order_date;

-- previous month sales --
WITH monthly_sales_cte AS (
    SELECT 
        YEAR(order_date) AS year,
        MONTH(order_date) AS month,
        SUM(sales) AS monthly_sales
    FROM superstore
    GROUP BY year, month
)

SELECT 
    year,
    month,
    monthly_sales,
    LAG(monthly_sales) OVER (ORDER BY year, month) AS previous_month_sales,
    
    ROUND(
        (monthly_sales - LAG(monthly_sales) OVER (ORDER BY year, month)) 
        / LAG(monthly_sales) OVER (ORDER BY year, month) * 100, 
    2) AS growth_percent

FROM monthly_sales_cte;

-- Current vs previous order --
SELECT 
    order_id,
    order_date,
    sales,
    LAG(sales) OVER (ORDER BY order_date) AS previous_order_sales
FROM superstore;

-- Customers above average sales --
with cust_sales as(
	select customer_name,sum(sales) as total_sales
    from superstore
    group by customer_name
)
select * from cust_sales
where total_sales > ( select avg(total_sales) from cust_sales);

-- Top 20% customers --
with cte as(
	select customer_name,sum(sales) as total_sales,
    ntile(5) over (order by sum(sales) desc) as bucket
    from superstore
    group by customer_name
)
select * from cte
where bucket = 1;

-- Consecutive order dates --
select distinct customer_name
from(
	select customer_name,order_date,
    lag(order_date) over (partition by customer_name order by order_date) as prev_date
    from superstore
)t
where datediff(order_date,prev_date)=1;

-- Customers who haven’t ordered in last 6 months --
select customer_name
from superstore
group by customer_name
having max(order_date) < current_date - interval 6 month;

-- Product contributing 80% revenue --
with cte as(
	select product_name, sum(sales) as total_sales
    from superstore
    group by product_name
),
running as(
	select product_name,total_sales,
    sum(total_sales) over (order by total_sales desc) as running_sales,
    sum(total_sales) over () as total_sales_all
    from cte
)
select * from running 
where running_sales <= 0.8 * total_sales_all;

-- Segment customers: High / Medium / Low spenders --
with cust_seg as (
	select customer_name,sum(sales) as total_sales
    from superstore
    group by customer_name
)
select * ,
case
	when total_sales > 10000 then 'High Spender'
    when total_sales between 5000 and 10000 then 'Medium Spender'
    else 'Low Spender'
end as segment
from cust_seg;

-- Most profitable order per category --
select*
from(
	select *,
    row_number() over (partition by category order by profit desc) as rnk
    from superstore
)t
where rnk = 1; 

-- Days between consecutive orders per customer --
SELECT 
    customer_name,
    order_date,
    DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_name ORDER BY order_date)) AS gap_days
FROM superstore;
    
-- Delete duplicate orders --
SELECT *
FROM (
    SELECT *,
           COUNT(*) OVER (PARTITION BY order_id) AS cnt
    FROM superstore
) t
WHERE cnt > 1;


-- Revenue Growth Trend --
WITH monthly AS (
    SELECT 
        YEAR(order_date) AS year,
        MONTH(order_date) AS month,
        SUM(sales) AS sales
    FROM superstore
    GROUP BY year, month
)
SELECT *,
       LAG(sales) OVER (ORDER BY year, month) AS prev_sales,
       ROUND((sales - LAG(sales) OVER (ORDER BY year, month)) 
       / LAG(sales) OVER (ORDER BY year, month) * 100, 2) AS growth
FROM monthly;