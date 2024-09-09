/* Funnction to get statictics of column of table*/


CREATE OR REPLACE FUNCTION get_statistics(table_name TEXT, column_name TEXT)
RETURNS TABLE (
    min_value NUMERIC,
    percentile_25 NUMERIC,
    median_value NUMERIC,
    percentile_75 NUMERIC,
    max_value NUMERIC
) AS $$
DECLARE
    sql TEXT;
BEGIN
    -- Construct the dynamic SQL query
    sql := format('
        SELECT
            MIN(%s)::NUMERIC AS min_value,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY %s)::NUMERIC AS percentile_25,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY %s)::NUMERIC AS median_value,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY %s)::NUMERIC AS median_value,
            MAX(%s)::NUMERIC AS max_value
        FROM %I',
        quote_ident(column_name), quote_ident(column_name), quote_ident(column_name),quote_ident(column_name), quote_ident(column_name), table_name);

    -- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

/* Checking for duplicates */
select distinct * from orders

select * from orders



select distinct * from order_items

select * from order_items



select distinct * from sellers

select * from sellers



select * from products
select distinct * from products


/* All the tables we are using seem to have unique rows*/




 --Update empty strings to NULL
UPDATE orders
SET order_purchase_timestamp = NULLIF(order_purchase_timestamp, ''),
    order_delivered_carrier_date = NULLIF(order_delivered_carrier_date, ''),
    order_delivered_customer_date = NULLIF(order_delivered_customer_date, ''),
    order_approved_at = NULLIF(order_approved_at, ''),
    order_estimated_delivery_date = NULLIF(order_estimated_delivery_date, '');



-- Alter the table to modify the data type of the columns from TEXT to TIMESTAMP

ALTER TABLE orders
ALTER COLUMN order_purchase_timestamp TYPE TIMESTAMP 
USING TO_TIMESTAMP(NULLIF(order_purchase_timestamp, ''), 'YYYY-MM-DD HH24:MI:SS'),
ALTER COLUMN order_delivered_carrier_date TYPE TIMESTAMP 
USING TO_TIMESTAMP(NULLIF(order_delivered_carrier_date, ''), 'YYYY-MM-DD HH24:MI:SS'),
ALTER COLUMN order_delivered_customer_date TYPE TIMESTAMP 
USING TO_TIMESTAMP(NULLIF(order_delivered_customer_date, ''), 'YYYY-MM-DD HH24:MI:SS'),
ALTER COLUMN order_approved_at TYPE TIMESTAMP 
USING TO_TIMESTAMP(NULLIF(order_approved_at, ''), 'YYYY-MM-DD HH24:MI:SS'),
ALTER COLUMN order_estimated_delivery_date TYPE TIMESTAMP 
USING TO_TIMESTAMP(NULLIF(order_estimated_delivery_date, ''), 'YYYY-MM-DD HH24:MI:SS');



/*Extracting year month order_purchase_timestamp and storing it in temp_orders*/
/* I am making a assumption here that the date we extracted this data is equal to the max(order_purchase_timestamp) */

/*Since we have orders  from 2016 and 2017 still under  processing/invoiced,I am creating a new column where if difference between max(order_purchase_timestamp) and expected_delivery_date is more than 45 days and delivered field is null ,I categorize the order as Abandoned/delayed */


CREATE TEMPORARY TABLE temp_orders AS
SELECT 
    *,
    EXTRACT(YEAR FROM order_purchase_timestamp) AS order_year,
    EXTRACT(MONTH FROM order_purchase_timestamp) AS order_month,
    CASE 
        WHEN AGE(
            max_date, 
            order_estimated_delivery_date
        ) > INTERVAL '45 days' 
        AND order_delivered_customer_date IS NULL 
        AND order_status <> 'canceled' 
        AND order_status <> 'unavailable'
        THEN 'Abandoned/Delayed'
        
        WHEN order_delivered_customer_date > order_estimated_delivery_date 
        THEN 'Late Delivery' 
        
        ELSE order_status 
    END AS detailed_order_status
FROM 
    orders,
    (
        SELECT 
            MAX(order_purchase_timestamp) AS max_date
        FROM 
            orders
    ) AS current_assumed_date;
        








/*Orders grouped by Date*/
/*Number of orders by year*/

SELECT 
    order_year,
    COUNT(order_id) 
FROM 
    temp_orders
GROUP BY 
    order_year;

/*Number of orders by year,month*/

SELECT 
    order_year,
    order_month,
    COUNT(order_id)
FROM 
    temp_orders
GROUP BY 
    order_year,
    order_month;


/*Number of orders by year,order_status*/

SELECT 
    order_year,detailed_order_status,
    COUNT(order_id)
FROM 
    temp_orders
GROUP BY 
    order_year,detailed_order_status
ORDER BY order_year
/* End*/





/*Active Customer by date*/
/* Number of Active customers by year*/
SELECT 
    order_year,
    COUNT(DISTINCT customer_unique_id) as no_of_customers
FROM 
    temp_orders
INNER JOIN
    customers
USING (customer_id)
GROUP BY 
    order_year;



/* Number of Active customers by year and month*/
SELECT 
    order_year,
    order_month,
    COUNT(DISTINCT customer_unique_id) as no_of_customers
FROM 
    temp_orders
INNER JOIN
    customers
USING (customer_id)
GROUP BY 
    order_year,
    order_month;


/* Number of Active customers by year and detailed_order_status */
SELECT 
    order_year,detailed_order_status,
    COUNT(DISTINCT customer_unique_id) as no_of_customers
FROM 
    temp_orders
INNER JOIN
    customers
USING (customer_id)
GROUP BY 
    order_year,detailed_order_status;




/* END */




/*Denormalizing ordes_items with products and sellers*/

CREATE TEMPORARY TABLE temp_denormalized_order_items AS

WITH product_name_translate AS (
    SELECT 
        product_id,
        product_category_name_english
    FROM 
        products
    INNER JOIN 
        product_category_name_translation
    USING (product_category_name)
)

SELECT 
    *,
    ROUND((price + freight_value)::numeric, 2) AS total_price
FROM 
    order_items
LEFT JOIN 
    sellers USING (seller_id)
LEFT JOIN 
    product_name_translate USING (product_id);





/*Joining temp_denormalized_order_items with temp_orders,so that we can get  seller metrics  by year*/

CREATE TEMPORARY TABLE orders_orderItems AS

SELECT 
    *
FROM 
    temp_denormalized_order_items
INNER JOIN 
    temp_orders
USING (order_id);









/*Active sellers by year */
SELECT 
    order_year,
    COUNT(DISTINCT seller_id) AS no_of_sellers
FROM orders_orderItems
GROUP BY 
    order_year;



 
/*Active sellers by year and month */
SELECT 
    order_year,
    order_month,
    COUNT(DISTINCT seller_id) AS no_of_sellers
From orders_orderItems
GROUP BY 
    order_year,
    order_month;


SELECT 
    order_year,detailed_order_status,
    COUNT(DISTINCT seller_id) AS no_of_sellers
FROM orders_orderItems
GROUP BY 
    order_year,detailed_order_status;

/*END*/



/*Total Revenue by date */

/* Total Renvenue by year */
SELECT 
    order_year,
    SUM(total_price) AS total_sales
FROM 
    temp_orders
INNER JOIN 
    temp_denormalized_order_items 
USING (order_id)
WHERE 
    order_status = 'delivered'
GROUP BY 
    order_year;


/* Total Revenue by year,month*/
SELECT 
    order_year,order_month,
    SUM(total_price) AS total_sales
FROM 
    temp_orders
INNER JOIN 
    temp_denormalized_order_items 
USING (order_id)
WHERE 
    order_status = 'delivered'
GROUP BY 
    order_year,order_month
ORDER BY 
     order_year,order_month;



/*END*/
















/*Seller Section*/

/* number of orders that are fulfilled and Total sales 
by a seller 
grouped by state*/







CREATE VIEW  seller_KPIs_by_state AS(

SELECT 
    seller_state,
    COUNT(DISTINCT seller_id) AS no_of_sellers,
    COUNT(DISTINCT order_id) AS seller_order_count
FROM 
    temp_denormalized_order_items
GROUP BY 
    seller_state
ORDER BY 
    seller_order_count DESC
)



/* Only considering states where sellers sold more than 100 orders*/

CREATE VIEW relevant_seller_kpis AS
SELECT 
    *,
    seller_order_count / no_of_sellers AS avg_orders_per_seller
FROM 
    seller_kpis_by_state
WHERE 
    seller_order_count > 100;

select * from relevant_seller_kpis

SELECT * 
FROM get_statistics('relevant_seller_kpis', 'no_of_sellers');


SELECT * 
FROM get_statistics('relevant_seller_kpis', 'seller_order_count');

/* Grouping seller orders by state and also order_status*/

CREATE VIEW  seller_orders_by_state_orderStatus AS(

SELECT 
    seller_state,detailed_order_status,
    COUNT(DISTINCT order_id) AS seller_order_count
FROM 
    orders_orderItems
    
GROUP BY 
    seller_state,detailed_order_status
ORDER BY 
   seller_state,seller_order_count desc
)


select * from seller_orders_by_state_orderStatus


/* Revenue by state */

CREATE VIEW seller_revenue AS

SELECT 
    seller_state,
    SUM(total_price) AS revenue
FROM 
    orders_orderItems
WHERE 
    order_status = 'delivered'
GROUP BY 
    seller_state;



SELECT * 
FROM seller_revenue
ORDER BY 
    revenue DESC;


SELECT * 
FROM get_statistics('seller_revenue', 'revenue');


SELECT * 
FROM get_statistics('seller_kpis', 'seller_order_count');





/*Customer Section*/ 



 
 
 -- Number of customer orders by state
 
 
CREATE VIEW order_join_customer AS

SELECT 
   *
FROM 
    temp_orders 
INNER JOIN 
    customers 
USING (customer_id)



CREATE VIEW order_count_by_state AS

SELECT 
    customer_state,count(DISTINCT customer_unique_id) as no_of_customers_by_state,
    COUNT(order_id) AS order_count
FROM 
    order_join_customer
GROUP BY 
    customer_state
ORDER BY 
    order_count DESC;






SELECT * 
FROM order_count_by_state;

SELECT * 
FROM get_statistics('order_count_by_state', 'order_count');

CREATE VIEW order_count_by_state_order_status AS

SELECT 
    customer_state,detailed_order_status,
    COUNT(order_id) AS order_count
FROM 
    order_join_customer
GROUP BY 
    customer_state,detailed_order_status
ORDER BY 
    customer_state, order_count DESC;


select * from order_count_by_state_order_status



-- Total Revenue by customers  grouped by state
CREATE VIEW total_revenue_by_state AS

SELECT 
    customer_state,
    SUM(total_price) AS total_sales
FROM 
    orders_orderItems
LEFT JOIN 
    customers
USING (customer_id)
WHERE 
    order_status = 'delivered'
GROUP BY 
    customer_state
ORDER BY 
    total_sales DESC;

SELECT * 
FROM total_revenue_by_state;

SELECT * 
FROM get_statistics('total_revenue_by_state', 'total_sales');






/* product */


/* Updating cells with other where product category is not provided*/
UPDATE temp_denormalized_order_items 
SET
product_category_name_english=CASE WHEN product_category_name_english is null Then 'other' else product_category_name_english end


/* number of users by product category*/
CREATE VIEW customer_count_by_category AS

SELECT 
    product_category_name_english,
    COUNT(customer_unique_id) AS no_of_customers
FROM 
    customers
INNER JOIN 
    orders_orderItems
USING (customer_id)
GROUP BY 
    product_category_name_english
ORDER BY 
    no_of_customers DESC;




SELECT * 
FROM customer_count_by_category;


SELECT * 
FROM get_statistics('customer_count_by_category', 'no_of_customers');



/* Revenue by product category */
CREATE VIEW category_revenue_and_orders AS

SELECT 
    product_category_name_english,
    SUM(total_price) AS revenue
FROM 
    temp_denormalized_order_items
GROUP BY 
    product_category_name_english
ORDER BY 
    revenue DESC;


SELECT * 
FROM category_revenue_and_orders;

SELECT * 
FROM get_statistics('category_revenue_and_orders', 'revenue');




/* no of sellers by product category */

CREATE VIEW category_seller_count AS

SELECT 
    product_category_name_english,
    COUNT(DISTINCT seller_id) AS seller_count
FROM 
    temp_denormalized_order_items
GROUP BY 
    product_category_name_english
ORDER BY COUNT(DISTINCT seller_id) DESC;



SELECT * 
FROM category_seller_count;

SELECT * 
FROM get_statistics('category_seller_count', 'seller_count');




/* no of orders by product category */
SELECT 
    product_category_name_english,
    COUNT(DISTINCT order_id) AS seller_count
FROM 
    temp_denormalized_order_items
GROUP BY 
    product_category_name_english
ORDER BY COUNT(DISTINCT order_id) DESC;



/* categories and thier corresponding states where the categories were sold the most */


CREATE VIEW ranked_orders_category_state AS
SELECT 
    product_category_name_english,
    seller_state,
    order_count,
    DENSE_RANK() OVER (
        PARTITION BY product_category_name_english 
        ORDER BY order_count DESC
    ) AS rank_order_count_by_category_state
FROM (
    SELECT 
        product_category_name_english,
        seller_state,
        COUNT(DISTINCT order_id) AS order_count
    FROM 
        temp_denormalized_order_items
    GROUP BY 
        product_category_name_english, 
        seller_state
) AS orders_by_category;


SELECT 
    product_category_name_english,
    seller_state,
    order_count
FROM 
    ranked_orders_category_state
WHERE 
    rank_order_count_by_category_state = 1;


SELECT 
    product_category_name_english,
    seller_state,
    order_count
FROM 
    ranked_orders_category_state
WHERE 
    rank_order_count_by_category_state = 2;







/*end*/










 
















