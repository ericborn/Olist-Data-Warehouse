/*
Eric Born
Oline data warehouse project
*/

-- Rename CSV imported table names for the business transactions data
USE Olist_Orders
EXEC sp_rename 'olist_customers_dataset', 'customers'
EXEC sp_rename 'olist_geolocation_dataset', 'geolocation'
EXEC sp_rename 'olist_order_items_dataset', 'order_items'
EXEC sp_rename 'olist_order_payments_dataset', 'order_payments'
EXEC sp_rename 'olist_order_reviews_dataset', 'order_reviews'
EXEC sp_rename 'olist_orders_dataset', 'orders'
EXEC sp_rename 'olist_products_dataset', 'products'
EXEC sp_rename 'olist_sellers_dataset', 'sellers'
EXEC sp_rename 'product_category_name_translation', 'category'

-- Rename CSV imported table names for the marketing data
USE Olist_Marketing
EXEC sp_rename 'olist_closed_deals_dataset', 'closed_deals'
EXEC sp_rename 'olist_marketing_qualified_leads_dataset', 'leads'

-- script to output database schema
-- Provided by lucidchart.com with their import data feature
SELECT 'sqlserver' dbms,t.TABLE_CATALOG,t.TABLE_SCHEMA,t.TABLE_NAME,c.COLUMN_NAME,c.ORDINAL_POSITION,c.DATA_TYPE,
c.CHARACTER_MAXIMUM_LENGTH,n.CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.TABLES t 
LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_CATALOG=c.TABLE_CATALOG AND t.TABLE_SCHEMA=c.TABLE_SCHEMA AND t.TABLE_NAME=c.TABLE_NAME 
LEFT JOIN(INFORMATION_SCHEMA.KEY_COLUMN_USAGE k 
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS n ON k.CONSTRAINT_CATALOG=n.CONSTRAINT_CATALOG 
AND k.CONSTRAINT_SCHEMA=n.CONSTRAINT_SCHEMA AND k.CONSTRAINT_NAME=n.CONSTRAINT_NAME 
LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r ON k.CONSTRAINT_CATALOG=r.CONSTRAINT_CATALOG AND k.CONSTRAINT_SCHEMA=r.CONSTRAINT_SCHEMA 
AND k.CONSTRAINT_NAME=r.CONSTRAINT_NAME)ON c.TABLE_CATALOG=k.TABLE_CATALOG AND c.TABLE_SCHEMA=k.TABLE_SCHEMA AND c.TABLE_NAME=k.TABLE_NAME AND c.COLUMN_NAME=k.COLUMN_NAME 
WHERE t.TABLE_TYPE='BASE TABLE';

----------------------------
IF EXISTS 
   (
     SELECT name FROM master.dbo.sysdatabases 
    WHERE name = N'Olist_DW'
    )
BEGIN
    SELECT 'Database already exists' AS Message
END
ELSE
BEGIN
    CREATE DATABASE [Olist_DW]
    SELECT 'Olist_DW database has been created'
END;

-- Code to setup the product table within the warehouse
--DROP SEQUENCE product_key
CREATE SEQUENCE product_key
START WITH 1000 
INCREMENT BY 1;

--DROP TABLE product
-- Select product names from Olist category
-- move into product table in the warehouse
-- filters out a header row that slipped in on the import
USE Olist_DW
SELECT NEXT VALUE FOR product_key AS product_key, Product_category_name_english AS 'product'
INTO product
FROM Olist_Orders.dbo.category
WHERE Product_category_name_english != 'Product_category_name_english';

-- Select business_segment from Olist marketing closed_deals
-- Move into product table in the warehouse where the product doesn't already exist
INSERT INTO product
SELECT NEXT VALUE FOR product_key AS product_key, cd.business_segment AS 'product' 
FROM (SELECT DISTINCT business_segment 
	  FROM Olist_Marketing.dbo.closed_deals
	  WHERE business_segment IS NOT NULL AND business_segment NOT IN (SELECT DISTINCT product FROM product)) cd;

--SELECT * FROM product;

SELECT DISTINCT geolocation_city, geolocation_state, geolocation_zip_code_prefix
FROM Olist_Orders.dbo.geolocation;

-- Code to setup the location table within the warehouse
--DROP SEQUENCE location_key
CREATE SEQUENCE location_key
START WITH 1
INCREMENT BY 1;

--DROP TABLE location
-- Select product names from Olist category
-- move into product table in the warehouse
-- filters out a header row that slipped in on the import
USE Olist_DW
SELECT NEXT VALUE FOR location_key AS location_key,
gl.geolocation_city AS 'city', gl.geolocation_state AS 'state', gl.geolocation_zip_code_prefix AS 'zip'
INTO location
FROM (SELECT DISTINCT geolocation_city, geolocation_state, geolocation_zip_code_prefix
	  FROM Olist_Orders.dbo.geolocation) gl;

--SELECT * FROM location;

-- Code to setup the location table within the warehouse
--DROP SEQUENCE origin_key
CREATE SEQUENCE origin_key
START WITH 1
INCREMENT BY 1;

--DROP SEQUENCE lead_type_key
CREATE SEQUENCE lead_type_key
START WITH 1
INCREMENT BY 1;

--DROP SEQUENCE business_type_key
CREATE SEQUENCE business_type_key
START WITH 1
INCREMENT BY 1;

--DROP TABLE origin
-- Setup marketing dimension tables
SELECT NEXT VALUE FOR origin_key AS 'origin_key', l.origin
INTO origin
FROM (SELECT DISTINCT origin FROM Olist_Marketing.dbo.leads) l;

--DROP TABLE lead_type
SELECT NEXT VALUE FOR lead_type_key AS 'lead_type_key', cd.lead_type
INTO lead_type
FROM (SELECT DISTINCT lead_type FROM Olist_Marketing.dbo.closed_deals) cd;

--DROP TABLE business_type
SELECT NEXT VALUE FOR business_type_key AS 'business_type_key', cd.business_type 
INTO business_type
FROM (SELECT DISTINCT business_type FROM Olist_Marketing.dbo.closed_deals) cd;

--SELECT * FROM origin;
--SELECT * FROM lead_type;
--SELECT * FROM business_type;


--DROP TABLE orders
-- Gathers the initial data from the Olist database and insert it into a table called orders in the Olist_DW database
-- does a convert on the time.date_key from INT to DATE
-- also converts orders order_purchase_timestamp from DATETIME to DATE
-- filters out any canceled orders and only orders earlier than 2019 for SSIS demonstration purposes
USE Olist_DW
SELECT t.date_key, l.location_key, p2.product_key, oi.seller_id, 
SUM(oi.price) AS 'sales_total', COUNT(oi.product_id) AS 'sales_quantity'
INTO orders
FROM Olist_Orders.dbo.orders o
INNER JOIN Olist_Orders.dbo.order_items oi ON oi.order_id = o.order_id
INNER JOIN Olist_Orders.dbo.products p ON p.product_id = oi.product_id
INNER JOIN Olist_Orders.dbo.category c ON c.product_category_name = p.product_category_name
INNER JOIN Olist_DW.dbo.product p2 ON p2.product = c.Product_category_name_english
INNER JOIN Olist_Orders.dbo.sellers s ON s.seller_id = oi.seller_id
INNER JOIN Olist_DW.dbo.time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
INNER JOIN Olist_DW.dbo.location l ON l.zip = s.seller_zip_code_prefix AND l.city = s.seller_city
WHERE o.order_status != 'canceled' AND order_purchase_timestamp < '20190101'
GROUP BY t.date_key, l.location_key, p2.product_key, oi.seller_id;

--SELECT * FROM orders;

--DROP TABLE conversions
-- select data from the marketing db to move into the data warehouse
-- does a convert on the time.date_key from INT to DATE
-- also converts orders order_purchase_timestamp from DATETIME to DATE
-- only selects rows that have an origin, not null or unknown
USE Olist_DW
SELECT DISTINCT
t.date_key, p.product_key, o.origin_key, lt.lead_type_key, bt.business_type_key,
AVG(DATEDIFF(HOUR, l.first_contact_date, cd.won_date)) AS 'avg_hrs_convert'
INTO conversions
FROM Olist_Marketing.dbo.leads l
INNER JOIN Olist_Marketing.dbo.closed_deals cd ON l.mql_id = cd.mql_id
INNER JOIN Olist_Orders.dbo.sellers s ON s.seller_id = cd.seller_id
INNER JOIN Olist_Orders.dbo.order_items oi ON oi.seller_id = s.seller_id
INNER JOIN time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,cd.won_date,112)
INNER JOIN product p ON p.product = cd.business_segment
INNER JOIN origin o ON o.origin = l.origin
INNER JOIN lead_type lt ON lt.lead_type = cd.lead_type
INNER JOIN business_type bt ON bt.business_type = cd.business_type
WHERE l.origin IS NOT NULL AND l.origin != 'unknown'
GROUP BY t.date_key, p.product_key, o.origin_key, lt.lead_type_key, bt.business_type_key;

-- delete the single row with a negative conversion hours
DELETE FROM conversions
WHERE avg_hrs_convert < 1;

--SELECT * FROM conversions;

----------------------------------------------
-- Create indexes for the top sellers by volume
USE Olist_Orders
CREATE INDEX orders_purchase_id_indx
ON orders ([order_purchase_timestamp]) INCLUDE ([order_id]);

CREATE INDEX order_items_order_id_indx
ON order_items ([order_id]) INCLUDE ([product_id], [seller_id]);

CREATE INDEX products_prod_id_category_indx
ON products (product_id) INCLUDE ([product_category_name]);

-- Create indexes for the orders table of the data warehouse total units and revenue queries
USE Olist_DW
CREATE INDEX orders_total_units_indx
ON orders ([date_key]) INCLUDE ([product_category], [seller_id], seller_state, [Units_Sold]);

CREATE INDEX orders_total_revenue_indx
ON orders ([date_key]) INCLUDE ([product_category], [seller_id], seller_state, [Total_value]);

------------------
-- Turn on statistics to measure performance between OLTP DB and DW
SET STATISTICS IO ON
SET STATISTICS TIME ON
--------------------

-- Top 5 seller id, seller state, product category by volume from the orders database
USE Olist_Orders
SELECT TOP 5 t.year, s.seller_id, s.seller_state, c.product_category_name_english, COUNT(product_category_name_english)  AS 'Total_Units'--, SUM(oi.Units_Sold)
FROM Olist_Orders.dbo.orders o
INNER JOIN Olist_Orders.dbo.order_items oi ON oi.order_id = o.order_id
INNER JOIN Olist_Orders.dbo.products p ON p.product_id = oi.product_id
INNER JOIN Olist_Orders.dbo.category c ON c.product_category_name = p.product_category_name
INNER JOIN Olist_Orders.dbo.sellers s ON s.seller_id = oi.seller_id
INNER JOIN Olist_Orders.dbo.time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
WHERE t.Year = 2018
GROUP BY t.Year, s.seller_id, s.seller_state, c.product_category_name_english
ORDER BY Total_Units DESC;

-- Data warehouse query
USE Olist_DW
SELECT TOP 5 t.Year, o.seller_id, l.state, p.product, SUM(o.sales_quantity) AS 'Total_Units'
FROM Olist_DW.dbo.orders o
INNER JOIN Olist_DW.dbo.time_period t ON t.date_key = o.date_key
INNER JOIN Olist_DW.dbo.location l ON l.location_key = o.location_key
INNER JOIN Olist_DW.dbo.product p ON p.product_key = o.product_key
WHERE t.Year = 2018
GROUP BY t.Year, o.seller_id, l.state, p.product
ORDER BY Total_Units DESC;

-- Top 5 seller id, seller state, product category by revenue from the orders database
USE Olist_Orders
SELECT TOP 5 t.year, s.seller_id, s.seller_state, c.product_category_name_english, ROUND(SUM(oi.price), 2) AS 'Total_Revenue'
FROM Olist_Orders.dbo.orders o
INNER JOIN Olist_Orders.dbo.order_items oi ON oi.order_id = o.order_id
INNER JOIN Olist_Orders.dbo.products p ON p.product_id = oi.product_id
INNER JOIN Olist_Orders.dbo.category c ON c.product_category_name = p.product_category_name
INNER JOIN Olist_Orders.dbo.sellers s ON s.seller_id = oi.seller_id
INNER JOIN Olist_Orders.dbo.time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
WHERE t.Year = 2018
GROUP BY t.Year, s.seller_id, s.seller_state, c.product_category_name_english
ORDER BY Total_Revenue DESC;

-- Data warehouse query
USE Olist_DW
SELECT TOP 5 t.Year, o.seller_id, l.state, p.product, ROUND(SUM(o.sales_total), 2) AS 'Total_Revenue'
FROM orders o
INNER JOIN Olist_DW.dbo.time_period t ON t.date_key = o.date_key
INNER JOIN Olist_DW.dbo.location l ON l.location_key = o.location_key
INNER JOIN Olist_DW.dbo.product p ON p.product_key = o.product_key
WHERE t.Year = 2018
GROUP BY t.Year, o.seller_id, l.state, p.product
ORDER BY Total_Revenue DESC;


---------------------------
USE Olist_Marketing
SELECT TOP 5 t.year, l.origin, cd.lead_type, AVG(DATEDIFF(HOUR, l.first_contact_date, cd.won_date)) AS 'avg_hrs_convert'
FROM closed_deals cd
INNER JOIN leads l ON l.mql_id = cd.mql_id
INNER JOIN time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,cd.won_date,112)
GROUP BY t.year, l.origin, cd.lead_type
ORDER BY avg_hrs_convert ASC;

-- Find the top 5 average fastest origin and lead types to convert through a marketing funnel
USE Olist_DW
SELECT TOP 5 t.year, c.origin, c.lead_type, MIN(avg_hrs_convert)-- AS 'avg_hrs_convert'
FROM conversions c
INNER JOIN time_period t ON t.date_key = c.date_key 
GROUP BY t.year, c.origin, c.lead_type--, avg_hrs_convert
ORDER BY avg_hrs_convert ASC;

SELECT * FROM conversions

---------
-- Remove fake data
--DELETE FROM Olist_Orders.dbo.orders
--WHERE order_purchase_timestamp > '20181231'
--DELETE FROM Olist_Orders.dbo.order_items
--WHERE shipping_limit_date = '20190105 12:30:00'
--DELETE FROM Olist_DW.dbo.orders
--WHERE date_key > '20181231'

-- Create fake 2019 orders data
USE Olist_Orders
INSERT INTO orders
VALUES
('023345456754dfg67875647032168abc', '9ef432eb6251297304e76186b10a928d', 'delivered', '20190101 12:30:00', '20190102 12:30:00', '20190103 12:30:00', '20190104 12:30:00', '20190105 12:30:00'),
('rsfdgrtgh455643hhtkiusdng2345246', 'b0830fb4747a6c6d20dea0b8c802d7ef', 'delivered', '20190102 12:30:00', '20190103 12:30:00', '20190104 12:30:00', '20190105 12:30:00', '20190106 12:30:00'),
('sdafgfdh45647fgh4564567hge456453', '41ce2a54c0b03bf3443c3d931a367089', 'delivered', '20190103 12:30:00', '20190104 12:30:00', '20190105 12:30:00', '20190106 12:30:00', '20190107 12:30:00'),
('2343dfg34543g3476jh576682tsrgrfg', 'f88197465ea7920adcdbec7375364d82', 'delivered', '20190104 12:30:00', '20190105 12:30:00', '20190106 12:30:00', '20190107 12:30:00', '20190108 12:30:00'),
('34565467tr453646vbdfg345346dgdrt', '8ab97904e6daea8866dbdbc4fb7aad2c', 'delivered', '20190105 12:30:00', '20190106 12:30:00', '20190107 12:30:00', '20190108 12:30:00', '20190201 12:30:00'),
('45654ydfgfdg45er65fdgh5664fthb54', '503740e9ca751ccdda7ba28e9ab8f608', 'delivered', '20190106 12:30:00', '20190107 12:30:00', '20190108 12:30:00', '20190109 12:30:00', '20190211 12:30:00');

INSERT INTO order_items
VALUES
('023345456754dfg67875647032168abc', 1, '4244733e06e7ecb4970a6e2683c13e61', '48436dade18ac8b2bce089ec2a041202', '20190105 12:30:00', 58.9, 13.29),
('rsfdgrtgh455643hhtkiusdng2345246', 1, 'e5f2d52b802189ee658865ca93d83a8f', 'dd7ddc04e1b6c2c614352b383efe2d36', '20190105 12:30:00', 239.9, 39),
('sdafgfdh45647fgh4564567hge456453', 1, 'c777355d18b72b67abbeef9df44fd0fd', 'dd7ddc04e1b6c2c614352b383efe2d36', '20190105 12:30:00', 199, 17.8),
('2343dfg34543g3476jh576682tsrgrfg', 1, '7634da152a4610f1595efa32f14722fc', '5b51032eddd242adc84c38acab88f23d', '20190105 12:30:00', 12.99, 12.79),
('34565467tr453646vbdfg345346dgdrt', 1, 'ac6c3623068f30de03045865e4e10089', '9d7a1d34a5052409006425275ba1c2b4', '20190105 12:30:00', 199.9, 11.85),
('45654ydfgfdg45er65fdgh5664fthb54', 1, 'ef92defde845ab8450f9d70c526ef70f', 'df560393f3a51e74553ab94004ba5c87', '20190105 12:30:00', 239.9, 11.4);


--FAKE DATA FOR order_items
SELECT TOP 10 * 
FROM order_items

-- Used to find new orders from the previous day and insert them into the data warehouse
-- filters greater than or equal to yesterday at midnight until less than today at midnight
-- dates are hard coded, but with dynamic SQL using date add, diff and get date the code can always grab
-- only records that were created yesterday

--USE Olist_DW
SELECT t.date_key, l.location_key, p2.product_key, oi.seller_id, 
SUM(oi.price) AS 'sales_total', COUNT(oi.product_id) AS 'sales_quantity'
FROM Olist_Orders.dbo.orders o
INNER JOIN Olist_Orders.dbo.order_items oi ON oi.order_id = o.order_id
INNER JOIN Olist_Orders.dbo.products p ON p.product_id = oi.product_id
INNER JOIN Olist_Orders.dbo.category c ON c.product_category_name = p.product_category_name
INNER JOIN Olist_DW.dbo.product p2 ON p2.product = c.Product_category_name_english
INNER JOIN Olist_Orders.dbo.sellers s ON s.seller_id = oi.seller_id
INNER JOIN Olist_DW.dbo.time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
INNER JOIN Olist_DW.dbo.location l ON l.zip = s.seller_zip_code_prefix AND l.city = s.seller_city
WHERE o.order_status != 'canceled'
GROUP BY t.date_key, l.location_key, p2.product_key, oi.seller_id;

-- view orders data greater than 2018
USE Olist_Orders
SELECT order_purchase_timestamp 
FROM orders
WHERE order_purchase_timestamp > '20181231'

USE Olist_Orders
SELECT * 
FROM orders
WHERE order_purchase_timestamp > '20181231'

-- view orders data greater than 2018
USE Olist_DW
SELECT * 
FROM orders
WHERE Date_Key > '20181231'