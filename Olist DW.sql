/*
Eric Born
Oline data warehouse project
*/

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
END

USE Olist_DW

--DROP TABLE orders

-- Gathers the initial data from the Olist database and insert it into a table called orders in the Olist_DW database
-- Uses a case statement to correct misspellings of S�o Paulo
-- does a convert on the time.datekey from INT to DATE
-- also converts orders order_purchase_timestamp from DATETIME to DATE
-- filters out any canceled orders and only orders earlier than 2019
SELECT t.DateKey, c.product_category_name_english AS 'product_category', oi.seller_id, 
CASE
	WHEN s.seller_city LIKE 'sao pau%' OR seller_city LIKE 'sao palu%'
	THEN 'S�o Paulo'
	ELSE s.seller_city
END AS 'seller_city',
s.seller_state, SUM(oi.price) AS 'Total_Value', COUNT(oi.product_id) AS 'Units_Sold'
INTO orders
FROM Olist.dbo.orders o
JOIN Olist.dbo.order_items oi ON oi.order_id = o.order_id
JOIN Olist.dbo.products p ON p.product_id = oi.product_id
JOIN Olist.dbo.category c ON c.product_category_name = p.product_category_name
JOIN Olist.dbo.sellers s ON s.seller_id = oi.seller_id
JOIN time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.DateKey,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
WHERE o.order_status != 'canceled' AND order_purchase_timestamp < '20190101'
GROUP BY t.DateKey, o.order_purchase_timestamp, c.product_category_name_english, oi.seller_id, s.seller_city, s.seller_state;

-- Create indexes for the top sellers by volume
USE Olist
CREATE INDEX orders_purchase_id_indx
ON orders ([order_purchase_timestamp]) INCLUDE ([order_id]);

CREATE INDEX order_items_order_id_indx
ON order_items ([order_id]) INCLUDE ([product_id], [seller_id]);

CREATE INDEX products_prod_id_category_indx
ON products (product_id) INCLUDE ([product_category_name]);

-- Create indexes for the orders table of the data warehouse total units and revenue queries
USE Olist_DW
CREATE INDEX orders_total_units_indx
ON orders ([DateKey]) INCLUDE ([product_category], [seller_id], seller_state, [Units_Sold]);

CREATE INDEX orders_total_revenue_indx
ON orders ([DateKey]) INCLUDE ([product_category], [seller_id], seller_state, [Total_value]);

------------------
-- Turn on statistics to measure performance between OLTP DB and DW
SET STATISTICS IO ON
SET STATISTICS TIME ON
--------------------

-- Top 5 seller id, seller state, product category by volume
-- Original database query
USE Olist
SELECT TOP 5 t.year, s.seller_id, s.seller_state, c.product_category_name_english, COUNT(product_category_name_english)  AS 'Total_Units'--, SUM(oi.Units_Sold)
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN category c ON c.product_category_name = p.product_category_name
JOIN sellers s ON s.seller_id = oi.seller_id
JOIN time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.DateKey,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
WHERE t.Year = 2018
GROUP BY t.Year, s.seller_id, s.seller_state, c.product_category_name_english
ORDER BY Total_Units DESC;

-- Data warehouse query
USE Olist_DW
SELECT TOP 5 t.Year, o.seller_id, o.seller_state, o.product_category, SUM(o.Units_Sold) AS 'Total_Units'
FROM orders o
JOIN time_period t ON t.DateKey = o.DateKey
WHERE t.Year = 2018
GROUP BY t.Year, o.seller_id, o.seller_state, o.product_category
ORDER BY Total_Units DESC;

-- Top 5 seller id, seller state, product category by revenue
-- Original database query
USE Olist
SELECT TOP 5 t.year, s.seller_id, s.seller_state, c.product_category_name_english, ROUND(SUM(oi.price), 2) AS 'Total_Revenue'
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN category c ON c.product_category_name = p.product_category_name
JOIN sellers s ON s.seller_id = oi.seller_id
JOIN time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.DateKey,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
WHERE t.Year = 2018
GROUP BY t.Year, s.seller_id, s.seller_state, c.product_category_name_english
ORDER BY Total_Revenue DESC;

-- Data warehouse query
USE Olist_DW
SELECT TOP 5 t.Year, o.seller_id, o.seller_state, o.product_category, ROUND(SUM(o.Total_Value), 2) AS 'Total_Revenue'
FROM orders o
JOIN time_period t ON t.DateKey = o.DateKey
WHERE t.Year = 2018
GROUP BY t.Year, o.seller_id, o.seller_state, o.product_category
ORDER BY Total_Revenue DESC;


---------
-- Remove fake data
--DELETE FROM orders
--WHERE order_purchase_timestamp > '20181231'

-- Create fake 2019 orders data
USE Olist
INSERT INTO orders
VALUES
('e481f51cbdc54678b7cc49136f2d6af7', '9ef432eb6251297304e76186b10a928d', 'delivered', '20190101 12:30:00', '20190102 12:30:00', '20190103 12:30:00', '20190104 12:30:00', '20190105 12:30:00'),
('53cdb2fc8bc7dce0b6741e2150273451', 'b0830fb4747a6c6d20dea0b8c802d7ef', 'delivered', '20190102 12:30:00', '20190103 12:30:00', '20190104 12:30:00', '20190105 12:30:00', '20190106 12:30:00'),
('47770eb9100c2d0c44946d9cf07ec65d', '41ce2a54c0b03bf3443c3d931a367089', 'delivered', '20190103 12:30:00', '20190104 12:30:00', '20190105 12:30:00', '20190106 12:30:00', '20190107 12:30:00'),
('949d5b44dbf5de918fe9c16f97b45f8a', 'f88197465ea7920adcdbec7375364d82', 'delivered', '20190104 12:30:00', '20190105 12:30:00', '20190106 12:30:00', '20190107 12:30:00', '20190108 12:30:00'),
('ad21c59c0840e6cb83a9ceb5573f8159', '8ab97904e6daea8866dbdbc4fb7aad2c', 'delivered', '20190105 12:30:00', '20190106 12:30:00', '20190107 12:30:00', '20190108 12:30:00', '20190201 12:30:00'),
('a4591c265e18cb1dcee52889e2d8acc3', '503740e9ca751ccdda7ba28e9ab8f608', 'delivered', '20190106 12:30:00', '20190107 12:30:00', '20190108 12:30:00', '20190109 12:30:00', '20190211 12:30:00'),
('136cce7faa42fdb2cefd53fdc79a6098', 'ed0271e0b7da060a393796590e7b737a', 'delivered', '20190107 12:30:00', '20190108 12:30:00', '20190109 12:30:00', '20190112 12:30:00', '20190203 12:30:00'),
('6514b8ad8028c9f2cc2374ded245783f', '9bdf08b4b3b52b5526ff42d37d47f222', 'delivered', '20190108 12:30:00', '20190109 12:30:00', '20190110 12:30:00', '20190113 12:30:00', '20190119 12:30:00'),
('76c6e866289321a7c93b82b54852dc33', 'f54a9f0e6b351c431402b8461ea51999', 'delivered', '20190109 12:30:00', '20190110 12:30:00', '20190111 12:30:00', '20190115 12:30:00', '20190119 12:30:00'),
('e69bfb5eb88e0ed6a785585b27e16dbf', '494dded5b201313c64ed7f100595b95c', 'delivered', '20190110 12:30:00', '20190111 12:30:00', '20190112 12:30:00', '20190116 12:30:00', '20190119 12:30:00'),
('e6ce16cb79ec1d90b1da9085a6118aeb', '31ad1d1b63eb9962463f764d4e6e0c9d', 'delivered', '20190111 12:30:00', '20190112 12:30:00', '20190113 12:30:00', '20190117 12:30:00', '20190122 12:30:00'),
('34513ce0c4fab462a55830c0989c7edb', '7711cf624183d843aafe81855097bc37', 'delivered', '20190112 12:30:00', '20190113 12:30:00', '20190114 12:30:00', '20190118 12:30:00', '20190123 12:30:00'),
('82566a660a982b15fb86e904c8d32918', 'd3e3b74c766bc6214e0c830b17ee2341', 'delivered', '20190113 12:30:00', '20190114 12:30:00', '20190115 12:30:00', '20190120 12:30:00', '20190125 12:30:00'),
('5ff96c15d0b717ac6ad1f3d77225a350', '19402a48fe860416adf93348aba37740', 'delivered', '20190114 12:30:00', '20190115 12:30:00', '20190116 12:30:00', '20190122 12:30:00', '20190127 12:30:00'),
('432aaf21d85167c2c86ec9448c4e42cc', '3df704f53d3f1d4818840b34ec672a9f', 'delivered', '20190115 12:30:00', '20190116 12:30:00', '20190117 12:30:00', '20190123 12:30:00', '20190126 12:30:00'),
('dcb36b511fcac050b97cd5c05de84dc3', '3b6828a50ffe546942b7a473d70ac0fc', 'delivered', '20190116 12:30:00', '20190117 12:30:00', '20190118 12:30:00', '20190125 12:30:00', '20190126 12:30:00'),
('403b97836b0c04a622354cf531062e5f', '738b086814c6fcc74b8cc583f8516ee3', 'delivered', '20190117 12:30:00', '20190118 12:30:00', '20190119 12:30:00', '20190130 12:30:00', '20190205 12:30:00'),
('116f0b09343b49556bbad5f35bee0cdf', '3187789bec990987628d7a9beb4dd6ac', 'delivered', '20190118 12:30:00', '20190119 12:30:00', '20190120 12:30:00', '20190129 12:30:00', '20190204 12:30:00'),
('85ce859fd6dc634de8d2f1e290444043', '059f7fc5719c7da6cbafe370971a8d70', 'delivered', '20190119 12:30:00', '20190120 12:30:00', '20190121 12:30:00', '20190131 12:30:00', '20190211 12:30:00')

-- Used to find new orders from the previous day and insert them into the data warehouse
-- filters greater than or equal to yesterday at midnight until less than today at midnight
-- dates are hard coded, but with dynamic SQL using date add, diff and get date the code can always grab
-- only records that were created yesterday
USE Olist_DW
INSERT INTO orders (DateKey, product_category, seller_id, seller_city, seller_state, total_value, units_sold)
SELECT t.DateKey, c.product_category_name_english AS 'product_category', oi.seller_id, 
CASE
	WHEN s.seller_city LIKE 'sao pau%' OR seller_city LIKE 'sao palu%'
	THEN 'S�o Paulo'
	ELSE s.seller_city
END AS 'seller_city',
s.seller_state, SUM(oi.price) AS 'Total_Value', COUNT(oi.product_id) AS 'Units_Sold'
FROM Olist.dbo.orders o
JOIN Olist.dbo.order_items oi ON oi.order_id = o.order_id
JOIN Olist.dbo.products p ON p.product_id = oi.product_id
JOIN Olist.dbo.category c ON c.product_category_name = p.product_category_name
JOIN Olist.dbo.sellers s ON s.seller_id = oi.seller_id
JOIN time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.DateKey,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
WHERE o.order_status != 'canceled' AND order_purchase_timestamp >= '20190101 00:00:00' AND order_purchase_timestamp < '20190102 00:00:00'
--order_purchase_timestamp >= DATEADD(DAY, DATEDIFF(DAY,1,GETDATE()),0)
--AND order_purchase_timestamp < DATEADD(DAY, DATEDIFF(DAY,0,GETDATE()),0)
GROUP BY t.DateKey, o.order_purchase_timestamp, c.product_category_name_english, oi.seller_id, s.seller_city, s.seller_state;

SELECT *
FROM orders
ORDER BY order_purchase_timestamp desc