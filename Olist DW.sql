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

-- Gathers the data from the Olist database and insert it into a table called orders in the Olist_DW database
-- Uses a case statement to correct misspellings of S�o Paulo
-- does a convert on the time.datekey from INT to DATE
-- also converts orders order_purchase_timestamp from DATETIME to DATE
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
GROUP BY t.DateKey, o.order_purchase_timestamp, c.product_category_name_english, oi.seller_id, s.seller_city, s.seller_state
 
-- Top 5 sellers by volume
SELECT TOP 5 seller_id, t.Year, SUM(Units_Sold) AS 'Total_Units'
FROM orders o
JOIN time_period t ON t.DateKey = o.DateKey
WHERE t.Year = 2018
GROUP BY seller_id, t.Year
ORDER BY Total_Units DESC

-- Top 5 sellers by revenue
SELECT TOP 5 seller_id, t.Year, ROUND(SUM(Total_Value), 2) AS 'Total_Revenue'
FROM orders o
JOIN time_period t ON t.DateKey = o.DateKey
WHERE t.Year = 2018
GROUP BY seller_id, t.Year
ORDER BY Total_Revenue DESC


SELECT *
FROM orders

