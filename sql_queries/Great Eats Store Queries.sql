/*
PROJECT: Revenue Analytics Data Pipeline for a Multi-City Food Delivery Platform

Business Context:
Great Eats operates a food delivery platform across approximately 100 cities.
Following a recent decline in revenue, business stakeholders (Marketing,
Operations, Finance) require a reliable and centralized data infrastructure
to investigate performance trends and uncover root causes.

The objective is to design a structured MySQL-based data pipeline that enables:

• Weekly operational data ingestion
• Structured data modeling
• Revenue computation standardization
• Business-ready analytical views
• Executive-level KPI reporting

This project supports scalable analytics, revenue monitoring, and
cross-functional decision-making.
*/


/* =======================================================
SECTION 01 — DATABASE INITIALIZATION & ENVIRONMENT SETUP
Objective: Create dedicated analytics environment
======================================================= */

DROP DATABASE IF EXISTS GL_EATS;
CREATE DATABASE GL_EATS;
USE gl_eats;

/* =============================================================
SECTION 02 — RAW DATA INGESTION LAYER (STAGING TABLE)
Objective: Create temporary ingestion layer for weekly uploads
============================================================= */
 
DROP TABLE IF EXISTS temp_t;

CREATE TABLE temp_t (
	WEEK_NUMBER INTEGER,
	CUSTOMER_ID INTEGER,
	CUSTOMER_NAME VARCHAR(20),
	COUNTRY_CD VARCHAR(25),
	EMAIL_ADDRESS VARCHAR(50),
	ORDER_ID INTEGER,
	ORDER_COST INTEGER,
	ORDER_ITEMS INTEGER,
	DISCOUNT INTEGER,
	DELIVERY_EMP_ID INTEGER,
	DELIVERY_ID INTEGER,
	STAR_RATING INTEGER,
	DELIVERY_STATUS INTEGER,
	RESTAURANT_ID INTEGER,
	RESTAURANT_NAME VARCHAR(50),
	CITY_NAME VARCHAR(60),
	ADDRESS VARCHAR(100),
	LOCALITY VARCHAR(60),
	LONGITUDE DECIMAL(14,8),
	LATITUDE DECIMAL(14,8),
	CUISINES VARCHAR(100),
	RESTAURANT_RATING DECIMAL(2,1),
	PRIMARY KEY (ORDER_ID, RESTAURANT_ID, DELIVERY_ID, CUSTOMER_ID)
);

/* ======================================================
SECTION 03 — CORE DATA MODEL (PRODUCTION TABLES)
Objective: Create normalized business-ready data tables
====================================================== */

-- Master Fact Table --

DROP TABLE IF EXISTS gl_eats_t;
CREATE TABLE gl_eats_t LIKE temp_t;

-- Dimension Tables --

-- Customer Table
DROP TABLE IF EXISTS gl_eats_cust_t;
CREATE TABLE gl_eats_cust_t (
	CUSTOMER_ID INTEGER PRIMARY KEY,
	CUSTOMER_NAME VARCHAR(20),
	EMAIL_ADDRESS VARCHAR(50),
	COUNTRY_CD VARCHAR(15)
); 

-- Restaurant Table
DROP TABLE IF EXISTS gl_eats_rest_t;
CREATE TABLE gl_eats_rest_t (
	RESTAURANT_ID INTEGER PRIMARY KEY,
	RESTAURANT_NAME VARCHAR(60),
	CITY_NAME VARCHAR(60),
	COUNTRY_CD VARCHAR(60),
	ADDRESS VARCHAR(100),
	LOCALITY VARCHAR(100),
	LATITUDE DECIMAL(14,8),
	LONGITUDE DECIMAL(14,8),
	CUISINES VARCHAR(100),
	RESTAURANT_RATING DECIMAL(2,1)
);

-- Fact Tables --

-- Order Table
DROP TABLE IF EXISTS gl_eats_ord_t;
CREATE TABLE gl_eats_ord_t (
	ORDER_ID INTEGER PRIMARY KEY,
	DELIVERY_EMP_ID INTEGER,
	CUSTOMER_ID INTEGER,
	RESTAURANT_ID INTEGER,
	ORDER_COST INTEGER,
	ORDER_ITEMS INTEGER,
	DISCOUNT INTEGER,
	WEEK_NUMBER INTEGER
);

-- Delivery Table
DROP TABLE IF EXISTS gl_eats_del_t;
CREATE TABLE gl_eats_del_t (	
	DELIVERY_ID INTEGER PRIMARY KEY,
	DELIVERY_EMP_ID INTEGER,
	ORDER_ID INTEGER,
	DELIVERY_STATUS VARCHAR(10),
	RESTAURANT_ID INTEGER,
	STAR_RATING INTEGER,
	WEEK_NUMBER INTEGER
);

/* =======================================================
SECTION 04 — ETL STORED PROCEDURES (DATA PIPELINE LOGIC)
Objective: Automate structured transformation & loading
======================================================= */

-- Master Fact Table Procedure
DROP PROCEDURE IF EXISTS gl_eats_p;

DELIMITER $$ 
CREATE PROCEDURE gl_eats_p()
BEGIN
	INSERT INTO gl_eats.gl_eats_t (
		WEEK_NUMBER,
		CUSTOMER_ID,
		CUSTOMER_NAME,
		COUNTRY_CD,
		EMAIL_ADDRESS,
		ORDER_ID,
		ORDER_COST,
		ORDER_ITEMS,
		DISCOUNT,
		DELIVERY_EMP_ID,
		DELIVERY_ID,
		STAR_RATING,
		DELIVERY_STATUS,
		RESTAURANT_ID,
		RESTAURANT_NAME,
		CITY_NAME,
		ADDRESS,
		LOCALITY,
		LONGITUDE,
		LATITUDE,
		CUISINES,
		RESTAURANT_RATING
	) SELECT * FROM gl_eats.temp_t;
END;

-- Customer Procedure
DROP PROCEDURE IF EXISTS gl_eats_cust_p;

DELIMITER $$
CREATE PROCEDURE gl_eats_cust_p()
BEGIN
	INSERT INTO gl_eats_cust_t (
        CUSTOMER_ID, 
        CUSTOMER_NAME, 
        EMAIL_ADDRESS, 
        COUNTRY_CD
    )
    SELECT DISTINCT 
	CUSTOMER_ID, 
        CUSTOMER_NAME, 
        EMAIL_ADDRESS, 
        COUNTRY_CD 
	FROM gl_eats_t WHERE CUSTOMER_ID NOT IN (SELECT DISTINCT CUSTOMER_ID FROM gl_eats_cust_t);
END;

-- Restaurant Procedure
DROP PROCEDURE IF EXISTS gl_eats_rest_p;

DELIMITER $$
CREATE PROCEDURE gl_eats_rest_p()
BEGIN
    INSERT INTO gl_eats_rest_t (
        RESTAURANT_ID, 
        RESTAURANT_NAME, 
        CITY_NAME, 
        COUNTRY_CD,
        ADDRESS, 
        LOCALITY, 
        LONGITUDE, 
        LATITUDE, 
        CUISINES, 
        RESTAURANT_RATING
	) 
    SELECT DISTINCT 
	RESTAURANT_ID, 
        RESTAURANT_NAME, 
        CITY_NAME, 
        COUNTRY_CD,
        ADDRESS, 
        LOCALITY, 
        LONGITUDE, 
        LATITUDE, 
        CUISINES, 
        RESTAURANT_RATING
	FROM gl_eats_t WHERE RESTAURANT_ID NOT IN (SELECT DISTINCT RESTAURANT_ID FROM gl_eats_rest_t);
END;

-- Order Procedure
DROP PROCEDURE IF EXISTS gl_eats_ord_p;

DELIMITER $$
CREATE PROCEDURE gl_eats_ord_p(weeknum INTEGER)
BEGIN
	INSERT INTO gl_eats_ord_t (
	ORDER_ID, 
        DELIVERY_EMP_ID, 
        CUSTOMER_ID, 
        RESTAURANT_ID,
        ORDER_COST, 
        ORDER_ITEMS, 
        DISCOUNT, 
        WEEK_NUMBER
	) 
    SELECT DISTINCT
	ORDER_ID, 
        DELIVERY_EMP_ID, 
        CUSTOMER_ID, 
        RESTAURANT_ID ,
        ORDER_COST, 
        ORDER_ITEMS, 
        DISCOUNT, 
        WEEK_NUMBER
	FROM gl_eats_t WHERE WEEK_NUMBER = weeknum;
END;

-- Delivery Procedure
DROP PROCEDURE IF EXISTS gl_eats_del_p;

DELIMITER $$
CREATE PROCEDURE gl_eats_del_p(weeknum INTEGER)
BEGIN
	INSERT INTO gl_eats_del_t (
	DELIVERY_ID,
	DELIVERY_EMP_ID, 
        ORDER_ID, 
        DELIVERY_STATUS, 
        RESTAURANT_ID, 
        STAR_RATING,
        WEEK_NUMBER
	) 
    SELECT DISTINCT
	DELIVERY_ID,
	DELIVERY_EMP_ID, 
        ORDER_ID, 
        DELIVERY_STATUS, 
        RESTAURANT_ID, 
        STAR_RATING,
        WEEK_NUMBER
	FROM gl_eats_t WHERE WEEK_NUMBER = weeknum;
END;

/*-----------------------------------
		Actual Data Ingestion
-----------------------------------*/

-- Ingestion code (for weekly ingestion) --

TRUNCATE temp_t;

LOAD DATA LOCAL INFILE 'D:/Downloads/PGDSDMS/2 -- SQL and Databases/Week Two Materials/Class/Data_File/gl_eats_dump_week-4.csv' --  this location is changed to load every new week
INTO TABLE temp_t
FIELDS TERMINATED by ','
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Calling the procedures after each weekly ingestion --  

CALL gl_eats_p();
CALL gl_eats_cust_p();
CALL gl_eats_rest_p();
CALL gl_eats_ord_p(4); -- week number changed for every new week
CALL gl_eats_del_p(4); -- week number changed for every new week

/* =============================================
SECTION 05 — ANALYTICAL VIEWS (BUSINESS LAYER)
Objective: Provide stakeholder-ready datasets
============================================= */

-- View "gl_eats_cust_ord_v" -- 

DROP VIEW IF EXISTS gl_eats_cust_ord_v;

CREATE VIEW gl_eats_cust_ord_v AS
    SELECT 
        cust.CUSTOMER_ID,
        cust.CUSTOMER_NAME,
        cust.COUNTRY_CD,
        ord.ORDER_ID,
        ord.RESTAURANT_ID,
        ord.ORDER_COST,
        ord.ORDER_ITEMS,
        ord.DISCOUNT,
        ord.WEEK_NUMBER
    FROM gl_eats_cust_t cust
        JOIN gl_eats_ord_t ord
			ON cust.CUSTOMER_ID = ord.CUSTOMER_ID;
            
-- View "gl_eats_ord_rest_v" -- 

DROP VIEW IF EXISTS gl_eats_ord_rest_v;

CREATE VIEW gl_eats_ord_rest_v AS
    SELECT 
        rest.RESTAURANT_NAME,
        rest.CITY_NAME,
        rest.COUNTRY_CD,
        rest.CUISINES,
        rest.RESTAURANT_RATING,
        ord.ORDER_ID,
        ord.CUSTOMER_ID,
        ord.RESTAURANT_ID,
        ord.ORDER_COST,
        ord.ORDER_ITEMS,
        ord.DISCOUNT,
        ord.WEEK_NUMBER
    FROM gl_eats_rest_t rest
        JOIN gl_eats_ord_t ord
			ON rest.RESTAURANT_ID = ord.RESTAURANT_ID;

-- View "gl_eats_del_ord_v" -- 

DROP VIEW IF EXISTS gl_eats_del_ord_v;

CREATE VIEW gl_eats_del_ord_v AS
    SELECT 
        ord.ORDER_ID,
        ord.CUSTOMER_ID,
        ord.RESTAURANT_ID,
        ord.DELIVERY_EMP_ID,
        ord.ORDER_COST,
        ord.DISCOUNT,
        ord.WEEK_NUMBER,
        del.DELIVERY_ID,
        del.DELIVERY_STATUS,
        del.STAR_RATING
    FROM gl_eats_ord_t ord 
        INNER JOIN gl_eats_del_t del 
            ON ord.ORDER_ID = del.ORDER_ID;

-- View "gl_eats_del_rest_v" -- 

DROP VIEW IF EXISTS gl_eats_del_rest_v;

CREATE VIEW gl_eats_del_rest_v AS
    SELECT 
        rest.RESTAURANT_ID,
        rest.RESTAURANT_NAME,
        rest.CITY_NAME,
        rest.COUNTRY_CD,
        rest.RESTAURANT_RATING,
        del.DELIVERY_ID,
        del.DELIVERY_EMP_ID,
        del.ORDER_ID,
        del.DELIVERY_STATUS,
        del.STAR_RATING,
        del.WEEK_NUMBER
    FROM gl_eats_rest_t rest
        JOIN gl_eats_del_t del 
            ON rest.RESTAURANT_ID = del.RESTAURANT_ID;

-- View "gl_eats_ord_dubai_v" -- 

DROP VIEW IF EXISTS gl_eats_ord_dubai_v;

CREATE VIEW gl_eats_ord_dubai_v AS (
	SELECT 
		ord.*,
		rest.CITY_NAME 
	FROM gl_eats_ord_t ord 
		INNER JOIN  (SELECT DISTINCT 
				RESTAURANT_ID,
				CITY_NAME
				 FROM gl_eats_rest_t
				 WHERE CITY_NAME = 'Dubai') rest
		using (RESTAURANT_ID)
);

/* ==========================================================================
SECTION 06 — ANALYTICAL FUNCTIONS (BUSINESS LOGIC STANDARDIZATION)
Objective: Standardize revenue, delivery status, and pricing categorization
========================================================================== */

-- Calculate Revenue Function
DROP FUNCTION IF EXISTS calcRevenue;

DELIMITER $$  
CREATE FUNCTION calcRevenue(order_cost INT, discount INT, delivery_status INT)  
RETURNS INT  
DETERMINISTIC  
BEGIN  
    DECLARE revenue INT;
    IF delivery_status = 4 THEN  
        SET revenue = 0;  
    ELSEIF delivery_status = 1 OR delivery_status = 2 OR delivery_status = 3 THEN  
        SET revenue = order_cost - discount;  
    END IF;  
    RETURN (revenue);  
END;

-- Delivery Status Description Function
DROP FUNCTION IF EXISTS deliveryStatusDesc;

DELIMITER $$  
CREATE FUNCTION deliveryStatusDesc(delivery_status INT)  
RETURNS VARCHAR(20)  
DETERMINISTIC  
BEGIN  
    DECLARE delivery_desc VARCHAR(20);  
    IF delivery_status = 1 THEN  
        SET delivery_desc = 'On-time Delivery';  
    ELSEIF delivery_status = 2 THEN  
        SET delivery_desc = 'Late Delivery';  
    ELSEIF delivery_status = 3 THEN  
        SET delivery_desc = 'Early Delivery';  
    ELSEIF delivery_status = 4 THEN   
	SET delivery_desc = 'Order Cancelled';  
    END IF;  
    RETURN (delivery_desc);  
END;

-- Order Cost Bucket Function
DROP FUNCTION IF EXISTS orderCostBucket;

DELIMITER $$  
CREATE FUNCTION orderCostBucket(order_cost INT)  
RETURNS VARCHAR(20)  
DETERMINISTIC  
BEGIN  
    DECLARE cost_bucket VARCHAR(20);  
    IF order_cost <= 70 THEN  
        SET cost_bucket = 'Low';  
    ELSEIF (order_cost > 70 AND order_cost <= 120 ) THEN  
        SET cost_bucket = 'Medium';  
    ELSEIF order_cost > 120 THEN  
        SET cost_bucket = 'High';  
    END IF;  
    RETURN (cost_bucket);  
END;

/*------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
                                               Queries
--------------------------------------------------------------------------------------------------                                               
------------------------------------------------------------------------------------------------*/

/* ===================================================================
SECTION 07 — OPERATIONAL PERFORMANCE ANALYSIS
Objective: Identify weekly performance trends and operational health
=================================================================== */

-- [1] Weekly Order Volume Trend
select
	WEEK_NUMBER,
    count(ORDER_ID) as NO_OF_ORDERS
from gl_eats_cust_ord_v
group by 1;

-- [2] Weekly Average Delivery Rating
select
	WEEK_NUMBER,
	avg(STAR_RATING) as AVG_DELIVERY_RATING
from gl_eats_del_ord_v
group by 1;

-- [3] Weekly Average Order Cost
select
	WEEK_NUMBER,
    avg(ORDER_COST) as AVG_ORDER_COST
from gl_eats_cust_ord_v
group by 1;

-- [4] Weekly Average Order Quantity
select
	WEEK_NUMBER,
    avg(ORDER_ITEMS) as AVG_QUANTITY
FROM gl_eats_cust_ord_v
group by 1;

/* ======================================================================
SECTION 08 — REVENUE & GROWTH ANALYSIS
Objective: Evaluate revenue contribution and week-over-week performance
====================================================================== */

-- [5] Weekly Revenue
select
    WEEK_NUMBER,
    sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE
from gl_eats_del_ord_v
group by 1;

-- [6] Weekly % of Revenue
select
	WEEK_NUMBER,
    sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE,
    (sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS))/(select sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) from gl_eats_del_ord_v))*100 as '%_WEEKLY_REVENUE'
from gl_eats_del_ord_v
group by 1;

-- [7] Week-over-Week % of Revenue

with WkoWk as (
	select
		WEEK_NUMBER,
        sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE
	from gl_eats_del_ord_v
	group by 1
    )
select
	WEEK_NUMBER,
    REVENUE,
    lag(REVENUE) over(order by WEEK_NUMBER) as PREVIOUS_REVENUE,
    ((REVENUE-lag(REVENUE) over(order by WEEK_NUMBER)) / lag(REVENUE) over(order by WEEK_NUMBER) * 100) as 'WEEK_OVER_WEEK_REVENUE(%)'
from WkoWk;

/* ========================================================================
SECTION 09 — CUSTOMER & DELIVERY PERFORMANCE INSIGHTS
Objective: Identify distribution, churn risk, satifaction/dissatisfaction,
			and workforce insights
======================================================================== */

-- [8] Customer Cancellation Rate
select
	CUSTOMER_ID,
    count(*) as TOTAL_ORDERS,
    sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) as CANCELLATIONS,
    round(sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) / count(*), 2)*100 as CANCELLATION_RATE_PERCENT
from gl_eats_del_ord_v
group by 1
order by 4 desc;

-- [9] Average Customer Rating 
select
	CUSTOMER_ID,
    avg(STAR_RATING) as AVG_RATING
from gl_eats_del_ord_v
group by 1
order by 2 desc, 1;

-- [10] Dissatisfied Customers (average rating not >3)
select
	CUSTOMER_ID,
    avg(STAR_RATING) as AVG_RATING
from gl_eats_del_ord_v
group by 1
having AVG_RATING <= 3;

-- [11] Customer Rating vis-a-vis Discount Offered
with CustDiscRating as (
	select
		CUSTOMER_ID,
        ORDER_ID,
        case
			when DISCOUNT >= 15 and DISCOUNT <= 18 then 'LOW'
            when DISCOUNT >= 19 and DISCOUNT <= 21 then 'Medium'
            when DISCOUNT >=22 then 'HIGH'
		end as DISCOUNT_BUCKET,
        case
			when STAR_RATING = 1 or STAR_RATING = 2 then 'LOW'
            when STAR_RATING = 3 or STAR_RATING = 4 then 'MEDIUM'
            when STAR_RATING = 5 then 'HIGH'
		end as RATING_BUCKET
	from gl_eats_del_ord_v
    )
select
	DISCOUNT_BUCKET,
    RATING_BUCKET,
    count(RATING_BUCKET) as CNT,
    count(RATING_BUCKET) / (select count(*) from CustDiscRating where DISCOUNT_BUCKET = 'HIGH') as RATIO
from CustDiscRating
where DISCOUNT_BUCKET = 'HIGH'
group by 1,2;

-- [12] Customer Distribution across Countries
select
	COUNTRY_CD,
    count(CUSTOMER_ID) as NO_OF_CUSTOMERS
from gl_eats_ord_rest_v
group by 1
order by 2 desc;

-- [13] Weekly Employee Performance
select
	WEEK_NUMBER,
    DELIVERY_EMP_ID,
    count(DELIVERY_ID) as NO_OF_DELIVERIES
from gl_eats_del_ord_v
group by 1, 2
order by 2, 1 asc, 3 desc;

-- [14] Weekly Average Employee Rating
select
	WEEK_NUMBER,
    DELIVERY_EMP_ID,
    avg(STAR_RATING) as AVG_DELIVERY_RATING
from gl_eats_del_ord_v
group by 1, 2
order by 1 asc, 3 desc;

-- [15] Overworked Employees (>150 deliveries per week)
with Overworked as (
select
	DELIVERY_EMP_ID,
    WEEK_NUMBER,
    count(DELIVERY_ID) as NO_OF_DELIVERIES
from gl_eats_del_ord_v
group by 1,2
having NO_OF_DELIVERIES > 150
order by 3 desc
)
select count(DELIVERY_EMP_ID) as COUNT_OF_OVERWORKED_EMPLOYEES from Overworked;

-- [16] Locality-wise Employee Dstribution
select
	rest.LOCALITY,
	count(DELIVERY_EMP_ID) as NO_OF_EMPLOYEES
from gl_eats_rest_t rest
	inner join gl_eats_del_t using(RESTAURANT_ID)
group by 1
order by 2 desc;

/* =============================================================
SECTION 10 — RESTAURANT & GEOGRAPHIC PERFORMANCE ANALYSIS
Objective: Identify distribution, high-performing/risk vendors & locations
============================================================= */

-- [17] High Revenue Restaurants
select
	RESTAURANT_ID,
    RESTAURANT_NAME,
    sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE
from gl_eats_t
group by 1,2
order by 3 desc;

-- [18] Cancellation Rate vis-a-vis Restaurant Rating
with RestCancelRating as (
	select
		RESTAURANT_ID,
        RESTAURANT_NAME,
        DELIVERY_STATUS,
        case
			when RESTAURANT_RATING <= 3 then 'low'
            when RESTAURANT_RATING > 3 and RESTAURANT_RATING < 4 then 'medium'
            when RESTAURANT_RATING >= 4 and RESTAURANT_RATING <= 5 then 'high'
		end as RATING_BUCKET
	from gl_eats_del_rest_v
    )
select
	RATING_BUCKET,
    count(*) as ALL_ORDERS,
    sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) as CANCELLATIONS,
    sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) / count(*) as RATE_OF_CANCELLATION
from RestCancelRating
group by 1;

-- [19] City-wise Restaurant distribution
select
	CITY_NAME,
    count(RESTAURANT_ID) as NO_OF_RESTAURANTS
from gl_eats_rest_t
group by 1
order by 2 desc;

-- [20] High Revenue Countries
select
	COUNTRY_CD,
    sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE
from gl_eats_t
group by 1
order by 2 desc;

-- [21] High Revenue Cities
select
	CITY_NAME,
    sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE
from gl_eats_t
group by 1
order by 2 desc;

-- [22] Cities with High Cancellation Rate (not <10%)
select
	CITY_NAME,
    count(*) as ALL_ORDERS,
    sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) as CANCELLED_ORDERS,
    sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) / count(*) as RATE_OF_CANCELLATIONS
from gl_eats_del_rest_v
group by 1
having RATE_OF_CANCELLATIONS >= 0.1;

/* ================================================================
SECTION 11 — EXECUTIVE SUMMARY OUTPUT (BOARD-READY METRICS)
Objective: Provide leadership-level KPIs for revenue & operations
================================================================ */

-- [23] Total Revenue to Date
select sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as TOTAL_REVENUE
from gl_eats_del_ord_v;

-- [24] Total Active Customers
select count(distinct CUSTOMER_ID) as TOTAL_CUSTOMERS
from gl_eats_ord_rest_v;

-- [25] Overall Cancellation Rate
select
    round(sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) / count(*) * 100, 2) as CANCELLATION_RATE_PERCENT
from gl_eats_del_ord_v;

-- [26] Average Delivery Rating
select
	round(avg(STAR_RATING), 2) as AVG_DELIVERY_RATING
from gl_eats_del_ord_v;

-- [27] Highest Revenue Week
select
    WEEK_NUMBER,
    sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE
from gl_eats_del_ord_v
group by 1
order by 2 desc
limit 1;