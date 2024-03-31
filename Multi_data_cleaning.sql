/* UPDATING MULTINATIONAL DATABASE DATA*/

/* UPDATING ORDERS_TABLE*/

select * from orders_table

CREATE EXTENSION "uuid-ossp";

ALTER TABLE orders_table

	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN store_code TYPE VARCHAR(25),
	ALTER COLUMN product_code TYPE VARCHAR(25),
	ALTER COLUMN product_quantity TYPE SMALLINT,
	ALTER COLUMN card_number TYPE VARCHAR(19);

/* UPDATING DIM_USERS TABLE*/

SELECT * FROM dim_users

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE;


/* UPDATING DIM_STORE DATA */

SELECT * FROM dim_store_details

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(25),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN latitude TYPE FLOAT,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);


/* UPDATING DIM_PRODUCTS DATA*/

SELECT * FROM dim_products

ALTER TABLE dim_products
    ADD weight_class VARCHAR(255);
	
UPDATE dim_products
SET weight_class = (CASE
    WHEN weight < 2 THEN 'Light'
	WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
	ELSE 'Truck_Required'
END);

ALTER TABLE dim_products
    RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT,
	ALTER COLUMN weight TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(255),
	ALTER COLUMN product_CODE TYPE VARCHAR(25),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN  uuid TYPE UUID USING (uuid_generate_v4()),
	ALTER COLUMN still_available TYPE boolean
	USING CASE still_available WHEN 'Still_available' THEN true ELSE false END,
	ALTER COLUMN weight_class TYPE VARCHAR(14);
	
	

/* UPDATING DIM_DATE_TIMES DATA */

SELECT * FROM dim_products;

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(12),
	ALTER COLUMN  date_uuid TYPE UUID USING (uuid_generate_v4());


/* UPDATING DIM_CARD_DETAILS*/

SELECT * FROM dim_card_details

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN date_payment_confirmed TYPE DATE,
	ALTER COLUMN expiry_date TYPE VARCHAR(7);
UPDATE dim_card_details 
     SET expiry_date = substring(expiry_date, 1,7);

/* UPDATING TABLES WITH PRIMARY KEYS*/
ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);

/* UPDATING FOREIGN KEYS*/
SELECT * FROM orders_table

ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_orders_table FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_dates_times_table FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);
	
ALTER TABLE orders_table
    ADD CONSTRAINT fk_products_table FOREIGN KEY (product_code) REFERENCES dim_products (product_code);
	
ALTER TABLE orders_table
    ADD CONSTRAINT fk_stores_table FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);
	
ALTER TABLE orders_table
    ADD CONSTRAINT fk_users_table FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);


                        /*Query data*/

/* stores in countries*/

SELECT
    country_code AS country, COUNT(*) AS total_no_stores
FROM
     dim_store_details
GROUP BY
    country
ORDER BY
    total_no_stores DESC;


/* lOCALITY WITH HIGHER NUMBER OF STORES*/
SELECT
    locality, COUNT(*) AS total_no_stores
FROM
     dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC;
	
/*Monthly largest sales*/
	
	
	
SELECT 
     ROUND(SUM(product_price * product_quantity)::Decimal,2) as total_sales,
	 
month
     FROM dim_products p

INNER JOIN orders_table o
          ON p.product_code = o.product_code
INNER JOIN dim_date_times d
ON d.date_uuid = o.date_uuid
GROUP BY month	
ORDER BY total_sales DESC;
	
/* Count online sales*/

SELECT  
      COUNT(product_quantity) AS numbers_of_sales,
	  
      ROUND(sum(product_quantity)) AS product_quantity_count,

CASE store_type 
	WHEN  'Web Portal' THEN 'Web'
ELSE 'Offine'
    END locations
FROM dim_products p

    INNER JOIN orders_table o
ON p.product_code = o.product_code
  INNER JOIN dim_store_details s
ON s.store_code = o.store_code
GROUP BY locations
ORDER BY numbers_of_sales;


/*Store type percentage of sales*/

SELECT 
s.store_type, 
round(sum(product_price * product_quantity)::Decimal,2) as total_sales,
ROUND(SUM(o.product_quantity) * 100.0 / SUM(SUM(o.product_quantity)) OVER (), 2)
as "percentage_total(%)"

FROM
dim_products p
INNER JOIN 
orders_table o
ON
p.product_code = o.product_code
INNER JOIN
dim_store_details s
ON
s.store_code = o.store_code
GROUP BY store_type
ORDER BY total_sales DESC;


/* highest monthly cost of sales each year*/

SELECT 
     ROUND(sum(product_price * product_quantity)::Decimal,2) as total_sales,
	 year,month	 	 
     FROM dim_products p
INNER JOIN orders_table o
          ON p.product_code = o.product_code
INNER JOIN dim_date_times d
ON d.date_uuid = o.date_uuid
GROUP BY year,month
ORDER BY total_sales DESC;

/* Overall Staff headcount*/

SELECT sum(staff_numbers) AS total_staff_numbers,
country_code
FROM 
dim_store_details
GROUP BY 
country_code
ORDER BY 
total_staff_numbers DESC;



/* German store top selling*/

SELECT ROUND(SUM(product_price * product_quantity)::Decimal,2) AS total_sales,
store_type, country_code
FROM
dim_products p
INNER JOIN orders_table o
ON p.product_code = o.product_code
INNER JOIN dim_store_details s
ON s.store_code = o.store_code
WHERE s.country_code = 'DE' 
GROUP BY store_type, country_code
ORDER BY total_sales ASC;


/*YEAR SALE AVERAGE TIME*/

WITH events_date AS (
   SELECT 
	     CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) AS datetimes,
	    year 
    FROM 
	dim_date_times
	),
interval_times AS (

		       SELECT year,
	           datetimes,
	           LEAD(datetimes,1,datetimes)
        OVER (partition by year order by 
    datetimes ) as next_datetimes
	
    FROM events_date 
),
average_time AS (
	          
              SELECT
	                year,
	                AVG(next_datetimes - datetimes) 
	                 as time_taken 
	
	          FROM interval_times
	GROUP BY year
	ORDER BY time_taken desc
)
	SELECT
	      year,
	      to_char(time_taken::TIME,'"hours": HH, "min": MI , "seconds": SS, "milliseconds": MS') AS actual_time_taken
	FROM average_time;
	


























