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

ALTER COLUMN uuid_column TYPE UUID USING column_name::uuid
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

UPDATE dim_products
SET still_available ='yes' WHERE still_available='Available',
SET still_available ='no' WHERE still_available='Removed';

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT,
	ALTER COLUMN weight TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(255),
	ALTER COLUMN product_CODE TYPE VARCHAR(25),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN  uuid TYPE UUID USING (uuid_generate_v4()),
	ALTER COLUMN still_available TYPE boolean
	USING CASE still_available WHEN 'yes' THEN true ELSE false END,
	ALTER COLUMN weight_class TYPE VARCHAR(14);
	

/* UPDATING DIM_DATE_TIMES DATA */

SELECT * FROM dim_date_times

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

/* sUPDATING FOREIGN KEYS*/
SELECT * FROM orders_table

ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_orders_table FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);
	
	
	
	
	