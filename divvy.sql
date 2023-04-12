--CREATE NEW DATABASE AND USE;
DROP DATABASE IF EXISTS divvy_trip_data;
CREATE DATABASE divvy_trip_data;
GO

USE divvy_trip_data;
GO

--CREATE TABLE AND ADD DATA INTO IT IMPORTED FROM WEB;
DROP TABLE IF EXISTS divvy_trip_01;
CREATE TABLE divvy_trip_01
	(
	trip_id	int,
	start_time  datetime2,
	end_time datetime2,
	bike_id  int,
	trip_duration varchar(20),
	from_station_id int,
	from_station_name varchar(50),
	to_station_id int,
	to_station_name varchar(50),
	user_type varchar(50),
	gender varchar(50),
	birth_year date
	);
GO

BULK INSERT 
	divvy_trip_01
FROM 
	'C:\Users\phurb\Desktop\Projects\SQL_PROJECT\Divvy_trips_2019\Divvy_Trips_2019_Q1.CSV'
WITH 
	( 
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR='0x0a'
	)
GO

DROP TABLE IF EXISTS divvy_trip_02;
CREATE TABLE divvy_trip_02
	(
	trip_id	int,
	start_time  datetime2,
	end_time datetime2,
	bike_id  int,
	trip_duration varchar(20),
	from_station_id int,
	from_station_name varchar(50),
	to_station_id int,
	to_station_name varchar(50),
	user_type varchar(50),
	gender varchar(50),
	birth_year date
	);
GO

BULK INSERT 
	divvy_trip_02
FROM
	'C:\Users\phurb\Desktop\Projects\SQL_PROJECT\Divvy_trips_2019\Divvy_Trips_2019_Q2.CSV'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR= '0X0a'
	);
GO


DROP TABLE IF EXISTS divvy_trip_03
CREATE TABLE divvy_trip_03
	(
	trip_id	int,
	start_time  datetime2,
	end_time datetime2,
	bike_id  int,
	trip_duration varchar(20),
	from_station_id int,
	from_station_name varchar(50),
	to_station_id int,
	to_station_name varchar(50),
	user_type varchar(50),
	gender varchar(50),
	birth_year date
	);
GO

BULK INSERT divvy_trip_03
FROM 
	'C:\Users\phurb\Desktop\Projects\SQL_PROJECT\Divvy_trips_2019\Divvy_Trips_2019_Q3.CSV'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	ROWTERMINATOR='0x0a'
	)
GO

DROP TABLE IF EXISTS divvy_trip_04
CREATE TABLE divvy_trip_04
	(
	trip_id	int,
	start_time  datetime2,
	end_time datetime2,
	bike_id  int,
	trip_duration varchar(20),
	from_station_id int,
	from_station_name varchar(50),
	to_station_id int,
	to_station_name varchar(50),
	user_type varchar(50),
	gender varchar(50),
	birth_year date
	);
GO

BULK INSERT divvy_trip_04
FROM 
	'C:\Users\phurb\Desktop\Projects\SQL_PROJECT\Divvy_trips_2019\Divvy_Trips_2019_Q4.CSV'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	ROWTERMINATOR='0x0a'
	)
GO

--ALL REQUIRED DATA OF 2019 IS IMPORTED--

--STEP:1-CLEAN DATASET, BUT BEFORE CLEANING WE NEED TO PUT ALL DATA INTO ONE TABLE TO WORKOUT EASILY.
DROP TABLE IF EXISTS divvy_trip
CREATE TABLE divvy_trip
	(
	trip_id	int,
	start_time  datetime2,
	end_time datetime2,
	bike_id  int,
	trip_duration varchar(20),
	from_station_id int,
	from_station_name varchar(50),
	to_station_id int,
	to_station_name varchar(50),
	user_type varchar(50),
	gender varchar(50),
	birth_year date
	);


INSERT INTO divvy_trip
	SELECT *
	FROM divvy_trip_01;

INSERT INTO divvy_trip
	SELECT *
	FROM divvy_trip_02;

INSERT INTO divvy_trip
	SELECT *
	FROM divvy_trip_03;

INSERT INTO divvy_trip
	SELECT *
	FROM divvy_trip_04;
GO

--CHECK DATASET 

SELECT TOP 100*
FROM divvy_trip;
GO
-----------------------------------------------DATA CLEANING----------------------------------------
--CONVERT TRIP DURATION FROM VARCHAR TO FLOAT.(REMOVED COMMA FROM AND CONVERTED TO FLOAT)
UPDATE divvy_trip
SET 
	trip_duration= CONVERT(FLOAT,REPLACE(trip_duration,',',''));
GO

--CHECK FOR DUPLICATES
SELECT COUNT(*) as duplicates,trip_id
FROM divvy_trip
GROUP BY trip_id
HAVING COUNT(*)>1
GO

--Finding NULLS in the table--
SELECT COUNT(*) AS 'Number of null values'
FROM divvy_trip
WHERE
	start_time IS NULL OR
	end_time IS NULL OR
	bike_id IS NULL OR
	trip_duration IS NULL OR
	from_station_id IS NULL OR
	from_station_name IS NULL OR
	to_station_id IS NULL OR
	to_station_name IS NULL OR
	user_type IS NULL AND
	gender IS NULL OR
	birth_year IS NULL
GO
--Removed all nulls values in gender and birth_year and created divvy_trip_new and insered into it.
DROP TABLE IF EXISTS divvy_trip_new
SELECT * 
	INTO divvy_trip_new
FROM 
	divvy_trip 
WHERE 
	gender is not null and 
	birth_year is not null
GO
--Created new table age and added values to it.
ALTER TABLE divvy_trip_new
ADD age int null
GO

UPDATE divvy_trip_new
SET age=(DATEDIFF(year,birth_year,GETDATE()))
GO

--Check dataset 'divvy_trip_new'
SELECT TOP 100 * 
FROM divvy_trip_new
GO



--PROCESSING OF DATA.---------------------------------------------------------------------------------------------------------------

SELECT DISTINCT 
	TOP 20 from_station_name,from_station_id,
				count(from_station_name)/360 AS 'no_of_visits per day'
FROM 
	divvy_trip_new
GROUP BY
	from_station_name,from_station_id
order by 
	count(from_station_name)/360 DESC
GO
--highest average visit per day was 145 from Canal St & Adams St.

--USERS BASED ON GENDER
SELECT COUNT(gender) AS 'COUNT',gender
FROM divvy_trip_new
GROUP BY gender
ORDER BY count(gender) DESC
GO
----male users are more compared to female--
DROP TABLE IF EXISTS divvy_trip_customer_analysis
SELECT 
	SUM(CONVERT(float,subscriber.trip_duration)) as subscriber_trip_duration,
	SUM(CONVERT(float,customer.trip_duration)) as customer_trip_duration,
	subscriber.from_station_id,
	subscriber.from_station_name,
	subscriber.to_station_id,
	subscriber.to_station_name
INTO 
	divvy_trip_customer_analysis
FROM
	(
	SELECT 
		trip_duration,
		from_station_id,
		from_station_name,
		to_station_id,
		to_station_name,
		user_type
	FROM 
		divvy_trip_new
	WHERE user_type = 'Subscriber'
	) subscriber
JOIN
	(
	SELECT 
		trip_duration,
		from_station_id,
		from_station_name,
		to_station_id,
		to_station_name,
		user_type
	FROM 
		divvy_trip_new
	WHERE user_type = 'Customer'
	) customer
	ON 
	customer.from_station_id = subscriber.from_station_id
GROUP BY 
	subscriber.from_station_id,
	subscriber.from_station_name,
	subscriber.to_station_id,
	subscriber.to_station_name
ORDER BY subscriber.from_station_name
GO
--CUSTOMER AND SUBSCRIBER TOTAL TRIP DURATION
SELECT TOP 10
	subscriber_trip_duration,
	customer_trip_duration,
	from_station_id,
	from_station_name,
	to_station_id,
	to_station_name
FROM divvy_trip_customer_analysis
ORDER BY subscriber_trip_duration DESC,customer_trip_duration DESC
GO 

----MOST OF THE TRIP WITH HIGHER DURATION ARE FROM CUSTOMER THAN SUBSCRIBER---

--Finding highest no. Trips with respect to station.
DROP TABLE IF EXISTS customer_subscriber_trip_analysis;
WITH customer_no_trips
AS 
	(SELECT COUNT(trip_id) AS ct_no_trips,
			from_station_id,
			from_station_name,
			to_station_id,
			to_station_name
	FROM divvy_trip_new
	WHERE user_type = 'Customer'
	GROUP BY
			from_station_id,
			from_station_name,
			to_station_id,
			to_station_name
	),
	subscriber_no_trips
AS
	(SELECT COUNT(trip_id) AS sb_no_trips,
			from_station_id,
			from_station_name,
			to_station_id,
			to_station_name
	FROM divvy_trip_new
	WHERE user_type = 'Subscriber'
	GROUP BY
			from_station_id,
			from_station_name,
			to_station_id,
			to_station_name
	)
	SELECT  ct.ct_no_trips,
			ct.from_station_name as customer_from_station,
			ct.to_station_name as customer_to_station,
			st.sb_no_trips,
			st.from_station_name as subscriber_from_station,
			st.to_station_name as subscriber_to_station
	INTO 
		customer_subscriber_trip_analysis
	FROM subscriber_no_trips as st 
		FULL JOIN
		customer_no_trips as ct
		ON
		st.from_station_name=ct.from_station_name 
		AND
		st.to_station_name = ct.to_station_name
ORDER BY st.from_station_name
GO
 --TOP 100 station wrt to customer ride behaviour.
SELECT TOP 100
	customer_from_station,
	customer_to_station,
	ct_no_trips,
	DENSE_RANK() OVER (ORDER BY ct_no_trips DESC) station_rank_wrt_trips
FROM
	customer_subscriber_trip_analysis
WHERE ct_no_trips is not null and
		ct_no_trips > 100
GO

--LEAST ACTIVE STATION WRT CUSTOMER RIDE BEHAVIOUR.
SELECT
	customer_from_station,
	customer_to_station,
	ct_no_trips,
	DENSE_RANK() OVER (ORDER BY ct_no_trips DESC) station_rank_wrt_trips
FROM
	customer_subscriber_trip_analysis
WHERE ct_no_trips is not null and
		ct_no_trips <100
GO

--TOP 100 most active station wrt subscriber ride behaviour.
SELECT TOP 100
	subscriber_from_station,
	subscriber_to_station,
	sb_no_trips,
	DENSE_RANK() OVER(ORDER BY sb_no_trips DESC) station_rank_wrt_trips
FROM
	customer_subscriber_trip_analysis
WHERE sb_no_trips is not null and
		sb_no_trips > 100
GO

--LEAST ACTIVE STATION WRT CUSTOMER RIDE BEHAVIOUR
SELECT  
	subscriber_from_station,
	subscriber_to_station,
	sb_no_trips,
	DENSE_RANK() OVER(ORDER BY sb_no_trips DESC) station_rank_wrt_trips
FROM
	customer_subscriber_trip_analysis
WHERE sb_no_trips is not null and
		sb_no_trips < 100
GO

--TOTAL Station where the number of ride taken by customer is very low.
SELECT DISTINCT
	customer_to_station
FROM
	customer_subscriber_trip_analysis
WHERE
	sb_no_trips<100
UNION
SELECT DISTINCT 
	customer_from_station
FROM
	customer_subscriber_trip_analysis
WHERE sb_no_trips<100
GO

--TOTAL Station where the number of ride taken by subscriber is very low.
SELECT DISTINCT
	subscriber_from_station
FROM
	customer_subscriber_trip_analysis
WHERE
	sb_no_trips < 100
UNION
SELECT DISTINCT
	subscriber_to_station
FROM
	customer_subscriber_trip_analysis
WHERE
	sb_no_trips < 100
GO
---FROM this we were able to find out the highest and lowest trips made by custumer and subscriber based on trip count.---


