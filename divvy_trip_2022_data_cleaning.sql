------------------------------
--CREATE DATABASE FOR PROJECT
------------------------------
CREATE 
	DATABASE divvy_trip_project;
GO

--SELECT CREATED DATABASE FOR PROJECT
USE divvy_trip_project;
GO

--Create new table to insert downloaded data of year 2022

DROP TABLE IF EXISTS divvy_trip_raw_2022;
GO

CREATE TABLE divvy_trip_raw_2022
	(
	ride_id varchar(50),
	ridable_type varchar(50),
	started_at datetime2,
	ended_at datetime2,
	start_station_name varchar(100),
	start_station_id varchar(50),
	end_station_name varchar(100),
	end_station_id varchar(50),
	start_lat float,
	start_lng float,
	end_lat float,
	end_lng float,
	user_type varchar(50)
	);
GO

--Now bulk insert data into table from file location

--INSERT DATA OF YEAR=2022,MONTH =01
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202201-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA OF YEAR=2022,MONTH=02 
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202202-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA OF YEAR=2022,MONTH=03
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202203-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA OF YEAR=2022,MONTH=04
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202204-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA OF YEAR=2022,MONTH=05
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202205-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA OF YEAR-2022,MONTH-06
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202206-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA 0F YEAR-2022,MONTH-07
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202207-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA OF YEAR-2022,MONTH-08
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202208-divvy-tripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO

--INSERT DATA OF YEAR-2022,MONTH-09
BULK INSERT divvy_trip_raw_2022
FROM 'C:\Users\Alien\Desktop\Project_1\raw_data_divytrip\2022\202209-divvy-publictripdata.csv'
WITH
	(
	FORMAT = 'csv',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '\n'
	);
GO
-----------------------------------------------------------------------------
--CHECK FOR DUPLICATES IN DATASET
WITH duplicate_vl
AS
	(
	SELECT 
		*,
		COUNT(*) AS duplicates
	FROM 
		divvy_trip_raw_2022
	GROUP BY 
		ride_id,
		ridable_type,
		start_lat,
		start_lng,
		start_station_id,
		start_station_name,
		end_lat,
		end_lng,
		end_station_id,
		end_station_name,
		started_at,
		ended_at,user_type
	)
SELECT 
	COUNT(*) as duplicate
FROM 
	duplicate_vl
WHERE
	duplicates > 1
GO
-------------------------------------------------------------------------
--CHECK FOR NULL VALUES
SELECT COUNT(*) as null_value
FROM 
	divvy_trip_raw_2022
WHERE 
	start_station_name is null
or
	start_station_id is null
and
	end_station_name is null
or
	end_station_id is null;
GO
---------------------------------------------------------------------------
-----------------------------------------------------------
/*SOLVE FOR NULLS IN STATION NAME AND ID COLUMN*/
-----------------------------------------------------------
--STEP 1:Find distinct start_lat and start_lng with null value in start and end station name and id
DROP VIEW IF EXISTS lat_lng
GO

CREATE VIEW 
	lat_lng
AS
SELECT *
FROM
(
SELECT 
	end_lat lat,
	end_lng lng
FROM 
	divvy_trip_raw_2022
WHERE 
	end_station_name is null
AND
	end_station_id is null
GROUP BY 
	end_lat,
	end_lng
UNION
SELECT 
	start_lat,
	start_lng
FROM 
	divvy_trip_raw_2022
WHERE
	start_station_name is null
AND
	start_station_id is null
GROUP BY 
	start_lat,
	start_lng
) as union_lat_lng
GO
---------------------------------------------------------------------------------
--Find station name for lat_lng
---------------------------------------------------------------------------------
DROP VIEW IF EXISTS station_name_CTE
GO

CREATE VIEW
	station_name_CTE
AS
	SELECT DISTINCT *
	FROM
		(SELECT DISTINCT
			start_station_name station_name,
			start_station_id station_id,
			lat,
			lng
		FROM
			lat_lng v1
		LEFT JOIN
			divvy_trip_raw_2022 v2
		ON
			v1.lat = v2.start_lat
			and
			v1.lng = v2.start_lng
		WHERE 
			v2.start_station_name is not null
UNION
		SELECT DISTINCT
			end_station_name,
			end_station_id,
			lat,
			lng 
		FROM
			lat_lng v1
		LEFT JOIN
			divvy_trip_raw_2022 v2
		ON 
			v1.lat = v2.end_lat
			and
			v1.lng = v2.end_lng
		WHERE 
			v2.end_station_name is not null
		) as lat_lng_t
GO
-------------------------------------------------------------------------------------------------------
/*Created a table with filled null start_station name and id*/
-------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS divvy_trip_data_semicleaned_2022
GO

WITH
start_station_name_CTE
AS
(
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY lat,lng ORDER BY lat,lng) grp
	FROM 
		station_name_CTE
	WHERE 
		station_name not like 'Public Rack%'--removed station with having public rack on its station name
)
SELECT 
		ride_id,
		ridable_type,
		started_at,
		ended_at,
		a2.station_name start_station_name,
		a2.station_id start_station_id,
		end_station_name,
		end_station_id,
		a1.start_lat,
		a1.start_lng,
		end_lat,
		end_lng,
		user_type
	INTO 
		divvy_trip_data_semicleaned_2022
	FROM 
		divvy_trip_raw_2022 a1
	INNER JOIN
		(
		SELECT *
		FROM 
			start_station_name_CTE
		WHERE 
			grp < 2
		) a2
	ON 
		a1.start_lat =a2.lat
	AND
		a1.start_lng = a2.lng
	WHERE 
		a1.start_station_name is null
	ORDER BY 
		start_lat,start_lng	
GO
-------------------------------------------------------------
--Filling end station name and id having nulls
-------------------------------------------------------------
--Joined newly created table(without null start_station name and id) with raw data table having not null values 
DROP VIEW IF EXISTS new_divvy_trip_2022
GO

CREATE VIEW
	new_divvy_trip_2022
AS
	SELECT *
	FROM
		divvy_trip_data_semicleaned_2022
	UNION
	SELECT *
	FROM
		divvy_trip_raw_2022
	WHERE start_station_name is not null
GO
---------------------------------------------------------------------
/*Created table by filling null end_station_name and station_id by 
joining with table having no null start_station_name and id*/
---------------------------------------------------------------------
DROP TABLE IF EXISTS divvy_trip_data_endcleaned_2022
GO

WITH
end_station_name_CTE
AS
(
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY lat,lng ORDER BY lat,lng) grp
	FROM 
		station_name_CTE
	WHERE 
		station_name not like 'Public Rack%'--removed Station mentioning public rack on its stationn name 
)
SELECT 
		ride_id,
		ridable_type,
		started_at,
		ended_at,
		start_station_name,
		start_station_id,
		a2.station_name end_station_name,
		a2.station_id end_station_id,
		a1.start_lat,
		a1.start_lng,
		end_lat,
		end_lng,
		user_type
	INTO 
		divvy_trip_data_endcleaned_2022
	FROM 
		new_divvy_trip_2022 a1
	INNER JOIN
		(
		SELECT *
		FROM 
			end_station_name_CTE
		WHERE 
			grp < 2
		) a2
	ON 
		a1.end_lat =a2.lat
	AND
		a1.end_lng = a2.lng
	WHERE 
		a1.end_station_name is null
	ORDER BY 
		end_lat,end_lng	
GO

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
/*CLEAN DATA IS SAVED WITH TABLE NAME divvy_trip_clean_data_2022*/
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS divvy_trip_data_clean_2022
GO

SELECT *
INTO divvy_trip_data_clean_2022
FROM
	(
	SELECT *
	FROM
		divvy_trip_raw_2022
	WHERE
		start_station_name is not null
		and
		end_station_name is not null
UNION 
	SELECT *
	FROM divvy_trip_data_endcleaned_2022
	) as dclean
GO

---CHECK FOR DUPLICATES AND NULLS
SELECT *
FROM 
	divvy_trip_data_clean_2022
WHERE 
	start_station_name is null
	or
	start_station_id is null
	or
	end_station_id is null
	or
	end_station_name is null
GO
--No nulls were found
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
--DELETE all rows with negative ride_len

SELECT *,
	DATEDIFF(MI,started_at,ended_at) trip_len
FROM
	divvy_trip_data_clean_2022
WHERE 
	DATEDIFF(MI,started_at,ended_at)<0
ORDER BY trip_len
--we found that 31 rows have -ve trip duration and one had -10353 
--but due to lack of information i have decided to remove all 31 rows
DELETE 
	FROM divvy_trip_data_clean_2022
WHERE 
	DATEDIFF(MI,started_at,ended_at) < 0
GO
---------------------------------------------------------------------------
---------------------------------------------------------------------------  
--LASTLY FORMAT DATE AS 'DD-MM-YY HH-MM-SS'
	
WITH check_CTE
as
(
SELECT
	IIF(TRY_CONVERT(DATETIME2,started_at) is null,0,1) check_1,
	IIF(TRY_CONVERT(DATETIME2,ended_at) IS NULL,0,1) check_2
FROM
	divvy_trip_data_clean_2022
)
SELECT 
	* 
FROM
	check_CTE 
WHERE
	check_1 < 1
	or
	check_2 < 1
GO

--FOUND ALL THE DATE ARE OKAY

-------------------------------------------------------------END------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------


