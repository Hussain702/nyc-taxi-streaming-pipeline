
--CREATING DIMENSIONAL TABLES
DROP TABLE dim_city
CREATE TABLE dim_city  
(  
    city_id INT IDENTITY(1,1) NOT NULL,  
    city_name VARCHAR(100)  
   
)  
WITH  
(  
    DISTRIBUTION = REPLICATE  
);

DROP TABLE dim_date
CREATE TABLE dim_date (
    date_id INT IDENTITY(1,1) NOT NULL,       
    full_date DATE,                  
    year INT,
    month INT,
    day INT,
    day_of_week VARCHAR(10),        
    week_of_year INT
)
WITH (
    DISTRIBUTION = REPLICATE
);

--CREATING FACT TABLES
CREATE TABLE fact_energy_usage
(
    d_id INT,
    city_id INT,
    usage_kw INT,
    ff_kw INT,
    dr_ac_kw INT,
    br_ac_kw INT,
    kitchen_kw INT,
    water_pump_kw INT,
    geyser_kw INT,
    washing_machine_kw INT,
    fridge_kw INT,
    ups_kw INT,
    temperature FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    pressure FLOAT
)
WITH
(
    DISTRIBUTION = HASH(city_id),
    CLUSTERED COLUMNSTORE INDEX
);
--CREATING STAGING TABLE FOR TEMPORARY STORAGE
DROP TABLE staging_energy_usage
CREATE TABLE staging_energy_usage
(
    ddate DATETIME,
    city VARCHAR(100),
    usage_kw FLOAT,
    ff_kw FLOAT,
    dr_ac_kw FLOAT,
    br_ac_kw FLOAT,
    kitchen_kw FLOAT,
    water_pump_kw FLOAT,
    geyser_kw FLOAT,
    washing_machine_kw FLOAT,
    fridge_kw FLOAT,
    ups_kw FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    pressure FLOAT
)
WITH
(
    DISTRIBUTION = HASH(city),
    CLUSTERED COLUMNSTORE INDEX
);
--POPULATING STAR SCHEMA
INSERT INTO dim_city (city_name)
SELECT DISTINCT city FROM staging_energy_usage
WHERE city NOT IN (
    SELECT city_name FROM dim_city
)

SELECT
*
FROM dim_city


--POPULATING DIM_DATE TABLE FROM STAGING TABLE
INSERT INTO dim_date (
    full_date,                  
    year,
    month,
    day,
    day_of_week,        
    week_of_year
)
SELECT DISTINCT
    CAST(d_id AS DATE) AS full_date,
    DATEPART(YEAR, d_id) AS year,
    DATEPART(MONTH, d_id) AS month,
    DATEPART(DAY, d_id) AS day,
    DATENAME(WEEKDAY, d_id) AS day_of_week,
    DATEPART(WEEK, d_id) AS week_of_year
FROM staging_energy_usage
WHERE CAST(d_id AS DATE) NOT IN (
    SELECT full_date FROM dim_date
);

SELECT * FROM dim_date