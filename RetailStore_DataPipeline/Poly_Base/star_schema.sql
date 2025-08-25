------------------CRETAING STAR SCHEMA-------------------------

CREATE TABLE dim_date (
    date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    dayofweek VARCHAR(10),      
    dayofweeknum INT,           
    weekofyear INT,
    quarter INT
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,  
    CLUSTERED COLUMNSTORE INDEX 
);

-------------------------CRETING STORE DIMENSION TABLE-----------------------


CREATE TABLE dim_store
(
   store_id INT,
   store_type VARCHAR(50),
   store_size INT
)
WITH 
(
    DISTRIBUTION=ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)

-----------------------CREATING FEATURES DIMESNSION TABLE-------------------------

CREATE TABLE  _economy
(
    [date] DATE NULL,
    [fuel_price] FLOAT NULL,
    [cpi] FLOAT NULL,
    [unemployment_rate] FLOAT NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);

-----------------------CREATING weather DIMESNSION TABLE-------------------------


CREATE TABLE dim_weather (
    date DATE NULL,
    time VARCHAR(20) NULL,
    temperature FLOAT NULL,
    precipitation FLOAT NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);


-----------------------CREATING HOLIDAYS DIMESNSION TABLE-------------------------
CREATE TABLE dim_holidays(
    date DATE NULL,
    holiday_type VARCHAR(50) NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);


-----------------------CREATING SALES FACT TABLE-------------------------

CREATE TABLE fact_sales (
    sales_id INT NULL,
    store_id INT NULL,
    dept_id INT NULL,
    weekly_sales FLOAT NULL,
    date DATE NULL
)
WITH
(
    DISTRIBUTION = HASH(sales_id),
    CLUSTERED COLUMNSTORE INDEX
);



