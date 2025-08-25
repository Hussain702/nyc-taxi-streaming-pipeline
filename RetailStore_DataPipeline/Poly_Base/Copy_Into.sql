

COPY INTO dim_date
FROM 'https://datatricksexternal190.dfs.core.windows.net/reatilProject/Gold/dim_date.parquet'
WITH
(
    FILE TYPE='PARQUET'
    CREDENTIAL=(IDEMTITY='Managed Identity')
)



COPY INTO dim_weather
FROM 'https://datatricksexternal190.dfs.core.windows.net/reatilProject/Gold/dim_weather.parquet'
WITH
(
    FILE TYPE='PARQUET'
    CREDENTIAL=(IDEMTITY='Managed Identity')
)


COPY INTO dim_holidays
FROM 'https://datatricksexternal190.dfs.core.windows.net/reatilProject/Gold/dim_holidays.parquet'
WITH
(
    FILE TYPE='PARQUET'
    CREDENTIAL=(IDEMTITY='Managed Identity')
)


COPY INTO dim_features
FROM 'https://datatricksexternal190.dfs.core.windows.net/reatilProject/Gold/dim_features.parquet'
WITH
(
    FILE TYPE='PARQUET'
    CREDENTIAL=(IDEMTITY='Managed Identity')
)

COPY INTO dim_store
FROM 'https://datatricksexternal190.dfs.core.windows.net/reatilProject/Gold/dim_store.parquet'
WITH
(
    FILE TYPE='PARQUET'
    CREDENTIAL=(IDEMTITY='Managed Identity')
)

COPY INTO fact_sales
FROM 'https://datatricksexternal190.dfs.core.windows.net/reatilProject/Gold/fact_sales.parquet'
WITH
(
    FILE TYPE='PARQUET'
    CREDENTIAL=(IDEMTITY='Managed Identity')
)