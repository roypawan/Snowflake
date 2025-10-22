grant imported privileges
on database SNOWFLAKE_SAMPLE_DATA
to role SYSADMIN;

select distinct c_mktsegment
from snowflake_sample_data.tpch_sf1.customer;

--Find out which Market Segments have the most customers
select c_mktsegment, count(*)
from snowflake_sample_data.tpch_sf1.customer
group by c_mktsegment
order by count(*);

-- Nations Table
select n_nationkey, n_name, n_regionkey
from snowflake_sample_data.tpch_sf1.nation;


-- Regions Table
select r_regionkey, r_name
from snowflake_sample_data.tpch_sf1.region;


-- Join the Tables and Sort
select r_name as region, n_name as nation
from snowflake_sample_data.tpch_sf1.nation
join snowflake_sample_data.tpch_sf1.region
on n_regionkey = r_regionkey
order by r_name, n_name asc;

--Group and Count Rows Per Region
select r_name as region, count(n_name) as num_countries
from snowflake_sample_data.tpch_sf1.nation
join snowflake_sample_data.tpch_sf1.region
on n_regionkey = r_regionkey
group by r_name;


use role SYSADMIN;

show databases;
use database UTIL_DB;

use role SYSADMIN;

create database INTL_DB;

use schema INTL_DB.PUBLIC;

create warehouse INTL_WH 
with 
warehouse_size = 'XSMALL' 
warehouse_type = 'STANDARD' 
auto_suspend = 600 --600 seconds/10 mins
auto_resume = TRUE;

use warehouse INTL_WH;

create or replace table intl_db.public.INT_STDS_ORG_3166 
(iso_country_name varchar(100), 
 country_name_official varchar(200), 
 sovreignty varchar(40), 
 alpha_code_2digit varchar(2), 
 alpha_code_3digit varchar(3), 
 numeric_country_code integer,
 iso_subdivision varchar(15), 
 internet_domain_code varchar(10)
);

create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '|' --pipe or vertical bar
  record_delimiter = '\r' --carriage return
  skip_header = 1  --1 header row
  field_optionally_enclosed_by = '\042'  --double quotes
  trim_space = FALSE;

show stages in account;

create stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';

list @util_db.public.AWS_S3_BUCKET;


copy into intl_db.public.INT_STDS_ORG_3166 
from @util_db.public.AWS_S3_BUCKET
files = ( 'ISO_Countries_UTF8_pipe.csv')
file_format = ( format_name='util_db.public.PIPE_DBLQUOTE_HEADER_CR' );


select count(*) as found, '249' as expected 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166; 

select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';


select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;



create view intl_db.public.NATIONS_SAMPLE_PLUS_ISO 
( iso_country_name
  ,country_name_official
  ,alpha_code_2digit
  ,region) AS
 select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey
;

select *
from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;


create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';

  create file format util_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- the n represents a Line Feed character
  skip_header = 1 
;

list @UTIL_DB.PUBLIC.AWS_S3_BUCKET/;

copy into intl_db.public.CURRENCIES 
from @util_db.public.AWS_S3_BUCKET
files = ( 'currencies.csv')
file_format=(format_name='util_db.public.CSV_COMMA_LF_HEADER');



copy into intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE
from @util_db.public.AWS_S3_BUCKET
files = ( 'country_code_to_currency_code.csv')
file_format=(format_name='util_db.public.CSV_COMMA_LF_HEADER');


SELECT COUNTRY_CHAR_CODE AS CTY_CODE,CURRENCY_CHAR_CODE AS CUR_CODE FROM intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE;

CREATE VIEW SIMPLE_CURRENCY AS
SELECT COUNTRY_CHAR_CODE AS CTY_CODE,CURRENCY_CHAR_CODE AS CUR_CODE FROM intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE;;

SELECT COUNT(*) FROM SIMPLE_CURRENCY;


SELECT CURRENT_ORGANIZATION_NAME() || '.' || CURRENT_ACCOUNT_NAME();

USE ROLE ORGADMIN;

SELECT SYSTEM$ENABLE_GLOBAL_DATA_SHARING_FOR_ACCOUNT('KK64116');
SELECT SYSTEM$IS_GLOBAL_DATA_SHARING_ENABLED_FOR_ACCOUNT('KK64116');


**************************************************************************************************************

alter view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
set secure; 

alter view intl_db.public.SIMPLE_CURRENCY
set secure; 

select * from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;

select * from intl_db.public.SIMPLE_CURRENCY;


select get_ddl('view','intl_db.public.SIMPLE_CURRENCY');


**************************************************************************************************************
SELECT CURRENT_ROLE();

SHOW DATABASES LIKE 'SNOWFLAKE%';

-- show grants on a database (may help see ownership/privileges)
SHOW GRANTS ON DATABASE SNOWFLAKE_SAMPLE_DATA;
**************************************************************************************************************

use role accountadmin;

SELECT DISTINCT SERVICE_TYPE
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
ORDER BY SERVICE_TYPE;
-- output:
--         AUTO_CLUSTERING
--     SERVERLESS_TASK
--     WAREHOUSE_METERING

SELECT COUNT(DISTINCT SERVICE_TYPE) AS NUM_SERVICE_TYPES
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY;

SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE = 'WAREHOUSE_METERING';

SELECT SERVICE_TYPE, SUM(CREDITS_USED) AS TOTAL_CREDITS FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY GROUP BY SERVICE_TYPE ORDER BY SERVICE_TYPE, TOTAL_CREDITS DESC;

--Check distinct USAGE_TYPE only:
SELECT DISTINCT SERVICE_TYPE FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY;
-- output::
--     WAREHOUSE_METERING
--     SERVERLESS_TASK
--     AUTO_CLUSTERING

SELECT DISTINCT SERVICE_TYPE FROM SNOWFLAKE.ORGANIZATION_USAGE.METERING_DAILY_HISTORY

//**************************************************************************************************************

--
SELECT GET_DDL('FILE FORMAT', 'UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER');

SHOW FILE FORMATS IN SCHEMA UTIL_DB.PUBLIC;

