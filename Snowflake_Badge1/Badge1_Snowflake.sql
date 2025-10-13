use role SYSADMIN;
CREATE DATABASE IF NOT EXISTS GARDEN_PLANTS;
Drop schema if exists GARDEN_PLANTS.PUBLIC;
CREATE SCHEMA IF NOT EXISTS GARDEN_PLANTS.VEGGIES;
CREATE SCHEMA IF NOT EXISTS GARDEN_PLANTS.FRUITS ;
CREATE SCHEMA IF NOT EXISTS GARDEN_PLANTS.FLOWERS;

show databases;

 SHOW SCHEMAS;

create or replace table ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   ); 

   show tables in account;

insert into root_depth 
values
(
    2,
    'M',
    'Medium',
    'cm',
    45,
    60
),
(
 3,
    'D',
    'Deep',
    'cm',
    60,
    90
);

SELECT * FROM ROOT_DEPTH;
--***********************************************************************************************
use role accountadmin;

create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');


create or replace external function util_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
; 

use role accountadmin;
use database util_db; 
use schema public; 

select grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 

select * 
from garden_plants.information_schema.schemata;

SELECT * 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES'); 

select count(*) as schemas_found, '3' as schemas_expected 
from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES'); 

--You can run this code, or you can use the drop lists in your worksheet to get the context settings right.
use database UTIL_DB;
use schema PUBLIC;
use role ACCOUNTADMIN;

--Do NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT
 'DWW01' as step
 ,( select count(*)  
   from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA 
   where schema_name in ('FLOWERS','VEGGIES','FRUITS')) as actual
  ,3 as expected
  ,'Created 3 Garden Plant schemas' as description
); 


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW02' as step 
 ,( select count(*) 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA 
   where schema_name = 'PUBLIC') as actual 
 , 0 as expected 
 ,'Deleted PUBLIC schema.' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW04' as step
 ,( select count(*) as SCHEMAS_FOUND 
   from UTIL_DB.INFORMATION_SCHEMA.SCHEMATA) as actual
 , 2 as expected
 , 'UTIL_DB Schemas' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
 SELECT 'DWW05' as step 
,( select row_count 
  from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
  where table_name = 'ROOT_DEPTH') as actual 
, 3 as expected 
,'ROOT_DEPTH row count' as description
);  

create table garden_plants.veggies.vegetable_details
(
plant_name varchar(25)
, root_depth_code varchar(1)    
);

SELECT
*
FROM
  "GARDEN_PLANTS"."VEGGIES"."VEGETABLE_DETAILS" where plant_name='Spinach';

use role accountadmin;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW06' as step
 ,( select count(*) 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
   where table_name = 'VEGETABLE_DETAILS') as actual
 , 1 as expected
 ,'VEGETABLE_DETAILS Table' as description
); 

use role sysadmin;


SELECT
*
FROM
  "GARDEN_PLANTS"."VEGGIES"."VEGETABLE_DETAILS" where plant_name='Spinach';

delete from VEGETABLE_DETAILS where plant_name='Spinach' and ROOT_DEPTH_CODE='D'


use database UTIL_DB;
use schema PUBLIC;
use role ACCOUNTADMIN;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW07' as step
 ,( select row_count 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
   where table_name = 'VEGETABLE_DETAILS') as actual
 , 41 as expected
 , 'VEG_DETAILS row count' as description
); 

USE ROLE SYSADMIN;

create or replace TABLE GARDEN_PLANTS.FLOWERS.FLOWER_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

use database UTIL_DB;
use schema PUBLIC;
use role ACCOUNTADMIN;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
   SELECT 'DWW08' as step 
   ,( select iff(count(*)=0, 0, count(*)/count(*))
      from table(information_schema.query_history())
      where query_text like 'execute NOTEBOOK%Uncle Yer%') as actual 
   , 1 as expected 
   , 'Notebook success!' as description 
); 


create or replace TABLE FRUITS_DETAILS (
	Fruit_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

select * from GARDEN_PLANTS.FRUITS.FRUITS_DETAILS;

create or replace table vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);

create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    type = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    field_delimiter = '|' --pipes as column separators
    skip_header = 1 --one header row to skip
    ;


copy into vegetable_details_soil_type
from @util_db.public.my_internal_stage
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );

select * from vegetable_details_soil_type;


create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = ',' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'; --this means that some values will be wrapped in double-quotes bc they have commas in them

select $1
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv;

select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );




create file format garden_plants.veggies.L9_CHALLENGE_FF  
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = '\t' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'; 

create or replace table LU_SOIL_TYPE(
SOIL_TYPE_ID number,	
SOIL_TYPE varchar(15),
SOIL_DESCRIPTION varchar(75)
);

copy into LU_SOIL_TYPE
from @util_db.public.my_internal_stage
files = ( 'LU_SOIL_TYPE.tsv')
file_format = ( format_name=garden_plants.veggies.L9_CHALLENGE_FF );

select * from LU_SOIL_TYPE;

 

create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT(
plant_name Varchar(50),
UOM varchar(2),
Low_End_of_Range Number,
High_End_of_Range number
);

copy into VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files = ( 'veg_plant_height.csv')
file_format = ( format_name=garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW );


select * from VEGETABLE_DETAILS_PLANT_HEIGHT;

use role sysadmin;

// Create a new database and set the context to use the new database
create database library_card_catalog comment = 'DWW Lesson 10 ';

//Set the worksheet context to use the new database
use database library_card_catalog;


use database library_card_catalog;
use role sysadmin;

// Create the book table and use AUTOINCREMENT to generate a UID for each new row

create or replace table book
( book_uid number autoincrement
 , title varchar(50)
 , year_published number(4,0)
);

// Insert records into the book table
// You don't have to list anything for the
// BOOK_UID field because the AUTOINCREMENT property 
// will take care of it for you

insert into book(title, year_published)
values
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

// Check your table. Does each row have a unique id? 
select * from book;


// Create Author table
create or replace table author (
   author_uid number 
  ,first_name varchar(50)
  ,middle_name varchar(50)
  ,last_name varchar(50)
);

// Insert the first two authors into the Author table
insert into author(author_uid, first_name, middle_name, last_name)  
values
(1, 'Fiona', '','Macdonald')
,(2, 'Gian','Paulo','Faleschini');

// Look at your table with it's new rows
select * from author;

create sequence seq_author_uid
    start = 1,
    increment = 1,
    ORDER,
    comment = 'use this to fill in author_uid';


use role sysadmin;

//See how the nextval function works
select seq_author_uid.nextval;

SHOW SEQUENCES;

//See how the nextval function works
select seq_author_uid.nextval,seq_author_uid.nextval;


use role sysadmin;

//Drop and recreate the counter (sequence) so that it starts at 3 
// then we'll add the other author records to our author table
create or replace sequence library_card_catalog.public.seq_author_uid
start = 3 
increment = 1 
ORDER
comment = 'Use this to fill in the AUTHOR_UID every time you add a row';

//Add the remaining author records and use the nextval function instead 
//of putting in the numbers
insert into author(author_uid,first_name, middle_name, last_name) 
values
(seq_author_uid.nextval, 'Laura', 'K','Egendorf')
,(seq_author_uid.nextval, 'Jan', '','Grover')
,(seq_author_uid.nextval, 'Jennifer', '','Clapp')
,(seq_author_uid.nextval, 'Kathleen', '','Petelinsek');

SELECT * FROM author;



use database library_card_catalog;
use role sysadmin;


// Create the relationships table
// this is sometimes called a "Many-to-Many table"
create table book_to_author
( book_uid number
  ,author_uid number
);

//Insert rows of the known relationships
insert into book_to_author(book_uid, author_uid)
values
 (1,1)  // This row links the 2001 book to Fiona Macdonald
,(1,2)  // This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  // Links 2006 book to Laura K Egendorf
,(3,4)  // Links 2008 book to Jan Grover
,(4,5)  // Links 2016 book to Jennifer Clapp
,(5,6); // Links 2015 book to Kathleen Petelinsek


//Check your work by joining the 3 tables together
//You should get 1 row for every author
select * 
from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid; 


use database library_card_catalog;
use role sysadmin;

// Create an Ingestion Table for JSON Data
create table library_card_catalog.public.author_ingest_json
(
  raw_author variant
);

create file format library_card_catalog.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = TRUE --Setting this to TRUE tells Snowflake to treat each element of that array as a separate row
STRIP_NULL_VALUES = FALSE --some records have "MIDDLE_NAME": null. Setting this to FALSE preserves explicit null values in the VARIANT; set TRUE only if you want to remove null fields entirely.
IGNORE_UTF8_ERRORS = FALSE;

DESC FILE FORMAT library_card_catalog.public.json_file_format;


select * from library_card_catalog.public.author_ingest_json;

select $1
from @util_db.public.my_internal_stage/author_with_header.json;

copy into library_card_catalog.public.author_ingest_json
from @util_db.public.my_internal_stage
files = ( 'author_with_header.json')
file_format = ( format_name=library_card_catalog.public.json_file_format );


select raw_author from author_ingest_json;

SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;


create or replace table library_card_catalog.public.nested_ingest_json 
(
  raw_nested_book VARIANT
);


COPY INTO library_card_catalog.public.nested_ingest_json 
FROM '@"UTIL_DB"."PUBLIC"."MY_INTERNAL_STAGE"/json_book_author_nested.txt'
file_format = ( format_name=library_card_catalog.public.json_file_format );

SELECT raw_nested_book FROM library_card_catalog.public.nested_ingest_json;


SELECT raw_nested_book:book_title FROM library_card_catalog.public.nested_ingest_json;


select raw_nested_book from nested_ingest_json;

select raw_nested_book:year_published from nested_ingest_json;

select raw_nested_book:authors from nested_ingest_json;

select value:first_name
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);

select value:first_name
from nested_ingest_json
,table(flatten(raw_nested_book:authors));

//Add a CAST command to the fields returned
SELECT value:first_name::varchar, value:last_name::varchar
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);

SELECT value:first_name::varchar, value:last_name::varchar
from nested_ingest_json
,TABLE(FLATTEN(raw_nested_book:authors));

//Assign new column  names to the columns using "AS"
select value:first_name::varchar as first_nm
, value:last_name::varchar as last_nm
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);


CREATE DATABASE SOCIAL_MEDIA_FLOODGATES;
USE DATABASE SOCIAL_MEDIA_FLOODGATES;

CREATE or replace TABLE SOCIAL_MEDIA_FLOODGATES.Public.TWEET_INGEST (RAW_STATUS variant);


create file format SOCIAL_MEDIA_FLOODGATES.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
STRIP_OUTER_ARRAY = TRUE ;


SELECT $1
FROM '@"UTIL_DB"."PUBLIC"."MY_INTERNAL_STAGE"/nutrition_tweets.json';

copy into SOCIAL_MEDIA_FLOODGATES.Public.TWEET_INGEST
FROM '@"UTIL_DB"."PUBLIC"."MY_INTERNAL_STAGE"/nutrition_tweets.json'
file_format=(format_name=SOCIAL_MEDIA_FLOODGATES.public.json_file_format);

select RAW_STATUS:created_at::string from SOCIAL_MEDIA_FLOODGATES.Public.TWEET_INGEST;

select raw_status
from tweet_ingest;

select raw_status:entities
from tweet_ingest;


select raw_status:entities:hashtags
from tweet_ingest;

//Explore looking at specific hashtags by adding bracketed numbers
//This query returns just the first hashtag in each tweet
select raw_status:entities:hashtags[0].text
from tweet_ingest;

//This version adds a WHERE clause to get rid of any tweet that 
//doesn't include any hashtags
select raw_status:entities:hashtags[0].text
from tweet_ingest
where raw_status:entities:hashtags[0].text is not null;

//Perform a simple CAST on the created_at key
//Add an ORDER BY clause to sort by the tweet's creation date
select raw_status:created_at::date
from tweet_ingest
order by raw_status:created_at::date;

select value
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls);

select value
from tweet_ingest
,table(flatten(raw_status:entities:urls));

select value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);

//Add the Tweet ID and User ID to the returned table so we could join the hashtag back to it's source tweet
select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);


create or replace view social_media_floodgates.public.HASHTAGS_NORMALIZED as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:display_url::text as url_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls)
);

select * from social_media_floodgates.public.urls_normalized;


select count(*) 
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.VIEWS 
    where table_name = 'HASHTAGS_NORMALIZED';

