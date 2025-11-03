***************************************************************************************************************
                                        LoadingData
***************************************************************************************************************

// Create database
CREATE DATABASE FIRST_DB;

// Rename database 
ALTER DATABASE FIRST_DB RENAME TO OUR_FIRST_DB; 

// Create the table + meta data
CREATE TABLE "OUR_FIRST_DB"."PUBLIC"."LOAN_PAYMENT" (
  "Loan_ID" STRING,
  "loan_status" STRING,
  "Principal" STRING,
  "terms" STRING,
  "effective_date" STRING,
  "due_date" STRING,
  "paid_off_time" STRING,
  "past_due_days" STRING,
  "age" STRING,
  "education" STRING,
  "Gender" STRING);
  
  
 // Check that table is empy
 USE DATABASE OUR_FIRST_DB;

 SELECT * FROM LOAN_PAYMENT where "loan_Status"='PAIDOFF';

 
 // Loading the data from S3 bucket
  
 COPY INTO LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    file_format = (type = csv 
                   field_delimiter = ',' 
                   skip_header=1);
    

//Validate
 SELECT * FROM LOAN_PAYMENT;
//***************************************************************************************************************
                       -- StorageMonitoring
//***************************************************************************************************************
show tables;

DESCRIBE TABLE LOAN_PAYMENT;

USE ROLE ACCOUNTADMIN;
-- Most detailed information:
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

CREATE OR REPLACE DATABASE STORAGE_TEST;
CREATE OR REPLACE TABLE STORAGE_TEST.PUBLIC.TEST_TABLE (COLUMN_1 VARCHAR);
INSERT INTO STORAGE_TEST.PUBLIC.TEST_TABLE VALUES ('Text1Text2Text3');

select * from STORAGE_TEST.PUBLIC.TEST_TABLE;


//****************************************************************************************************
                             Create stage
------------------------------------------------------------------------------------------------------

// Show all named stages
SHOW STAGES;

// List files in user stage;
LIST @~;

// List files in user stage;
LIST @%LOAN_PAYMENT;


// Database to manage stage objects, fileformats etc.

CREATE OR REPLACE DATABASE manage_db;

CREATE OR REPLACE SCHEMA external_stages;


// Creating external stage

CREATE OR REPLACE STAGE manage_db.external_stages.aws_stage
    url='s3://bucketsnowflakes3'
    credentials=(aws_key_id='ABCD_DUMMY_ID' aws_secret_key='1234abcd_key');


// Description of external stage

DESC STAGE manage_db.external_stages.aws_stage; 
    
    
// Alter external stage   

ALTER STAGE aws_stage
    SET credentials=(aws_key_id='XYZ_DUMMY_ID' aws_secret_key='987xyz');
    
    
// Publicly accessible staging area    

CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3://bucketsnowflakes3';

// List files in stage

LIST @aws_stage;


//******************************************************************************************************


// Creating ORDERS table

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS;

 // First copy command

 COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
     FROM @MANAGE_DB.external_stages.aws_stage
     file_format = (type = csv field_delimiter=',' skip_header=1);

     

// Copy command with fully qualified stage object name

 COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
     FROM @MANAGE_DB.external_stages.aws_stage
     file_format = (type = csv field_delimiter=',' skip_header=1);


 // List files contained in stage

LIST @MANAGE_DB.external_stages.aws_stage;


 // Copy command with specified file(s)

 COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
     FROM @MANAGE_DB.external_stages.aws_stage
     file_format = (type = csv field_delimiter=',' skip_header=1)
     files = ('OrderDetails.csv');



 // Copy command with pattern for file names

 COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
     FROM @MANAGE_DB.external_stages.aws_stage
     file_format = (type = csv field_delimiter=',' skip_header=1)
     pattern = '.*Order.*';

     




//***************************************************************************************************************
                //File_format 


DESC STAGE MANAGE_DB.external_stages.aws_stage;


// Creating schema to keep things organized
CREATE OR REPLACE SCHEMA MANAGE_DB.file_formats;

// Creating file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.my_file_format
TYPE = CSV;

// See properties of file format object
DESC file format MANAGE_DB.file_formats.my_file_format;


CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
URL= 's3://bucketsnowflakes3'
FILE_FORMAT = (FORMAT_NAME=MANAGE_DB.file_formats.my_file_format);

// Reset table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));    

// Specifying file_format in Copy command
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    files = ('OrderDetails.csv')
       
    

// Altering file format object
ALTER file format MANAGE_DB.file_formats.my_file_format
    SET SKIP_HEADER = 1;

    
// Defining properties on creation of file format object   
CREATE OR REPLACE file format MANAGE_DB.file_formats.my_file_format
    TYPE=JSON,
    TIME_FORMAT=AUTO;    
    
// See properties of file format object    
DESC file format MANAGE_DB.file_formats.my_file_format;   

  
//***************************************************************************************************************
        //Insert&Update
// Reset table

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
	ORDER_ID VARCHAR (30),
	AMOUNT INT,
	PROFIT INT,
	QUANTITY INT,
	CATEGORY VARCHAR(30),
	SUBCATEGORY VARCHAR(30));

// Insert single row
INSERT INTO OUR_FIRST_DB.PUBLIC.ORDERS
VALUES (1,0,0,0, 'None','None');


SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS;

// Insert multiple rows
INSERT INTO OUR_FIRST_DB.PUBLIC.ORDERS
VALUES 
(2,12,4,1, 'Garden','Flowers'),
(3,15,6,2, 'House','Kitchen'),
(4,11,2,1, 'House','Sleeping');

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS;


// Insert into specific columns
INSERT INTO OUR_FIRST_DB.PUBLIC.ORDERS
VALUES 
(2,'Flowers'),
(3,'Kitchen'),
(4,'Sleeping');

INSERT INTO OUR_FIRST_DB.PUBLIC.ORDERS (ORDER_ID, SUBCATEGORY)
VALUES 
(2,'Flowers'),
(3,'Kitchen'),
(4,'Sleeping');

INSERT INTO OUR_FIRST_DB.PUBLIC.ORDERS (ORDER_ID, SUBCATEGORY)
VALUES 
(2,'Flowers'),
(3,'Kitchen'),
(4,'Sleeping');

INSERT INTO OUR_FIRST_DB.PUBLIC.ORDERS (ORDER_ID, SUBCATEGORY)
VALUES 
(2,'Flowers'),
(3,'Kitchen'),
(4,'Sleeping');


select * from OUR_FIRST_DB.PUBLIC.ORDERS;

//INSERT OVERWRITE - Truncates the table
INSERT OVERWRITE INTO OUR_FIRST_DB.PUBLIC.ORDERS (ORDER_ID, SUBCATEGORY)
VALUES 
(20,'Flowers'),
(30,'Kitchen'),
(40,'Sleeping');

UPDATE OUR_FIRST_DB.PUBLIC.ORDERS
SET ORDER_ID=1
WHERE ORDER_ID=20;

// Truncate (removes all data in the table)

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS;

// Drop Table
DROP TABLE OUR_FIRST_DB.PUBLIC.ORDERS;




//***************************************************************************************************************

                --Create stage and integration


USE DATABASE MANAGE_DB;

create or replace stage manage_db.public.stage_azure
    URL = 'azure://<your-container-url>';
   

-- list files
LIST @manage_db.public.stage_azure;

-- create integration object that contains the access information
CREATE OR REPLACE STORAGE INTEGRATION azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID =  'ef058d6e-fb4d-41fa-b90b-067b342605c3'
  STORAGE_ALLOWED_LOCATIONS = ( 'https://<your-container-url>');

  
  
-- Describe integration object to provide access
DESC STORAGE integration azure_integration;

---- Create file format & stage objects ----

-- create file format
create or replace file format manage_db.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage manage_db.public.stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'https://<your-container-url>'
    FILE_FORMAT = fileformat_azure;
    

-- list files
LIST @snowpipe.public.stage_azure;



create or replace stage manage_db.public.stage_azure
    URL = 'https://<your-container-url>';
   

-- list files
LIST @manage_db.public.stage_azure;


//*************************************************************************************************************** --Copy into and ON_ERROR

// Create new stage
 CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage_errorex
    url='s3://bucketsnowflakes4';
 
 // List files in stage
 LIST @MANAGE_DB.external_stages.aws_stage_errorex;


 
 SELECT $1,$2,$3,$4,$5,$6 FROM  @MANAGE_DB.external_stages.aws_stage_errorex; 
 
 // Create example table
 CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));


   
 
 // Demonstrating error message
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
     file_format= (type = csv field_delimiter=',' skip_header=1)
     files = ('OrderDetails_error.csv');
    

 // Validating table is empty    
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX  ;  
    
LIST @MANAGE_DB.external_stages.aws_stage_errorex/OrderDetails_error.csv;


  // Error handling using the ON_ERROR option
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'CONTINUE';
    
  // Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX; --1498

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;

// Error handling using the ON_ERROR option = ABORT_STATEMENT (default)
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'ABORT_STATEMENT';


  // Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;

// Error handling using the ON_ERROR option = SKIP_FILE
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE';
    
    
  // Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;    
    

// Error handling using the ON_ERROR option = SKIP_FILE_<number>
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE_2';    
    
    
  // Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;    

    
// Error handling using the ON_ERROR option = SKIP_FILE_<number>
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE_3%'; 
  
  
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;

SELECT count(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;


 CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));





COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = SKIP_FILE_3 
    SIZE_LIMIT = 30;

//***************************************************************************************************************
---- VALIDATION_MODE ----
// Prepare database & table
CREATE OR REPLACE DATABASE COPY_DB;


CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

// Prepare stage object
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3://snowflakebucket-copyoption/size/';
  
LIST @COPY_DB.PUBLIC.aws_stage_copy;
  
    
 //Load data using copy command
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;
    
SELECT * FROM ORDERS;    
    
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
   VALIDATION_MODE = RETURN_5_ROWS ;


--- Use files with errors ---

create or replace stage copy_db.public.aws_stage_copy
    url ='s3://snowflakebucket-copyoption/returnfailed/';
    
list @copy_db.public.aws_stage_copy;

-- show all errors --
copy into copy_db.public.orders
    from @copy_db.public.aws_stage_copy
    file_format = (type=csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    validation_mode=return_errors;

-- validate first n rows --
copy into copy_db.public.orders
    from @copy_db.public.aws_stage_copy
    file_format = (type=csv field_delimiter=',' skip_header=1)
    pattern='.*error.*'
    validation_mode=return_1_rows;
    
    

//***************************************************************************************************************

//***************************************************************************************************************



//***************************************************************************************************************



//***************************************************************************************************************


//***************************************************************************************************************


//***************************************************************************************************************


//***************************************************************************************************************







//***************************************************************************************************************






