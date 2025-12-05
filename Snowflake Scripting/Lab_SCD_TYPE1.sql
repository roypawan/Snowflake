SCD Type-I implementation using Streams
-----------------------------------------

CREATE DATABASE IF NOT EXISTS EMP;

CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS TARGET;

-- Create a stage table
CREATE TABLE STAGING.STG_EMPL
( eid INTEGER not null,
  ename VARCHAR(30),
  dob DATE,
  mail VARCHAR(50),
  phone VARCHAR(20),
  salary INTEGER,
  dept VARCHAR(30),
  loc VARCHAR(20),
  PRIMARY KEY(eid)
);

-- Create the target table
CREATE OR REPLACE TABLE TARGET.EMPL_SCD1
( emp_id INTEGER not null,
  emp_name VARCHAR(50),
  date_of_birth DATE,
  email_id VARCHAR(50),
  phone_number VARCHAR(20),
  salary INTEGER,
  department VARCHAR(30),
  work_location VARCHAR(30),
  insert_ts TIMESTAMP,
  last_update_ts TIMESTAMP,
  PRIMARY KEY(emp_id)
);

-- Create a stream on stage table
CREATE STREAM STAGING.STREAM_STG_EMPL ON TABLE STAGING.STG_EMPL;

-- Merge the changes to target table
MERGE INTO TARGET.EMPL_SCD1 TGT
USING STAGING.STREAM_STG_EMPL STR 
ON TGT.emp_id = STR.eid

WHEN MATCHED 
 AND STR.METADATA$ACTION ='INSERT' 
 AND STR.METADATA$ISUPDATE ='TRUE'
 AND (TGT.email_id <> STR.mail OR
      TGT.phone_number <> STR.phone OR
      TGT.salary <> STR.salary OR
      TGT.department <> STR.dept OR
      TGT.work_location <> STR.loc
    )
THEN UPDATE SET 
    TGT.email_id = STR.mail,
    TGT.phone_number = STR.phone,
    TGT.salary = STR.salary,
    TGT.department = STR.dept,
    TGT.work_location = STR.loc,
    TGT.last_update_ts = current_timestamp

WHEN NOT MATCHED THEN
INSERT(emp_id, emp_name, date_of_birth, email_id, phone_number, salary, department, work_location, insert_ts, last_update_ts)
VALUES(STR.eid, STR.ename, STR.dob, STR.mail, STR.phone, STR.salary, STR.dept, STR.loc, current_timestamp, current_timestamp)
;

-- Schedule this merge query using Task
CREATE OR REPLACE TASK TARGET.TASK_EMPL_DATA_LOAD
    SCHEDULE = '2 MINUTES'
    WHEN SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_STG_EMPL')
AS
MERGE INTO TARGET.EMPL_SCD1 TGT
USING STAGING.STREAM_STG_EMPL STR 
ON TGT.emp_id = STR.eid

WHEN MATCHED 
 AND STR.METADATA$ACTION ='INSERT' 
 AND STR.METADATA$ISUPDATE ='TRUE'
 AND (TGT.email_id <> STR.mail OR
      TGT.phone_number <> STR.phone OR
      TGT.salary <> STR.salary OR
      TGT.department <> STR.dept OR
      TGT.work_location <> STR.loc
    )
THEN UPDATE SET 
    TGT.email_id = STR.mail,
    TGT.phone_number = STR.phone,
    TGT.salary = STR.salary,
    TGT.department = STR.dept,
    TGT.work_location = STR.loc,
    TGT.last_update_ts = current_timestamp

WHEN NOT MATCHED THEN
INSERT(emp_id, emp_name, date_of_birth, email_id, phone_number, salary, department, work_location, insert_ts, last_update_ts)
VALUES(STR.eid, STR.ename, STR.dob, STR.mail, STR.phone, STR.salary, STR.dept, STR.loc, current_timestamp, current_timestamp)
;

// Start the task
ALTER TASK TARGET.TASK_EMPL_DATA_LOAD RESUME;

-- Data load into stage table
INSERT INTO STAGING.STG_EMPL VALUES
(1, 'Rahul Sharma', '1986-04-15', 'rahul.sharma@gmail.com','9988776655', 92000, 'Administration', 'Bangalore'),
(2, 'Renuka Devi', '1993-10-19', 'renuka1993@yahoo.com','+91 9911882255', 61000, 'Sales', 'Hyderabad'),
(3, 'Kamalesh', '1991-02-08', 'kamal91@outlook.com','9182736450', 59000, 'Sales', 'Chennai'),
(4, 'Arun Kumar', '1989-05-20', 'arun_kumar@gmail.com','901-287-3465', 74500, 'IT', 'Bangalore')
;

SELECT * FROM STAGING.STG_EMPL;

-- Observe the Streams now with change capture
SELECT * FROM STAGING.STREAM_STG_EMPL;

-- After 1st run
-- Verify the data in Target table
SELECT * FROM TARGET.EMPL_SCD1;

-- Observe the Streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL;

-------------------------------------------
-- Make changes to the Stage table (Assume it is truncate and load with new and updated records)

INSERT INTO STAGING.STG_EMPL VALUES
(5, 'Deepika Kaur', '1995-09-03', 'deepikakaur@gmail.com', '9871236054', 58000, 'IT', 'Pune')
;

UPDATE STAGING.STG_EMPL SET phone = '9911882255' WHERE eid = 2;

-- Observe the Streams now with change capture
SELECT * FROM STAGING.STREAM_STG_EMPL;

-- After 2 minutes (task will be running for every 2 mins)
-- Verify the data in Target table
SELECT * FROM TARGET.EMPL_SCD1;

-- Observe the Streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL;

-------------------------------------------
-- One more time Make changes to the Stage table (Assume it is truncate and load with new and updated records)

INSERT INTO STAGING.STG_EMPL VALUES
(6, 'Venkatesh D', '1992-01-27', 'dvenkat92@gmail.com', '8921764305', 63500, 'IT', 'Chennai')
;

UPDATE STAGING.STG_EMPL SET salary = 65000 WHERE eid = 2;
UPDATE STAGING.STG_EMPL SET phone = '9012873465', loc = 'Hyderabad' WHERE eid = 4;


-- Observe the Streams now with change capture
SELECT * FROM STAGING.STREAM_STG_EMPL;

-- After 2 minutes (task will be running for every 2 mins)
-- Verify the data in Target table
SELECT * FROM TARGET.EMPL_SCD1;

-- Observe the Streams now after consuming the changes
SELECT * FROM STAGING.STREAM_STG_EMPL;

--------------
-- Stop or drop the task, otherwise all your free credits will be consumed
ALTER TASK TARGET.TASK_EMPL_DATA_LOAD SUSPEND;