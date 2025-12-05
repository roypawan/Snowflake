/*
Delta/Incremental load process:
Step 1: Check if the target table has Primary Key defined on the table, if not raise one exception
Step 2: Prepare the Merge query required
	--> Get the list of columns of Primary Key 
	--> Get the list of columns required to form update query
	--> Get the list of columns and values required to form insert query 
	--> Combine all these and prepare the Merge Query
Step 3: Run the Merge query to load the data
*/

CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_SRC_TO_TGT_DELTA_LOAD("SOURCE_TABLE" VARCHAR, "TARGET_TABLE" VARCHAR) 
RETURNS VARCHAR 
LANGUAGE SQL 
EXECUTE AS CALLER 
AS

DECLARE
pk_count INTEGER;
tgt_db_name VARCHAR;
tgt_schema_name VARCHAR;
tgt_table_name VARCHAR;
join_keys VARCHAR;
update_columns VARCHAR;
insert_columns VARCHAR;
insert_values VARCHAR;
merge_statement VARCHAR;

no_keys_found EXCEPTION (-20100, 'As there are no Primary Key present in Target table, Delta Load can\'t be done');
-- Error code must be between -20,999 and -20,000

BEGIN 
-- Here SOURCE_TABLE and TARGET_TABLE are fully qualified table names (DB.SCHEMA.TABLE)

tgt_db_name := SPLIT_PART(:TARGET_TABLE, '.', 1);
tgt_schema_name := SPLIT_PART(:TARGET_TABLE, '.', 2);
tgt_table_name := SPLIT_PART(:TARGET_TABLE, '.', 3);

USE DATABASE IDENTIFIER(:tgt_db_name);
USE SCHEMA IDENTIFIER(:tgt_schema_name);

-- Get list of Primary Keys into a Temp Table
SHOW PRIMARY KEYS IN IDENTIFIER(:TARGET_TABLE);

CREATE OR REPLACE TEMPORARY TABLE temp_primary_key
AS 
SELECT "column_name" as column_name, "key_sequence" as key_sequence 
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
	
-- Make sure that Target table has Primary Key, if not raise exception
SELECT COUNT(1) INTO :pk_count FROM temp_primary_key;

IF(:pk_count = 0) THEN
	RAISE no_keys_found;
END IF;

-- Prepare Join Keys for Merge
SELECT LISTAGG('TGT.' || column_name || ' = SRC.' || column_name, ' AND ') 
WITHIN GROUP(ORDER BY key_sequence) INTO :join_keys
FROM temp_primary_key;

-- Prepare Update Columns for Merge
SELECT LISTAGG('TGT.' || COLUMN_NAME || ' = SRC.' || COLUMN_NAME, ', ') 
WITHIN GROUP(ORDER BY ORDINAL_POSITION) INTO :update_columns 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = UPPER(:tgt_db_name)
AND TABLE_SCHEMA = UPPER(:tgt_schema_name)
AND TABLE_NAME = UPPER(:tgt_table_name)
AND COLUMN_NAME NOT IN (SELECT column_name FROM temp_primary_key);

-- Prepare Insert columns and values list for Merge
SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP(ORDER BY ORDINAL_POSITION), 
		LISTAGG('SRC.'||COLUMN_NAME, ', ') WITHIN GROUP(ORDER BY ORDINAL_POSITION) 
		INTO :insert_columns, :insert_values
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = UPPER(:tgt_db_name)
AND TABLE_SCHEMA = UPPER(:tgt_schema_name)
AND TABLE_NAME = UPPER(:tgt_table_name);

-- Create merge statement
merge_statement := 'MERGE INTO ' || :TARGET_TABLE || ' AS TGT USING ' || :SOURCE_TABLE || ' AS SRC ' || 'ON ' || :join_keys || ' WHEN MATCHED THEN UPDATE SET ' || :update_columns || ' WHEN NOT MATCHED THEN INSERT(' || :insert_columns || ') VALUES(' || :insert_values || ')';

EXECUTE IMMEDIATE :merge_statement;

RETURN 'Delta Load Successfully Completed';

EXCEPTION 
WHEN statement_error THEN
	INSERT INTO WORK.SP_ERROR_LOGS (PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE)
	VALUES ('PROC_NAME', 'STATEMENT_ERROR', :SQLCODE, :SQLERRM, :SQLSTATE);
	
WHEN no_keys_found THEN
	INSERT INTO WORK.SP_ERROR_LOGS (PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE)
	VALUES ('PROC_NAME', 'PRIMARY_KEYS_NOT_FOUND', :SQLCODE, :SQLERRM, :SQLSTATE);

WHEN other THEN
	INSERT INTO WORK.SP_ERROR_LOGS (PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE)
	VALUES ('PROC_NAME', 'OTHER ERROR', :SQLCODE, :SQLERRM, :SQLSTATE);

END;


SELECT COUNT(1) FROM emp.staging.employees; -- 107
SELECT COUNT(1) FROM emp.hrdata.employees; -- 0

-- run the procedure
CALL EMP.PROCS.SP_SRC_TO_TGT_DELTA_LOAD('emp.staging.employees', 'emp.hrdata.employees');

SELECT COUNT(1) FROM emp.hrdata.employees; -- 107
SELECT * FROM emp.hrdata.employees;

-- Error logging table
SELECT * from EMP.WORK.SP_ERROR_LOGS;

-- now delete data from staging table and insert 2 records
-- one record is new record, one record is an update to the existing record
DELETE FROM emp.staging.employees;

INSERT INTO emp.staging.employees VALUES -- new record
   ( 300,  'Priyanka',  'Gurram',  'PGURRAM',  '420.231.8914',  TO_DATE('11-JUN-1988', 'dd-MON-yyyy'),  'ADM_PRES',  10500,  NULL,  108,  30);

INSERT INTO emp.staging.employees VALUES -- existing record with an update
   ( 100,  'Ramana',  'Rao',  'RRAO',  '420.271.4567',  TO_DATE('17-JUN-1987', 'dd-MON-yyyy'),  'ADM_PRES',  30000,  NULL,  NULL,  90);

SELECT * FROM emp.hrdata.employees WHERE employee_id in (100, 300);

-- mow run the procedure again to do the Delta load
CALL EMP.PROCS.SP_SRC_TO_TGT_DELTA_LOAD('emp.staging.employees', 'emp.hrdata.employees');

SELECT COUNT(1) FROM emp.hrdata.employees; -- 108

SELECT * FROM emp.hrdata.employees WHERE employee_id in (100, 300);
