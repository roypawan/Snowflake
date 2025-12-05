CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_SRC_TO_TGT_FULL_LOAD("SOURCE_TABLE" VARCHAR, "TARGET_TABLE" VARCHAR) 
RETURNS VARCHAR 
LANGUAGE SQL 
EXECUTE AS CALLER 
AS

BEGIN 

-- Here SOURCE_TABLE and TARGET_TABLE are fully qualified table names (DB.SCHEMA.TABLE)

-- Truncate target before full load
TRUNCATE IDENTIFIER(:TARGET_TABLE);

-- Insert the full data into target table
INSERT INTO IDENTIFIER(:TARGET_TABLE)
SELECT * FROM IDENTIFIER(:SOURCE_TABLE);

/* or simply write like
INSERT OVERWRITE INTO IDENTIFIER(:TARGET_TABLE)
SELECT * FROM IDENTIFIER(:SOURCE_TABLE);
*/

/* if Source and Target table DDL are different then you can't automate this procedure
   you have to write a new procedure for each and every table 
   and you have to mention the column names explicitly like shown below
   
INSERT INTO IDENTIFIER(:TARGET_TABLE)
( 	employee_id
   ,first_name
   ,last_name
   ,email
   ,phone_number
   ,hire_date
   ,job_id
   ,salary
   ,commission_pct
   ,manager_id
   ,dept_id
)
SELECT 
	employee_id
   ,first_name
   ,last_name
   ,email
   ,phone_number
   ,hire_date
   ,job_id
   ,salary
   ,commission_pct
   ,manager_id
   ,dept_id 
FROM IDENTIFIER(:SOURCE_TABLE);

*/

RETURN 'Full Load Completed Successfully';

END;

SELECT COUNT(1) FROM emp.staging.employees; -- 107
SELECT COUNT(1) FROM emp.hrdata.employees; -- 0

CALL EMP.PROCS.SP_SRC_TO_TGT_FULL_LOAD('emp.staging.employees', 'emp.hrdata.employees');

SELECT COUNT(1) FROM emp.hrdata.employees; -- 107

SELECT * FROM emp.hrdata.employees;