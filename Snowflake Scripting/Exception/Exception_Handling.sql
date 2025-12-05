-- Demo program to handle exceptions
-- Get the number of employees working in the given Country, return if any errors
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_EXCEPTION_HANDLING_DEMO("COUNTRY" VARCHAR)
RETURNS INTEGER
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    no_data_found EXCEPTION (-20101, 'No Data Found in Employee Table');
    cnt INTEGER;
    loc_emp_count INTEGER;
    lec DATE;
    
BEGIN
SELECT COUNT(*) into :cnt FROM hrdata.employees;

IF(:cnt = 0) THEN
	RAISE no_data_found;
END IF;

SELECT COUNT(*) into :loc_emp_count FROM hrdata.employees e
JOIN hrdata.departments d ON e.dept_id = d.dept_id
JOIN hrdata.locations l ON l.location_id = d.location_id
WHERE UPPER(l.country_id) = :COUNTRY;

--lec := loc_emp_count;

RETURN 'Number of Employees working in '||COUNTRY||' is: '||loc_emp_count;
--RETURN 'Number of Employees working in '||COUNTRY||' is: '||lec;

EXCEPTION 
WHEN statement_error THEN
RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT ERROR',
						'SQLCODE', sqlcode,
						'SQLERRM', sqlerrm,
						'SQLSTATE', sqlstate);
						
WHEN expression_error THEN 
RETURN OBJECT_CONSTRUCT('Error type', 'EXPRESSION ERROR',
						'SQLCODE', sqlcode,
						'SQLERRM', sqlerrm,
						'SQLSTATE', sqlstate); 

WHEN no_data_found THEN
RETURN OBJECT_CONSTRUCT('Error type', 'USER-DEFINED EXCEPTION',
						'SQLCODE', sqlcode,
						'SQLERRM', sqlerrm,
						'SQLSTATE', sqlstate);
END;


CALL EMP.PROCS.SP_EXCEPTION_HANDLING_DEMO('US');
CALL EMP.PROCS.SP_EXCEPTION_HANDLING_DEMO('UK'); 

============================================
-- Inserting the errors into a log table
CREATE SCHEMA IF NOT EXISTS EMP.WORK;

CREATE OR REPLACE TABLE EMP.WORK.SP_ERROR_LOGS
(PROC_NAME VARCHAR, ERROR_TYPE VARCHAR, ERROR_CODE VARCHAR, ERROR_MESSAGE VARCHAR, SQL_STATE VARCHAR, LOAD_TIME TIMESTAMP);

-- Get the number of employees working in the given Country, log if any error into a log table
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_ERROR_LOGGING_DEMO("COUNTRY" VARCHAR)
RETURNS INTEGER
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    no_data_found EXCEPTION (-20101, 'No Data Found in Employee Table');
    cnt INTEGER;
    loc_emp_count INTEGER;
    lec DATE;
    
BEGIN
SELECT COUNT(*) into :cnt FROM hrdata.employees;

IF(:cnt = 0) THEN
	RAISE no_data_found;
END IF;

SELECT COUNT(*) into :loc_emp_count FROM hrdata.employees e
JOIN hrdata.departments d ON e.dept_id = d.dept_id
JOIN hrdata.locations l ON l.location_id = d.location_id
WHERE UPPER(l.country_id) = :COUNTRY;

--lec := loc_emp_count;

RETURN 'Number of Employees working in '||COUNTRY||' is: '||loc_emp_count;
--RETURN 'Number of Employees working in '||COUNTRY||' is: '||lec;

EXCEPTION 
WHEN statement_error THEN
	INSERT INTO WORK.SP_ERROR_LOGS (PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE, LOAD_TIME)
	VALUES ('SP_ERROR_LOGGING_DEMO', 'STATEMENT ERROR', :SQLCODE, :SQLERRM, :SQLSTATE, SYSDATE());
	
WHEN expression_error THEN
	INSERT INTO WORK.SP_ERROR_LOGS (PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE, LOAD_TIME)
	VALUES ('SP_ERROR_LOGGING_DEMO', 'EXPRESSION ERROR', :SQLCODE, :SQLERRM, :SQLSTATE, SYSDATE());

WHEN no_data_found THEN
	INSERT INTO WORK.SP_ERROR_LOGS (PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE, LOAD_TIME)
	VALUES ('SP_ERROR_LOGGING_DEMO', 'USER DEFINED EXCEPTION', :SQLCODE, :SQLERRM, :SQLSTATE, SYSDATE());
END;


CALL EMP.PROCS.SP_ERROR_LOGGING_DEMO('US');
CALL EMP.PROCS.SP_ERROR_LOGGING_DEMO('UK'); 

SELECT * FROM EMP.WORK.SP_ERROR_LOGS; 

--DELETE FROM hrdata.employees;
