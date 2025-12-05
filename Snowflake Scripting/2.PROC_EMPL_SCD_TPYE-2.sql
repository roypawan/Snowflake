-- Procedure to implement SCD Type-2
CREATE OR REPLACE PROCEDURE EMP.TARGET.PROC_EMPL_SCD_TPYE2()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS

DECLARE 
	cur_ts TIMESTAMP;
	
BEGIN
	cur_ts := CURRENT_TIMESTAMP();
	
	--Expire old records and insert new records
	MERGE INTO TARGET.EMPL_SCD2 TGT
	USING STAGING.STREAM_STG_EMPL_U STR
	ON TGT.emp_id = STR.eid
	AND TGT.expiry_datetime is NULL
	
	WHEN MATCHED
	  AND STR.METADATA$ACTION = 'DELETE'
	  AND STR.METADATA$ISUPDATE = 'TRUE'
	THEN
	  UPDATE SET TGT.expiry_datetime = :cur_ts
	  
	WHEN NOT MATCHED THEN
	  INSERT(emp_id, emp_name, date_of_birth, email_id, phone_number, salary, department, work_location, effective_datetime, expiry_datetime)
	  VALUES(STR.eid, STR.ename, STR.dob, STR.mail, STR.phone, STR.salary, STR.dept, STR.loc, :cur_ts, null)
	;
	
	--Insert the new version of old records
	INSERT INTO TARGET.EMPL_SCD2
	(emp_id, emp_name, date_of_birth, email_id, phone_number, salary, department, work_location, effective_datetime, expiry_datetime)
	SELECT eid, ename, dob, mail, phone, salary, dept, loc, :cur_ts, null
	FROM STAGING.STREAM_STG_EMPL_I
	WHERE METADATA$ACTION = 'INSERT'
	  AND METADATA$ISUPDATE = 'TRUE'
	;
	
	RETURN 'Procedure Completed Successfully';
	
END;