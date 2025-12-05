CREATE OR REPLACE PROCEDURE DEV_EMP.PROCS.SP_DROP_SCHEMA_TABLES("DB" VARCHAR, "SCHEMA" VARCHAR) 
RETURNS VARCHAR 
LANGUAGE SQL 
EXECUTE AS CALLER 
AS

DECLARE 

cur_tbl_list cursor for SELECT DISTINCT TABLE_NAME FROM TABLE(?) WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE';
ret_string VARCHAR;
tbl_nm VARCHAR;
fq_tbl_nm VARCHAR;
drop_qry VARCHAR;

BEGIN

IF (UPPER(:DB) like '%PROD%' or UPPER(:DB) like '%PRD%') THEN
	ret_string := 'Warning!! you are running this Procedure on Production Database';
		
ELSE
	OPEN cur_tbl_list USING(:DB||'.INFORMATION_SCHEMA.TABLES', :SCHEMA);
	
	FOR tbl IN cur_tbl_list DO
		tbl_nm := tbl.TABLE_NAME;
		fq_tbl_nm := :DB||'.'||:SCHEMA||'."'||:tbl_nm||'"';
		drop_qry := 'DROP TABLE ' || :fq_tbl_nm;
		execute immediate :drop_qry;
		
	END FOR;
	
	CLOSE cur_tbl_list;
	
	ret_string := 'ALL Tables Dropped Successfully';
	
END IF;

return ret_string;

END;


-- Calling procedure
-- CALL DEV_EMP.PROCS.SP_DROP_SCHEMA_TABLES('DEV_EMP', 'HRDATA');