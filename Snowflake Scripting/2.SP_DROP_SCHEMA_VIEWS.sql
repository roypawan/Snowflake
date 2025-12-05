CREATE OR REPLACE PROCEDURE DEV_EMP.PROCS.SP_DROP_SCHEMA_VIEWS("DB" VARCHAR, "SCHEMA" VARCHAR) 
RETURNS VARCHAR 
LANGUAGE SQL 
EXECUTE AS CALLER 
AS

DECLARE 

cur_tbl_list cursor for SELECT DISTINCT TABLE_NAME as VIEW_NAME FROM TABLE(?) WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'VIEW';
ret_string VARCHAR;
vw_nm VARCHAR;
fq_vw_nm VARCHAR;
drop_qry VARCHAR;

BEGIN

IF (UPPER(:DB) like '%PROD%') THEN
	ret_string := 'Warning!! you are running this Procedure on Production Environment';
		
ELSE
	OPEN cur_tbl_list USING(:DB||'.INFORMATION_SCHEMA.TABLES', :SCHEMA);
	
	FOR tbl IN cur_tbl_list DO
		vw_nm := tbl.VIEW_NAME;
		fq_vw_nm := :DB||'.'||:SCHEMA||'."'||:vw_nm||'"';
		drop_qry := 'DROP VIEW ' || :fq_vw_nm;
		execute immediate :drop_qry;
		
	END FOR;
	
	CLOSE cur_tbl_list;
	
	ret_string := 'ALL Views Dropped Successfully';
	
END IF;

return ret_string;

END;


-- Calling procedure
-- CALL DEV_EMP.PROCS.SP_DROP_SCHEMA_VIEWS('DEV_EMP', 'HRDATA');