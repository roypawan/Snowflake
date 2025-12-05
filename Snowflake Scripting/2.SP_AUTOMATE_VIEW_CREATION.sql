CREATE OR REPLACE PROCEDURE DEV_EMP.PROCS.SP_AUTOMATE_VIEW_CREATION("DB" VARCHAR, "SCHEMA" VARCHAR) 
RETURNS VARCHAR 
LANGUAGE SQL 
EXECUTE AS CALLER 
AS

DECLARE

cur cursor for SELECT DISTINCT TABLE_CATALOG as CATL, TABLE_SCHEMA as SCH, TABLE_NAME as TBL 
				FROM TABLE(?) WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE';
				
dbname varchar;
schname varchar;
tblname varchar;
CreateViewQuery string; 

BEGIN

open cur using(:DB||'.INFORMATION_SCHEMA.TABLES', :SCHEMA);

for rec in cur do 
	dbname := rec.CATL;
	schname := rec.SCH;
	tblname := rec.TBL;
	CreateViewQuery := '';
	
  SELECT concat('CREATE VIEW ', :dbname, '\.', :schname, '\.V_',:tblname,' as (SELECT ' ,listagg(COLUMN_NAME, ','),' FROM ',:dbname,'.',:schname,'.',:tblname, ')')  into :CreateViewQuery 
	FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE TABLE_SCHEMA= :schname and TABLE_NAME = :tblname; 
	
	--CREATE VIEW EMP.HRDATA.V_EMPLOYEES as (SELECT empid, first_name, last_name, salary FROM EMP.HRDATA.EMPLOYEES);
	
	execute immediate :CreateViewQuery;
	
end for;

close cur;

return CreateViewQuery; 

END; 

-- Calling procedure
-- CALL DEV_EMP.PROCS.SP_AUTOMATE_VIEW_CREATION('DEV_EMP', 'HRDATA');