SET DB = 'EMP';
SET TBL_SCHEMA = 'HRDATA';
SET PROC_SCHEMA = 'PROCS';

USE DATABASE $DB; -- Not Correct
USE DATABASE IDENTIFIER($DB); -- Correct

CREATE SCHEMA IF NOT EXISTS ($PROC_SCHEMA); -- Not Correct
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($PROC_SCHEMA); -- Correct
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($TBL_SCHEMA);

USE SCHEMA IDENTIFIER($PROC_SCHEMA);

-- Procedure to return the row counts of 4 tables in EMP database.
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_LITERALS_IDENTIFIERS_DEMO("DBNAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Demo Program on Literals and Identifiers'
EXECUTE AS CALLER 
AS

DECLARE
    table1 VARCHAR;
    table2 VARCHAR default 'departments';
    dept INTEGER;
    empl INTEGER;
    cntry INTEGER;
    loc INTEGER;
	
BEGIN
    table1 := 'employees';
    LET table3 := 'countries';

    USE DATABASE IDENTIFIER(:DBNAME);
    USE SCHEMA IDENTIFIER($TBL_SCHEMA);

    SELECT COUNT(*) into :empl FROM TABLE(:table1);
    SELECT COUNT(*) into :dept FROM IDENTIFIER(:table2);
    SELECT COUNT(*) into :cntry FROM TABLE(:table3);
    SELECT COUNT(*) into :loc FROM IDENTIFIER('emp.hrdata.locations');

    RETURN 'employees count '||empl||', departments count '||dept||', countries count '||cntry||', locations count '||loc;
END;

CALL EMP.PROCS.SP_LITERALS_IDENTIFIERS_DEMO('EMP');
