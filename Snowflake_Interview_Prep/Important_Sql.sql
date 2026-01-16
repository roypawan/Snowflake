Questions::Is Query Result Cache used when querying tables with Dynamic Data Masking?
==>The Query Result Cache is supported for masking policies. But Query Result Cache is not supported for UDFs (User Defined Functions). So, the limitation is that if UDF is used inside a masking policy body, then the Query Result Cache is not used when querying tables with Dynamic Data Masking. 

A sample script to demonstrate Result cache re-use with Dynamic Data Masking:
--create masking policy
CREATE MASKING POLICY policy1 AS (VAL VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN VAL
    ELSE '****'
  END
;

-- create table
CREATE TABLE table1 (
  id             INTEGER
 ,sensitive_col  VARCHAR
);

-- apply masking policy to the table
ALTER TABLE table1 MODIFY COLUMN sensitive_col SET MASKING POLICY policy1;

-- insert some data into the table
INSERT INTO table1 (id, sensitive_col) VALUES (1, 'xyz');
INSERT INTO table1 (id, sensitive_col) VALUES (2, 'abc');

-- Execute query first time
SELECT * FROM table1 WHERE id = 1;

-- Execute exact same query again, the result set cache used
SELECT * FROM table1 WHERE id = 1;

*************************************************************************************************************************

A sample script to demonstrate Result cache not being used for Dynamic Data Masking policy with a user defined function(UDF):
--create user defined function
CREATE OR REPLACE FUNCTION full_masking(v varchar)
  RETURNS varchar
  LANGUAGE JAVASCRIPT
  AS
  $$
    return "***MASKED***";
  $$;

--create masking policy which invokes the user defined function
CREATE MASKING POLICY policy5 AS (VAL VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN VAL
    ELSE full_masking(val)
  END
;

-- create table
CREATE TABLE table5 (
  id             INTEGER
 ,sensitive_col  VARCHAR
);

-- apply masking policy to the table
ALTER TABLE table5 MODIFY COLUMN sensitive_col SET MASKING POLICY policy5;

-- insert some data into the table
INSERT INTO table5 (id, sensitive_col) VALUES (1, 'xyz');
INSERT INTO table5 (id, sensitive_col) VALUES (2, 'abc');

-- Execute query first time
SELECT * FROM table5 WHERE id = 1;

-- Execute exact same query again, the result set cache not used
SELECT * FROM table5 WHERE id = 1;
