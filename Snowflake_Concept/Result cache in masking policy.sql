--create masking policy
USE DATABASE TEST_DEMO;
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

