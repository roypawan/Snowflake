////// ROW ACCESS POLICIES ///////


-- Table of interest
SELECT * FROM DATA_S.PUBLIC.ORDERS;

-- Thinking of policy based on category
SELECT DISTINCT CATEGORY FROM DATA_S.PUBLIC.ORDERS;

USE DATA_S.PUBLIC;
USE ROLE ACCOUNTADMIN;

-- Set up role
create or replace role home_manager;

grant usage on database data_s to role home_manager;
grant usage on schema data_s.public to role home_manager; -- wouldn't be necessary since it is public schema
grant select on table data_s.public.orders to role home_manager;

grant usage on warehouse compute_wh to role home_manager;

grant role home_manager to user nikolai;

-- Table can be queried and all rows are visible
USE ROLE home_manager;     
SELECT * FROM ORDERS;


-- Policy that role can see everything
USE ROLE accountadmin;
CREATE OR REPLACE ROW ACCESS POLICY category_policy
AS (category varchar) RETURNS BOOLEAN ->
  CASE WHEN 'HOME_MANAGER' = current_role()  and 'Furniture'=category then true
      else false
      end;

-- roles are in ALL CAPS
SELECT current_role();
      
DESC ROW ACCESS POLICY category_policy;

-- Test policy (not working)      
USE ROLE home_manager;     
SELECT * FROM ORDERS;


-- Add policy to table
USE ROLE ACCOUNTADMIN;
ALTER TABLE data_s.public.orders ADD ROW ACCESS POLICY category_policy on (category);

-- Remove policy from table
ALTER TABLE data_s.public.orders DROP ROW ACCESS POLICY category_policy;
ALTER TABLE data_s.public.orders DROP ALL ROW ACCESS POLICIES;

-- Test policy (working)      
USE ROLE home_manager;     
SELECT * FROM ORDERS;


SELECT current_user();

-- Alter policy
USE ROLE ACCOUNTADMIN;
ALTER ROW ACCESS POLICY category_policy
SET BODY -> 
  CASE WHEN 'NIKOLAI' = current_user() and 'Furniture'=category then true
      when  'ELECTRIC_MANAGER' = current_role() and 'Electronics'=category then true
      else false
      end;
