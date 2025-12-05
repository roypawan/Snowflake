-- Find the Total Salary of Sales department
EXECUTE IMMEDIATE 
$$
DECLARE
    c1 CURSOR for SELECT d.department_name, e.salary FROM EMP.HRDATA.employees e
                    JOIN EMP.HRDATA.departments d ON e.dept_id = d.dept_id
                    WHERE UPPER(d.department_name) = 'SALES';
    dname VARCHAR;
    sal INTEGER;
    tot_sal INTEGER default 0;
    
BEGIN
for rec in c1 do
    dname := rec.department_name;
    sal := rec.salary;
    tot_sal := tot_sal + sal;
end for;

RETURN 'Total salary of '||dname||' is: '||tot_sal;
END;
$$;


-- Find the Total Salary of given department
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_CUR_TOTAL_SALARY("DEPT" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    c1 CURSOR for SELECT d.department_name, e.salary FROM EMP.HRDATA.employees e
                    JOIN EMP.HRDATA.departments d ON e.dept_id = d.dept_id
                    WHERE UPPER(d.department_name) = UPPER(?);
    dname VARCHAR;
    sal INTEGER;
    tot_sal INTEGER default 0;
    
BEGIN

OPEN c1 USING(:DEPT);

for rec in c1 do
    dname := rec.department_name;
    sal := rec.salary;
    tot_sal := tot_sal + sal;
end for;

CLOSE c1;

RETURN 'Total salary of '||dname||' is: '||tot_sal;
END;

CALL EMP.PROCS.SP_CUR_TOTAL_SALARY('Sales');

CALL EMP.PROCS.SP_CUR_TOTAL_SALARY('IT');

CALL EMP.PROCS.SP_CUR_TOTAL_SALARY('maRkeTing');


-- Find top N salaried persons of given department
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_RS_TOP_N_SALARIED("DEPT" VARCHAR, "N" INTEGER)
RETURNS TABLE(VARCHAR, INTEGER)
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    res RESULTSET;
    query VARCHAR;
        
BEGIN

query := 'SELECT first_name||'' ''||last_name as emp_name, salary FROM
            (SELECT e.first_name, e.last_name, e.salary, DENSE_RANK() OVER(PARTITION BY e.dept_id ORDER BY SALARY DESC) as rank
              FROM emp.hrdata.employees e JOIN emp.hrdata.departments d
              ON e.dept_id = d.dept_id
              WHERE UPPER(d.department_name) = UPPER('''||:DEPT||''')
            ) ABC WHERE rank <= '||:N;

res := (EXECUTE IMMEDIATE :query);

RETURN TABLE(res);

END;

CALL EMP.PROCS.SP_RS_TOP_N_SALARIED('Sales', 3);

CALL EMP.PROCS.SP_RS_TOP_N_SALARIED('IT', 4);

CALL EMP.PROCS.SP_RS_TOP_N_SALARIED('finance', 2);