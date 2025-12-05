-- Find given number is Even or Odd

SET N=1237;

EXECUTE IMMEDIATE 
$$
BEGIN
    IF($N % 2 = 0) THEN
        RETURN 'Given number is Even Number';
    ELSE
        RETURN 'Given Number is ODD Number';
    END IF;
END;
$$
;

-- Find the TAX amount for given Monthly Salary amount
SET monthly_sal=70000;

EXECUTE IMMEDIATE 
$$
DECLARE
    TAX FLOAT;
    TAX10 FLOAT; TAX20 FLOAT; TAX30 FLOAT;
BEGIN
    IF($monthly_sal * 12 <= 500000) THEN
        TAX := 0;
    ELSEIF(($monthly_sal * 12) between 500001 and 1000000) THEN
        TAX := (($monthly_sal * 12) - 500000) * 10/100;
    ELSEIF(($monthly_sal * 12) between 1000001 and 1500000) THEN
        TAX10 := (1000000 - 500000) * 10/100;
        TAX20 := (($monthly_sal * 12) - 1000000) * 20/100;
        TAX := TAX10 + TAX20;
    ELSE 
        TAX10 := (1000000 - 500000) * 10/100;
        TAX20 := (1500000 - 1000000) * 20/100;
        TAX30 := (($monthly_sal * 12) - 1500000) * 30/100;
        TAX := TAX10 + TAX20 + TAX30;
    END IF;
RETURN 'Calculated Annual Tax is: ' || TAX;
END;
$$
;

-- Find the TAX amount for given Monthly Salary amount
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_CASE_DEMO("MON_GROS_SAL" FLOAT)
RETURNS FLOAT
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
	tax FLOAT;
BEGIN
tax := CASE WHEN (MON_GROS_SAL * 12 <= 500000) THEN 0
		WHEN ((MON_GROS_SAL * 12) between 500001 and 1000000)
		  THEN ((MON_GROS_SAL * 12) - 500000) * 10/100
		WHEN ((MON_GROS_SAL * 12) between 1000001 and 1500000)
		  THEN ((1000000 - 500000) * 10/100) + (((MON_GROS_SAL * 12) - 1000000) * 20/100)
		ELSE
		  ((1000000 - 500000) * 10/100) + ((1500000 - 1000000) * 20/100) + (((MON_GROS_SAL * 12) - 1500000) * 30/100)
		END;

RETURN 'Calculated Annual Tax is: ' || tax;
END;

CALL EMP.PROCS.SP_CASE_DEMO(85000);