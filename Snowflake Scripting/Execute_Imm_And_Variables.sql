-- Variable Declaration and Assignment

EXECUTE IMMEDIATE 
$$
    DECLARE
        First_Name VARCHAR default 'This is';
		Last_Name VARCHAR;
		Full_Name STRING;
    BEGIN
        LET Middle_Name := ' ';
        Last_Name := 'Snowflake Scripting';
		
		Full_Name := First_Name || Middle_Name || Last_Name;        
		-- SELECT :First_Name || :Middle_Name || :Last_Name into :Full_Name;
        
		RETURN Full_Name;
    END;
$$
;

EXECUTE IMMEDIATE $$
    DECLARE
        profit number(38, 2) DEFAULT 0.0;
    BEGIN
        LET cost number(38, 2) := 100.0;
        LET revenue number(38, 2) DEFAULT 110.0;

        profit := revenue - cost;
        RETURN profit;
    END;
$$
;

-- Session Level Variables
SET A=60;
SET (B, C) = (220, 10);
SET Age = 'My Age is ';

EXECUTE IMMEDIATE 
$$
    DECLARE
        D Integer;
        
    BEGIN
        D := ($A + $B) / $C;        
		RETURN $Age || D;
    END;
$$
;

SET (A, B, place) = (30, 52, 'Hyderabad');

EXECUTE IMMEDIATE 
$$
    BEGIN
    RETURN $place || ' - ' || ($A+$B);
    
    END;
$$;

-- Session Variables can't be modified
SET A=60;
EXECUTE IMMEDIATE 
$$
    BEGIN
        A := 100;
        --$A := 100;
		RETURN $A;
    END;
$$;

