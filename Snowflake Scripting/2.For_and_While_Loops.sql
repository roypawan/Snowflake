-- Code to print stars in triangle shape

EXECUTE IMMEDIATE 
$$
DECLARE
    i INTEGER;
    j INTEGER;
    pattern VARCHAR default '';
BEGIN
    FOR i IN 1 TO 5 
    DO
        FOR j IN 1 TO i
        DO
            pattern := pattern || '*\t';
        END FOR;
        pattern := pattern || '\n';
    END FOR;
RETURN pattern;
END;
$$
;

-- Find prime numbers upto given number
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_WHILE_PRIME_NUMBERS("N" INTEGER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
	i INTEGER default 3;
    j INTEGER;
    flag INTEGER;
    prime VARCHAR default '2 ';
BEGIN
    WHILE (i <= N) 
    DO
        flag := 0;
        FOR j IN 2 TO i-1 
        DO
            IF (i % j = 0) THEN
                flag := 1;
                break;
            END IF;            
        END FOR;
        IF (flag = 0) THEN
            prime := prime || ', ' || i;
        END IF;
    i := i+1;
    END WHILE;  

RETURN prime;                
END;

CALL EMP.PROCS.SP_WHILE_PRIME_NUMBERS(100);

-- Find prime numbers upto given number in the form of a Table.
CREATE OR REPLACE PROCEDURE EMP.PROCS.SP_WHILE_PRIME_NUMBERS("N" INTEGER)
RETURNS TABLE(PRIME INTEGER)
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
	i INTEGER default 2;
    j INTEGER;
    flag INTEGER;
    res RESULTSET;
    prime VARCHAR;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE PRIME_NUMBERS(PRIME INTEGER);
    WHILE (i <= N) 
    DO
        flag := 0;
        FOR j IN 2 TO i-1 
        DO
            IF (i % j = 0 and i <> 2) THEN
                flag := 1;
                break;
            END IF;            
        END FOR;
        IF (flag = 0) THEN
            --prime := prime || ', ' || i;
            INSERT INTO PRIME_NUMBERS VALUES(:i);
        END IF;
    i := i+1;
    END WHILE;
    
    prime := 'SELECT * FROM PRIME_NUMBERS';
    res := (EXECUTE IMMEDIATE :prime);

RETURN TABLE(res); 
END;

CALL EMP.PROCS.SP_WHILE_PRIME_NUMBERS(100);