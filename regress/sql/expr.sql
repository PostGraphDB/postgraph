
LOAD 'postgraph';

CREATE GRAPH expr;
USE GRAPH expr;

--
-- a bunch of comparison operators
--
RETURN 1 = 1.0;
RETURN 1 > -1.0;
RETURN -1.0 < 1;
RETURN "aaa" < "z";
RETURN "z" > "aaa";
RETURN false = false;
RETURN ("string" < true);
RETURN true < 1;
RETURN (1 + 1.0) = (7 % 5);

-- IS NULL
RETURN null IS NULL;
RETURN 1 IS NULL;
-- IS NOT NULL
RETURN 1 IS NOT NULL;
RETURN null IS NOT NULL;
-- NOT
RETURN NOT false;
RETURN NOT true;
-- AND
RETURN true AND true;
RETURN true AND false;
RETURN false AND true;
RETURN false AND false;
-- OR
RETURN true OR true;
RETURN true OR false;
RETURN false OR true;
RETURN false OR false;
-- The ONE test on operator precedence...
RETURN NOT ((true OR false) AND (false OR true));
-- XOR
RETURN true XOR true;
RETURN true XOR false;
RETURN false XOR true;
RETURN false XOR false;

DROP GRAPH expr;