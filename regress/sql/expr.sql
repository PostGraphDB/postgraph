
/*
 * Copyright (C) 2023-2024 PostGraphDB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */ 

LOAD 'postgraph';

CREATE GRAPH expr;
USE GRAPH expr;

--
-- a bunch of comparison operators
--
RETURN 1 = 1.0;
RETURN 1 > -1.0;
RETURN -1.0 < 1;
RETURN 'aaa' < 'z';
RETURN 'z' > 'aaa';
RETURN false = false;
RETURN ('string' < true);
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