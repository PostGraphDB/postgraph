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
SET search_path TO postgraph;

CREATE GRAPH lists;
USE GRAPH lists;

--
-- list literal
--
-- empty list
RETURN [];

-- list of scalar values
RETURN ['str', 1, 1.0, true, null] ;

-- nested lists
RETURN [['str'], [1, [1.0], [[true]]], null] ;

--list concatenation
RETURN ['str', 1, 1.0] + [true, null];

-- IN
RETURN 1 IN ['str', 1, 1.0, true, null];
RETURN 'str' IN ['str', 1, 1.0, true, null];
RETURN 1.0 IN ['str', 1, 1.0, true, null];
RETURN true IN ['str', 1, 1.0, true, null];
RETURN [1,3,5,[2,4,6]] IN ['str', 1, 1.0, true, null, [1,3,5,[2,4,6]]];
RETURN {bool: true, int: 1} IN ['str', 1, 1.0, true, null, {bool: true, int: 1}, [1,3,5,[2,4,6]]];
RETURN null IN ['str', 1, 1.0, true, null];
RETURN null IN ['str', 1, 1.0, true];
RETURN 'str' IN null ;
RETURN 0 IN ['str', 1, 1.0, true, null];
RETURN 1.1 IN ['str', 1, 1.0, true, null];
RETURN 'Str' IN ['str', 1, 1.0, true, null];
RETURN [1,3,5,[2,4,5]] IN ['str', 1, 1.0, true, null, [1,3,5,[2,4,6]]];
RETURN {bool: true, int: 2} IN ['str', 1, 1.0, true, null, {bool: true, int: 1}, [1,3,5,[2,4,6]]];
RETURN null IN 'str' ;
RETURN 'str' IN 'str' ;

-- TODO ALL, ANY/SOME, NONE

-- list access
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][5];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][10];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-3];
-- should return null
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][11];

-- list slice
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][..11];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..0];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][10..10];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..1];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][9..10];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..11];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-3..11];
RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..10];
RETURN 0[0..1];
RETURN 0[[0]..[1]];
RETURN [0][0..-2147483649];

-- access and slice operators nested
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[0] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-1] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][-2] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][-2..] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-1..] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][0] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-1] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-2..-1] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][0] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2][-2..] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2][-2..][0] ;

-- empty list
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-2..-2] ;

-- should return null
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][3] ;
CYPHER WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-2] ;

-- size() of a string
 RETURN size('12345') ;
 RETURN size("1234567890") ;
-- size() of an array
 RETURN size([1, 2, 3, 4, 5]) ;
 RETURN size([]) ;
 RETURN size(null) ;
-- head
 RETURN head([1, 2, 3, 4, 5]) ;
 RETURN head([1]) ;
 RETURN head([]) ;
 RETURN head(null) ;
-- last
 RETURN last([1, 2, 3, 4, 5]) ;
 RETURN last([1]) ;
 RETURN last([]) ;
 RETURN last(null) ;

-- range()
RETURN range(0, 10);
RETURN range(0, 10, 1);
RETURN range(0, -10, -3);
RETURN range(0, 10, 11);
RETURN range(-20, 10, 5);
RETURN range(0, 10, -1);
RETURN range(null, -10, -3);
RETURN range(0, null, -3);
RETURN range(0, -10.0, -3.0);


--
-- Cleanup
--
DROP GRAPH lists CASCADE;

--
-- End of tests
--
