/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

SET extra_float_digits = 0;
LOAD 'postgraph';
SET search_path TO postgraph;
set timezone TO 'GMT';

--
-- avg(), sum(), count(), & count(*)
--
SELECT create_graph('UCSC');

SELECT * FROM cypher('UCSC', $$ CREATE (:students {name: "Jack", gpa: 3.0, age: 21, zip: 94110}) $$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ CREATE (:students {name: "Jill", gpa: 3.5, age: 27, zip: 95060}) $$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ CREATE (:students {name: "Jim", gpa: 3.75, age: 32, zip: 96062}) $$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ CREATE (:students {name: "Rick", gpa: 2.5, age: 24, zip: "95060"}) $$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ CREATE (:students {name: "Ann", gpa: 3.8::numeric, age: 23}) $$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ CREATE (:students {name: "Derek", gpa: 4.0, age: 19, zip: 90210}) $$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ CREATE (:students {name: "Jessica", gpa: 3.9::numeric, age: 20}) $$) AS (a gtype);

SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN corr(u.gpa, u.age) $$) AS (corr gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN covar_pop(u.gpa, u.age) $$) AS (covar_pop gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN covar_samp(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_sxx(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_syy(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_sxy(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_slope(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_intercept(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_avgx(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_avgy(u.gpa, u.age) $$) AS (covar_samp gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN regr_r2(u.gpa, u.age) $$) AS (covar_samp gtype);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN avg(u.gpa),
           sum(u.gpa),
	   sum(u.gpa)/count(u.gpa),
	   count(u.gpa),
	   count(*)
$$) AS (avg gtype, sum gtype, sum_divided_by_count gtype, count gtype, count_star gtype);

SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Dave", age: 24})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Mike", age: 18})$$) AS (a gtype);

SELECT * FROM cypher('UCSC', $$ 
    MATCH (u) RETURN (u)
$$) AS (vertex vertex);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN avg(u.gpa),
           sum(u.gpa),
	   sum(u.gpa) / count(u.gpa),
	   count(u.gpa),
	   count(*)
$$) AS (avg gtype, sum gtype, sum_divided_by_count gtype, count gtype, count_star gtype);

-- should return null
SELECT * FROM cypher('UCSC', $$ RETURN avg(NULL) $$) AS (avg gtype);
SELECT * FROM cypher('UCSC', $$ RETURN sum(NULL) $$) AS (sum gtype);
SELECT * FROM cypher('UCSC', $$ RETURN count(NULL) $$) AS (count gtype);


--
-- min() & max()
--
SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN min(u.gpa),
           max(u.gpa),
	   count(u.gpa),
	   count(*)
$$) AS (min gtype, max gtype, count gtype, count_star gtype);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN min(u.gpa),
           max(u.gpa),
	   count(u.gpa),
	   count(*) 
$$) AS (min gtype, max gtype, count gtype, count_star gtype);

SELECT * FROM cypher('UCSC', $$ 
    MATCH (u)
    RETURN min(u.name),
           max(u.name), 
           count(u.name),
           count(*) 
$$) AS (min gtype, max gtype, count gtype, count_star gtype);

SELECT * FROM cypher('UCSC', $$
     MATCH (u)
     RETURN min(u.zip),
            max(u.zip),
	    count(u.zip),
	    count(*)
$$) AS (min gtype, max gtype, count gtype, count_star gtype);

-- should return null
SELECT * FROM cypher('UCSC', $$
    RETURN min(NULL)
$$) AS (min gtype);

SELECT * FROM cypher('UCSC', $$
    RETURN max(NULL)
$$) AS (max gtype);

--
-- stDev() & stDevP()
--
SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN stDev(u.gpa),
           stDevP(u.gpa)
$$) AS (stDev gtype, stDevP gtype);

-- should return 0
SELECT * FROM cypher('UCSC', $$ RETURN stDev(NULL) $$) AS (stDev gtype);
SELECT * FROM cypher('UCSC', $$ RETURN stDevP(NULL) $$) AS (stDevP gtype);

--
-- percentileCont() & percentileDisc()
--
SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN percentileCont(u.gpa, .55),
           percentileDisc(u.gpa, .55),
	   percentileCont(u.gpa, .9),
	   percentileDisc(u.gpa, .9)
$$) AS (percentileCont1 gtype, percentileDisc1 gtype, percentileCont2 gtype, percentileDisc2 gtype);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN percentileCont(u.gpa, .55)
$$) AS (percentileCont gtype);

SELECT * FROM cypher('UCSC', $$
    MATCH (u) RETURN percentileDisc(u.gpa, .55)
$$) AS (percentileDisc gtype);

--
-- collect()
--
SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN collect(u.name),
           collect(u.age),
	   collect(u.gpa),
	   collect(u.zip)
$$) AS (name gtype, age gtype, gqa gtype, zip gtype);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN collect(u.gpa),
           collect(u.gpa)
$$) AS (gpa1 gtype, gpa2 gtype);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN collect(u.zip),
           collect(u.zip)
$$) AS (zip1 gtype, zip2 gtype);

SELECT * FROM cypher('UCSC', $$ RETURN collect(5) $$) AS (result gtype);

-- should return an empty aray
SELECT * FROM cypher('UCSC', $$
     MATCH (u)
     WHERE u.name =~ "doesn't exist"
     RETURN collect(u.name)
$$) AS (name gtype);

--
-- DISTINCT inside aggregate functions
--
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Sven", gpa: 3.2, age: 27, zip: 94110})$$) AS (a gtype);

SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN (u) $$) AS (vertex vertex);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN count(u.zip),
           count(DISTINCT u.zip)
$$) AS (zip gtype, distinct_zip gtype);

SELECT * FROM cypher('UCSC', $$
    MATCH (u)
    RETURN count(u.age),
           count(DISTINCT u.age)
$$) AS (age gtype, distinct_age gtype);

-- test AUTO GROUP BY for aggregate functions
SELECT create_graph('group_by');
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 1, j: 2, k:3})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 1, j: 2, k:4})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 1, j: 3, k:5})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 2, j: 3, k:6})$$) AS (result gtype);

SELECT * FROM cypher('group_by', $$
    MATCH (u:row) RETURN u.i, u.j, u.k
$$) AS (i gtype, j gtype, k gtype);

SELECT * FROM cypher('group_by', $$
    MATCH (u:row) RETURN u.i, u.j, sum(u.k)
$$) AS (i gtype, j gtype, sumk gtype);

SELECT * FROM cypher('group_by', $$CREATE (:L {a: 1, b: 2, c:3})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:L {a: 2, b: 3, c:1})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:L {a: 3, b: 1, c:2})$$) AS (result gtype);

/*
 * TODO: Get the link from the opencypher website.
 */
SELECT * FROM cypher('group_by', $$
    MATCH (x:L)
    RETURN x.a, x.b, x.c,
           x.a + count(*) + x.b + count(*) + x.c
$$)  AS (a gtype, b gtype, c gtype, result gtype);

SELECT * FROM cypher('group_by', $$
    MATCH (x:L)
    RETURN x.a + x.b + x.c,
           x.a + x.b + x.c + count(*) + count(*)
$$) AS (a_b_c gtype,  result gtype);

-- with WITH clause
SELECT * FROM cypher('group_by', $$
    MATCH (x:L)
    WITH x, count(x) AS cnt
    RETURN x.a + x.b + x.c + cnt$$) AS (result gtype);

SELECT * FROM cypher('group_by', $$
    MATCH(x:L)
    WITH x, count(x) AS cnt RETURN x.a + x.b + x.c + cnt + cnt
$$) AS (result gtype);

SELECT * FROM cypher('group_by', $$
    MATCH(x:L)
    WITH x.a + x.b + x.c AS v, count(x) as cnt
    RETURN v + cnt + cnt
$$) AS (result gtype);


SELECT * FROM cypher('group_by', $$
     MATCH (x:L)
     RETURN x.a, x.a + count(*) + x.b + count(*) + x.c
     GROUP BY x.a, x.b, x.c
$$) AS (a gtype, result gtype);

SELECT * FROM cypher('group_by', $$
     MATCH (x:L)
     RETURN x.a + count(*) + x.b + count(*) + x.c
     GROUP BY x.a, x.b, x.c
$$) AS (result gtype);


SELECT * FROM cypher('group_by', $$
     MATCH (x:L)
     RETURN x.a + x.b + x.c + count(*) + count(*)
     GROUP BY x.a + x.b + x.c
$$) AS (result gtype);



SELECT * FROM cypher('group_by', $$
     MATCH (x:L)
     RETURN x.a + x.b + x.c + count(*) + count(*)
$$) AS (result gtype);


SELECT * FROM cypher('group_by', $$
     MATCH (x:L)
     WITH x.a + count(*) + x.b + count(*) + x.c as cnt
     GROUP BY x.a, x.b, x.c
     RETURN cnt
$$) AS (cnt gtype);


SELECT * FROM cypher('group_by', $$
     MATCH (x:L)
     WITH x.a + x.b + x.c + count(*) + count(*) as cnt
     GROUP BY x.a + x.b + x.c 
     RETURN cnt
$$) AS (cnt gtype);


-- should fail
SELECT * FROM cypher('group_by', $$
     MATCH (x:L)
     RETURN x.a, x.a + count(*) + x.b + count(*) + x.c
$$) AS (a gtype, result gtype);

SELECT * FROM cypher('group_by', $$
    MATCH (x:L)
    RETURN x.a + count(*) + x.b + count(*) + x.c
$$) AS (result gtype);


SELECT * FROM cypher('group_by', $$
    MATCH (x)
    RETURN x.i, x.j, x.k, COUNT(*)
    GROUP BY ROLLUP (x.i, x.j, x.k)
$$) AS (i int, j int, k int, cnt int);

SELECT * FROM cypher('group_by', $$
    MATCH (x)
    RETURN x.i, x.j, x.k, COUNT(*)
    GROUP BY CUBE (x.i, x.j, x.k)
$$) AS (i int, j int, k int, cnt int);


SELECT * FROM cypher('group_by', $$
    MATCH (x)
    WITH count(x.i) AS cnt GROUP BY x.i
    RETURN cnt
$$) AS (i int);

SELECT * FROM cypher('group_by', $$
    MATCH (x)
    WITH count(x.i) AS cnt GROUP BY x.i HAVING count(x.i) > 1
    RETURN cnt
$$) AS (i int);

SELECT * FROM cypher('group_by', $$
    MATCH (x)
    WITH x, row_number() OVER (PARTITION BY x.i ORDER BY COALESCE(x.j, x.c) ) AS row_num
    RETURN x, row_num
$$) AS (x vertex, i int); 


SELECT * FROM cypher('group_by', $$
    MATCH (x)
    WITH x, row_number() OVER w AS row_num WINDOW w AS (PARTITION BY x.i ORDER BY COALESCE(x.j, x.c) )
    RETURN x, row_num
$$) AS (x vertex, i int);

SELECT create_graph('edge_aggregates');

SELECT * FROM cypher('edge_aggregates', $$CREATE ()-[:e]->() $$) AS (result gtype);
SELECT * FROM cypher('edge_aggregates', $$CREATE ()-[:e]->() $$) AS (result gtype);
SELECT * FROM cypher('edge_aggregates', $$CREATE ()-[:e]->() $$) AS (result gtype);

SELECT * FROM cypher('edge_aggregates', $$
    MATCH ()-[e]->()
    RETURN collect(e)
$$) AS (result edge[]);

SELECT * FROM cypher('edge_aggregates', $$
    MATCH ()-[e]->()
    RETURN collect(e, 2)
$$) AS (result edge[]);

SELECT * FROM cypher('group_by', $$MATCH () RETURN COUNT(*) $$) AS (result gtype);
--
-- Cleanup
--
SELECT drop_graph('edge_aggregates', true);
SELECT drop_graph('group_by', true);
SELECT drop_graph('UCSC', true);

--
-- End of tests
--
