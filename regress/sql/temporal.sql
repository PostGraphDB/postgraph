/*
 * PostGraph
 * Copyright (C) 2023 PostGraphDB
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

LOAD 'postgraph';
SET search_path TO postgraph;
set timezone TO 'GMT';

SELECT * FROM create_graph('temporal');

--
-- Timestamp
--
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '06/23/2023 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN 'Fri Jun 23 13:39:40.00 2023"'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '06/23/1970 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN 0::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN NULL::timestamp
$$) AS r(c gtype);

--
-- interval
--  
SELECT * FROM cypher('temporal', $$
RETURN '1997-12-17 07:37:16-08'::timestamp
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
RETURN '12/17/1997 07:37:16.00'::timestamp
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
RETURN 'Wed Dec 17 07:37:16 1997'::timestamp
$$) AS r(result gtype);

--
-- timestamptz
--
SELECT * FROM cypher('temporal', $$
RETURN '1997-12-17 07:37:16-06'::timestamptz
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
RETURN '12/17/1997 07:37:16.00+00'::timestamptz
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
RETURN 'Wed Dec 17 07:37:16 1997+09'::timestamptz
$$) AS r(result gtype);

--
-- date
--
SELECT * FROM cypher('temporal', $$
RETURN '1997-12-17'::date
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
RETURN '12/17/1997'::date
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
RETURN 'Wed Dec 17 1997'::date
$$) AS r(result gtype);

--
-- time
--
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16-08'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16'::time $$) AS r(result gtype);

--
-- timetz
--
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16-08'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '15 Minutes'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '10 Hours'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '40 Days'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '10 Weeks'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '10 Months'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '3 Years'::interval $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds Ago'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '15 Minutes Ago'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '10 Hours Ago'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '40 Days Ago'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '10 Weeks Ago'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '10 Months Ago'::interval $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '3 Years Ago'::interval $$) AS r(result gtype);

--
-- toTimestamp()
--
SELECT * FROM cypher('temporal', $$
    RETURN toTimestamp('12/17/1997 07:37:16.00+00'::timestamptz)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTimestamp('12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTimestamp('12/17/1997'::date)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTimestamp(100000000000)
$$) AS r(result gtype);

--
-- Postgres Timestamp to GType
--
SELECT '12/17/1997 07:37:16.00+00'::timestamp::gtype;

--
-- toTimestampTz()
--
SELECT * FROM cypher('temporal', $$
    RETURN toTimestampTz('12/17/1997 07:37:16.00+00'::timestamp)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTimestampTz('12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTimestampTz('12/17/1997'::date)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTimestampTz(100000000000)
$$) AS r(result gtype);

--
-- Postgres Timestamp to GType
--
SELECT '12/17/1997 07:37:16.00+08'::timestamptz::gtype;

--
-- toDate()
--
SELECT * FROM cypher('temporal', $$
    RETURN toDate('12/17/1997 07:37:16.00+00'::timestamp)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toDate('12/17/1997 07:37:16.00+00'::timestamptz)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toDate('12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toDate('12/17/1997'::date)
$$) AS r(result gtype);

--
-- Postgres Date to GType
--
SELECT '12/17/1997'::date::gtype;

--
-- toTime()
--
SELECT * FROM cypher('temporal', $$
    RETURN toTime('12/17/1997 07:37:16.00+00'::timestamp)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTime('12/17/1997 07:37:16.00+00'::timestamptz)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTime('07:37:16.00+00')
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTime('07:37:16.00+00'::timetz)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTime('7 Hours 37 Minutes 16 Seconds'::interval)
$$) AS r(result gtype);

--
-- Postgres Time to GType
--
SELECT '07:37:16.00'::time::gtype;


--
-- toTimeTz()
--
SELECT * FROM cypher('temporal', $$
    RETURN toTimeTz('12/17/1997 07:37:16.00+00'::timestamptz)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ 
    RETURN toTimeTz('07:37:16.00+00')
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ 
    RETURN toTimeTz('07:37:16.00+00'::timetz)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN toTimeTz('07:37:16.00+00'::time)
$$) AS r(result gtype);

--
-- Postgres Time to GType
--
SELECT '07:37:16.00+08'::timetz::gtype;

--
-- Current Temporal Keywords
--
SELECT result = CURRENT_TIMESTAMP::gtype FROM cypher('temporal', $$
    RETURN CURRENT_TIMESTAMP
$$) AS r(result gtype);

SELECT result = CURRENT_TIMESTAMP(1)::gtype FROM cypher('temporal', $$
    RETURN CURRENT_TIMESTAMP(1)
$$) AS r(result gtype);

SELECT result = CURRENT_DATE::gtype FROM cypher('temporal', $$
    RETURN CURRENT_DATE
$$) AS r(result gtype);

SELECT result = CURRENT_TIME::gtype FROM cypher('temporal', $$
    RETURN CURRENT_TIME
$$) AS r(result gtype);

SELECT result = CURRENT_TIME(1)::gtype FROM cypher('temporal', $$
    RETURN CURRENT_TIME(1)
$$) AS r(result gtype);

--
-- Local Temporal Keywords
--
SELECT result = LOCALTIMESTAMP::gtype FROM cypher('temporal', $$ RETURN LOCALTIMESTAMP $$) AS r(result gtype);
SELECT result = LOCALTIMESTAMP(1)::gtype FROM cypher('temporal', $$ RETURN LOCALTIMESTAMP(1) $$) AS r(result gtype);
SELECT result = LOCALTIME::gtype FROM cypher('temporal', $$ RETURN LOCALTIME $$) AS r(result gtype);
SELECT result = LOCALTIME(1)::gtype FROM cypher('temporal', $$ RETURN LOCALTIME(1) $$) AS r(result gtype);

SELECT result = now()::gtype FROM cypher('temporal', $$ RETURN now() $$) AS r(result gtype);
SELECT result = transaction_timestamp()::gtype FROM cypher('temporal', $$ RETURN transaction_timestamp() $$) AS r(result gtype);
SELECT result = statement_timestamp()::gtype FROM cypher('temporal', $$ RETURN statement_timestamp() $$) AS r(result gtype);
SELECT 1 FROM cypher('temporal', $$ RETURN clock_timestamp() $$) AS r(result gtype);
SELECT 1 FROM cypher('temporal', $$ RETURN timeofday() $$) AS r(result gtype);

--
-- Timestamp Comparison
--
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);


SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

--
-- Timestamp With Timezone Comparison
--
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);


SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);


--
-- Timestamp With Timezone Timestamp Comparison
--
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);


SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-07-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-05-23 13:39:40.00'::timestamp
$$) AS r(c gtype);

--
-- Timestamp With Timezone Comparison
--
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);


SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-06-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-07-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-05-23 13:39:40.00'::timestamptz
$$) AS r(c gtype);


--
-- date comparison
--
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date = '1997-12-17'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date = '1997-12-16'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date = '1997-12-18'::date $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date <> '1997-12-17'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date <> '1997-12-16'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date <> '1997-12-18'::date $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date > '1997-12-17'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date > '1997-12-16'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date > '1997-12-18'::date $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date < '1997-12-17'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date < '1997-12-16'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date < '1997-12-18'::date $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date >= '1997-12-17'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date >= '1997-12-16'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date >= '1997-12-18'::date $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date <= '1997-12-17'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date <= '1997-12-16'::date $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '1997-12-17'::date <= '1997-12-18'::date $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
RETURN '2023-06-23 0:0:00.00'::timestamp = '2023-06-23'::date
$$) AS r(c gtype);
--
-- Timestamp and Date Comparison
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamp = '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamp <> '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamp > '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamp < '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamp >= '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamp <= '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date = '2023-06-23 0:0:00.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date = '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date = '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date <> '2023-06-23 0:0:00.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date <> '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date <> '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date > '2023-06-23 0:0:00.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date > '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date > '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date < '2023-06-23 0:0:00.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date < '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date < '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date >= '2023-06-23 0:0:00.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date >= '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date >= '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date <= '2023-06-23 0:0:00.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date <= '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date <= '2023-06-23 13:39:40.00'::timestamp $$) AS r(c gtype);

--
-- Timestamp With TimeZone and Date Comparison
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamptz = '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamptz <> '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamptz > '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamptz < '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamptz >= '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 0:0:00.00'::timestamptz <= '2023-06-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-07-23'::date $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-05-23'::date $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date = '2023-06-23 0:0:00.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date = '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date = '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date <> '2023-06-23 0:0:00.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date <> '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date <> '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date > '2023-06-23 0:0:00.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date > '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date > '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date < '2023-06-23 0:0:00.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date < '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date < '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date >= '2023-06-23 0:0:00.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date >= '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date >= '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);

SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date <= '2023-06-23 0:0:00.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-07-23'::date <= '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '2023-05-23'::date <= '2023-06-23 13:39:40.00'::timestamptz $$) AS r(c gtype);


--
-- Time Comparison
--
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time = '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time = '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time = '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <> '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <> '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <> '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time > '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time > '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time > '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time < '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time < '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time < '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time >= '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time >= '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time >= '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <= '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <= '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <= '08:37:16.00'::time $$) AS r(result gtype);

--
-- Time With Timezone Comparison
--
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz = '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz = '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz = '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <> '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <> '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <> '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz > '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz > '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz > '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz < '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz < '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz < '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz >= '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz >= '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz >= '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <= '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <= '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <= '08:37:16.00'::timetz $$) AS r(result gtype);


SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz = '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz = '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz = '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <> '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <> '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <> '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz > '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz > '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz > '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz < '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz < '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz < '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz >= '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz >= '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz >= '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <= '07:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <= '06:37:16.00'::time $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::timetz <= '08:37:16.00'::time $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time = '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time = '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time = '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <> '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <> '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <> '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time > '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time > '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time > '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time < '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time < '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time < '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time >= '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time >= '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time >= '08:37:16.00'::timetz $$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <= '07:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <= '06:37:16.00'::timetz $$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '07:37:16.00'::time <= '08:37:16.00'::timetz $$) AS r(result gtype);

--
-- Interval Comparison
--
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval = '30 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval = '20 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval = '40 Seconds'::interval$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval <> '30 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval <> '20 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval <> '40 Seconds'::interval$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval > '30 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval > '20 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval > '40 Seconds'::interval$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval < '30 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval < '20 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval < '40 Seconds'::interval$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval >= '30 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval >= '20 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval >= '40 Seconds'::interval$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval <= '30 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval <= '20 Seconds'::interval$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval <= '40 Seconds'::interval$$) AS r(result gtype);


--
-- Timestamp + Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp + '10 Days'::interval $$) AS r(c gtype);

--
-- Timestamp With Timezone + Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz + '10 Days'::interval $$) AS r(c gtype);

--
-- Date + Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date + '10 Days'::interval $$) AS r(c gtype);

--
-- Time + Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '13:39:40.00'::time + '8 Hours'::interval $$) AS r(c gtype);

--
-- Time With Timezone + Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '13:39:40.00'::timetz + '8 Hours'::interval $$) AS r(c gtype);

--
-- Interval + Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '10 Days'::interval + '8 Hours'::interval $$) AS r(c gtype);

--
-- Timestamp - Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamp - '10 Days'::interval $$) AS r(c gtype);

--
-- Timestamp With Timezone - Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23 13:39:40.00'::timestamptz - '10 Days'::interval $$) AS r(c gtype);

--
-- Date - Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '2023-06-23'::date - '10 Days'::interval $$) AS r(c gtype);

--
-- Time - Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '13:39:40.00'::time - '8 Hours'::interval $$) AS r(c gtype);

--
-- Time With Timezone - Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '13:39:40.00'::timetz - '8 Hours'::interval $$) AS r(c gtype);

--
-- Interval - Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '10 Days'::interval - '8 Hours'::interval $$) AS r(c gtype);

--
-- - Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN - '8 Hours'::interval $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN - '8 Hours Ago'::interval $$) AS r(c gtype);


--
-- * Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '8 Hours'::interval * 8.0 $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '8 Hours'::interval * 8 $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN 8 * '8 Hours'::interval $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN 8.0 * '8 Hours'::interval $$) AS r(c gtype);

--
-- / Interval Operator
--
SELECT * FROM cypher('temporal', $$ RETURN '8 Hours'::interval / 8.0 $$) AS r(c gtype);
SELECT * FROM cypher('temporal', $$ RETURN '8 Hours'::interval / 8 $$) AS r(c gtype);


SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(day FROM TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(day FROM TIMESTAMP WITHOUT TIME ZONE '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(day FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(hour FROM TIME '07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(hour FROM TIME WITH TIME ZONE '07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(hour FROM TIME WITHOUT TIME ZONE '07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(day FROM DATE '12/17/1997')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
	RETURN EXTRACT(day FROM INTERVAL '6 Years 11 Months 24 Days 5 Hours 23 Minutes')
$$) AS r(result gtype);


SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(CENTURY FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(DECADE FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(DOW FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(EPOCH FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(ISODOW FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(ISOYEAR FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(JULIAN FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(MICROSECONDS FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(MILLISECONDS FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(ISOYEAR FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(MINUTE FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(MONTH FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(QUARTER FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(SECOND FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(TIMEZONE FROM TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN EXTRACT(YEAR FROM TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);


SELECT * FROM cypher('temporal', $$
    RETURN date_part('day', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('day', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN date_part('hour', TIME '07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('hour', TIME WITH TIME ZONE '07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('day', DATE '12/17/1997')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
        RETURN date_part('day', INTERVAL '6 Years 11 Months 24 Days 5 Hours 23 Minutes')
$$) AS r(result gtype);


SELECT * FROM cypher('temporal', $$
    RETURN date_part('CENTURY', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('DECADE', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('DOW', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('EPOCH', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('ISODOW', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('ISOYEAR', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('JULIAN', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('MICROSECONDS', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('MILLISECONDS', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('ISOYEAR', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('MINUTE', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('MONTH', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('QUARTER', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('SECOND', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('TIMEZONE', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_part('YEAR', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

--
-- date_bin
--
SELECT * FROM cypher('temporal', $$
    RETURN date_bin(INTERVAL '15 minutes', TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_bin(INTERVAL '15 minutes', TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01')
$$) AS r(result gtype);


SELECT * FROM cypher('temporal', $$
    RETURN date_bin('15 minutes'::interval, '2020-02-11 15:44:17+08'::timestamptz, '2001-01-01'::timestamptz)
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_bin('15 minutes'::interval, '2020-02-11 15:44:17+08'::timestamp, '2001-01-01'::date)
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_bin('15 minutes'::interval, '2001-01-01'::date, '2020-02-11 15:44:17+08'::timestamp)
$$) AS r(result gtype);



--
-- date_trunc
--
SELECT * FROM cypher('temporal', $$
    RETURN date_trunc('day', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_trunc('day', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00', 'Australia/Sydney')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
    RETURN date_trunc('day', TIMESTAMP '12/17/1997 07:37:16.00+00')
$$) AS r(result gtype);

SELECT * FROM cypher('temporal', $$
        RETURN date_trunc('day', INTERVAL '6 Years 11 Months 24 Days 5 Hours 23 Minutes')
$$) AS r(result gtype);



--
-- Temporal Functions
--
SELECT * FROM cypher('temporal', $$
    RETURN age('12/17/1997 07:37:16.00+00'::timestamptz, '6/12/2007 12:45:19.89+00'::timestamptz)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN age('12/17/1997 07:37:16.00+00'::timestamp, '6/12/2007 12:45:19.89+00'::timestamp)
$$) AS r(result gtype);
SELECT * FROM cypher('temporal', $$
    RETURN age('12/17/1997 07:37:16.00+00'::timestamptz, '6/12/2007 12:45:19.89+00'::timestamp)
$$) AS r(result gtype);

--
-- Overlap
--

-- date date date date
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'2001-10-29'::date) overlaps ('2001-10-30'::date,'2002-10-30'::date)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'2001-10-31'::date) overlaps ('2001-10-30'::date,'2002-10-30'::date)
$$) as (result gtype);

-- date interval date interval
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'10 days'::interval) overlaps ('2001-10-30'::date,'10 days'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'365 days'::interval) overlaps ('2001-10-30'::date,'10 days'::interval)
$$) as (result gtype);

-- date interval date date
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'10 days'::interval) overlaps ('2001-10-30'::date,'2001-11-09'::date)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'365 days'::interval) overlaps ('2001-10-30'::date,'2001-11-09'::date)
$$) as (result gtype);

-- date date date interval
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'2001-10-28'::date) overlaps ('2001-10-30'::date,'-2 days'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::date,'2001-10-28'::date) overlaps ('2001-10-30'::date,'-3 days'::interval)
$$) as (result gtype);

-- timestamp timestamp timestamp timestamp
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'2001-10-29'::timestamp) overlaps ('2001-10-30'::timestamp,'2002-10-30'::timestamp)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'2001-10-31'::timestamp) overlaps ('2001-10-30'::timestamp,'2002-10-30'::timestamp)
$$) as (result gtype);

-- timestamp interval timestamp interval
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'10 days'::interval) overlaps ('2001-10-30'::timestamp,'10 days'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'365 days'::interval) overlaps ('2001-10-30'::timestamp,'10 days'::interval)
$$) as (result gtype);

-- timestamp interval timestamp timestamp
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'10 days'::interval) overlaps ('2001-10-30'::timestamp,'2001-11-09'::timestamp)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'365 days'::interval) overlaps ('2001-10-30'::timestamp,'2001-11-09'::timestamp)
$$) as (result gtype);

-- timestamp timestamp timestamp interval
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'2001-10-28'::timestamp) overlaps ('2001-10-30'::timestamp,'-2 days'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('2001-02-16'::timestamp,'2001-10-28'::timestamp) overlaps ('2001-10-30'::timestamp,'-3 days'::interval)
$$) as (result gtype);

-- time time time time
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'21:00:00'::time) overlaps ('21:00:00'::time,'22:00:00'::time)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'21:01:00'::time) overlaps ('21:00:00'::time,'22:00:00'::time)
$$) as (result gtype);

-- time interval time interval
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'10 minutes'::interval) overlaps ('20:10:00'::time,'10 minutes'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'11 minutes'::interval) overlaps ('20:10:00'::time,'10 minutes'::interval)
$$) as (result gtype);

-- time interval time time
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'10 minutes'::interval) overlaps ('20:10:00'::time,'20:20:00'::time)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'11 minutes'::interval) overlaps ('20:10:00'::time,'20:20:00'::time)
$$) as (result gtype);

-- time time time interval
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'20:08:00'::time) overlaps ('20:10:00'::time,'-2 minutes'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00'::time,'20:08:00'::time) overlaps ('20:10:00'::time,'-3 minutes'::interval)
$$) as (result gtype);

-- timetz timetz timetz timetz
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'21:00:00+00'::timetz) overlaps ('21:00:00+00'::timetz,'22:00:00+00'::timetz)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'21:01:00+00'::timetz) overlaps ('21:00:00+00'::timetz,'22:00:00+00'::timetz)
$$) as (result gtype);

-- timetz interval timetz interval
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'10 minutes'::interval) overlaps ('20:10:00+00'::timetz,'10 minutes'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'11 minutes'::interval) overlaps ('20:10:00+00'::timetz,'10 minutes'::interval)
$$) as (result gtype);

-- timetz interval timetz timetz
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'10 minutes'::interval) overlaps ('20:10:00+00'::timetz,'20:20:00+00'::timetz)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'11 minutes'::interval) overlaps ('20:10:00+00'::timetz,'20:20:00+00'::timetz)
$$) as (result gtype);

-- timetz timetz timetz interval
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'20:08:00+00'::timetz) overlaps ('20:10:00+00'::timetz,'-2 minutes'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN ('20:00:00+00'::timetz,'20:08:00+00'::timetz) overlaps ('20:10:00+00'::timetz,'-3 minutes'::interval)
$$) as (result gtype);

--
-- justify_interval
--
select * from cypher('temporal',$$
    RETURN justify_interval('1 month -1 hours'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_interval('1 month 33 days 1 hours'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_interval('1 week 6 days 27 hours'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_interval('27 hours'::interval)
$$) as (result gtype);

--
-- justify_days
--
select * from cypher('temporal',$$
    RETURN justify_days('5 weeks'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_days('35 days'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_days('4 weeks 8 days'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_days('1 month 5 weeks'::interval)
$$) as (result gtype);

--
-- justify_hours
--
select * from cypher('temporal',$$
    RETURN justify_hours('27 hours'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_hours('1 week 27 hours'::interval)
$$) as (result gtype);
select * from cypher('temporal',$$
    RETURN justify_hours('2 days 30 hours'::interval)
$$) as (result gtype);

--
-- isfinite
--
select * from cypher('temporal',$$ 
    RETURN isfinite('infinity'::date)
$$) as (result gtype);
select * from cypher('temporal',$$ 
    RETURN isfinite('2001-02-16'::date)
$$) as (result gtype);
select * from cypher('temporal',$$ 
    RETURN isfinite('2001-02-16 23:40:00'::timestamp)
$$) as (result gtype);
select * from cypher('temporal',$$ 
    RETURN isfinite('infinity'::timestamp)
$$) as (result gtype);
select * from cypher('temporal',$$ 
    RETURN isfinite('15 minutes'::interval)
$$) as (result gtype);

--
-- Temporal Make Functions
--
SELECT * from cypher('temporal', $$RETURN make_date(0, 7, 15)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_date(2013, 2, 30)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_date(2013, 13, 1)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_date(-44, 3, 15)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_date(2013, 11, -1)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_date(2013, 7, 15)$$) as (g gtype);

SELECT * from cypher('temporal', $$RETURN make_time(8, 20, 0.0)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_time(10, 55, 100.1)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_time(24, 0, 2.1)$$) as (g gtype);

SELECT * from cypher('temporal', $$RETURN make_timestamp(2023, 2, 14, 5, 30, 0.0)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_timestamptz(2023, 2, 14, 5, 30, 0.0)$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_timestamptz(2023, 2, 14, 5, 30, 0.0, 'KST')$$) as (g gtype);
SELECT * from cypher('temporal', $$RETURN make_timestamptz(2023, 2, 14, 5, 30, 0.0, 'GMT')$$) as (g gtype);


--
-- Typeasting
--
--Interval
SELECT * FROM cypher('temporal', $$ RETURN '30 Seconds'::interval $$) AS r(result interval);
--timestamp
SELECT * FROM cypher('temporal', $$ RETURN TIMESTAMP '2020-02-11 15:44:17' $$) AS r(result timestamp);
SELECT * FROM cypher('temporal', $$ RETURN '2020-02-11 15:44:17' $$) AS r(result timestamp);

--
-- Clean up
--
SELECT * FROM drop_graph('temporal', true);
