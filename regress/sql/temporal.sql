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

CREATE GRAPH temporal;
USE GRAPH temporal;

--
-- Basic I/O
--
-- Timestamp
 RETURN '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '06/23/2023 13:39:40.00'::timestamp ;
 RETURN 'Fri Jun 23 13:39:40.00 2023"'::timestamp ;
 RETURN '06/23/1970 13:39:40.00'::timestamp ;
 RETURN 0::timestamp ;
 RETURN NULL::timestamp ;
 RETURN '1997-12-17 07:37:16-08'::timestamp ;
 RETURN '12/17/1997 07:37:16.00'::timestamp ;
 RETURN 'Wed Dec 17 07:37:16 1997'::timestamp ;
-- timestamptz
 RETURN '1997-12-17 07:37:16-06'::timestamptz ;
 RETURN '12/17/1997 07:37:16.00+00'::timestamptz ;
 RETURN 'Wed Dec 17 07:37:16 1997+09'::timestamptz ;
-- date
 RETURN '1997-12-17'::date ;
 RETURN '12/17/1997'::date ;
 RETURN 'Wed Dec 17 1997'::date ;
-- time
 RETURN '07:37:16-08'::time ;
 RETURN '07:37:16.00'::time ;
 RETURN '07:37:16'::time ;
-- timetz
 RETURN '07:37:16-08'::timetz ;
 RETURN '07:37:16.00'::timetz ;
 RETURN '07:37:16'::timetz ;
-- Interval
 RETURN '30 Seconds'::interval ;
 RETURN '15 Minutes'::interval ;
 RETURN '10 Hours'::interval ;
 RETURN '40 Days'::interval ;
 RETURN '10 Weeks'::interval ;
 RETURN '10 Months'::interval ;
 RETURN '3 Years'::interval ;
 RETURN '30 Seconds Ago'::interval ;
 RETURN '15 Minutes Ago'::interval ;
 RETURN '10 Hours Ago'::interval ;
 RETURN '40 Days Ago'::interval ;
 RETURN '10 Weeks Ago'::interval ;
 RETURN '10 Months Ago'::interval ;
 RETURN '3 Years Ago'::interval ;

--
-- toTimestamp()
--
 RETURN toTimestamp('12/17/1997 07:37:16.00+00'::timestamptz) ;
 RETURN toTimestamp('12/17/1997 07:37:16.00+00') ;
 RETURN toTimestamp('12/17/1997'::date) ;
 RETURN toTimestamp(100000000000) ;

--
-- Postgres Timestamp to GType
--
SELECT '12/17/1997 07:37:16.00+00'::timestamp::gtype;

--
-- toTimestampTz()
--
 RETURN toTimestampTz('12/17/1997 07:37:16.00+00'::timestamp) ;
 RETURN toTimestampTz('12/17/1997 07:37:16.00+00') ;
 RETURN toTimestampTz('12/17/1997'::date) ;
 RETURN toTimestampTz(100000000000) ;

--
-- Postgres Timestamp to GType
--
SELECT '12/17/1997 07:37:16.00+08'::timestamptz::gtype;

--
-- toDate()
--
 RETURN toDate('12/17/1997 07:37:16.00+00'::timestamp) ;
 RETURN toDate('12/17/1997 07:37:16.00+00'::timestamptz) ;
 RETURN toDate('12/17/1997 07:37:16.00+00') ;
 RETURN toDate('12/17/1997'::date) ;

--
-- Postgres Date to GType
--
SELECT '12/17/1997'::date::gtype;

--
-- toTime()
--
 RETURN toTime('12/17/1997 07:37:16.00+00'::timestamp) ;
 RETURN toTime('12/17/1997 07:37:16.00+00'::timestamptz) ;
 RETURN toTime('07:37:16.00+00') ;
 RETURN toTime('07:37:16.00+00'::timetz) ;
 RETURN toTime('7 Hours 37 Minutes 16 Seconds'::interval) ;

--
-- Postgres Time to GType
--
SELECT '07:37:16.00'::time::gtype;


--
-- toTimeTz()
--
 RETURN toTimeTz('12/17/1997 07:37:16.00+00'::timestamptz) ;
 RETURN toTimeTz('07:37:16.00+00') ;
 RETURN toTimeTz('07:37:16.00+00'::timetz) ;
 RETURN toTimeTz('07:37:16.00+00'::time) ;

--
-- Postgres Time to GType
--
SELECT '07:37:16.00+08'::timetz::gtype;


--
-- Timestamp Comparison
--
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-05-23 13:39:40.00'::timestamp ;

--
-- Timestamp With Timezone Comparison
--
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-05-23 13:39:40.00'::timestamptz ;

--
-- Timestamp With Timezone Timestamp Comparison
--
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-05-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-07-23 13:39:40.00'::timestamp ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-05-23 13:39:40.00'::timestamp ;

--
-- Timestamp With Timezone Comparison
--
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-05-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-07-23 13:39:40.00'::timestamptz ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-05-23 13:39:40.00'::timestamptz ;

--
-- date comparison
--
 RETURN '1997-12-17'::date = '1997-12-17'::date ;
 RETURN '1997-12-17'::date = '1997-12-16'::date ;
 RETURN '1997-12-17'::date = '1997-12-18'::date ;

 RETURN '1997-12-17'::date <> '1997-12-17'::date ;
 RETURN '1997-12-17'::date <> '1997-12-16'::date ;
 RETURN '1997-12-17'::date <> '1997-12-18'::date ;

 RETURN '1997-12-17'::date > '1997-12-17'::date ;
 RETURN '1997-12-17'::date > '1997-12-16'::date ;
 RETURN '1997-12-17'::date > '1997-12-18'::date ;

 RETURN '1997-12-17'::date < '1997-12-17'::date ;
 RETURN '1997-12-17'::date < '1997-12-16'::date ;
 RETURN '1997-12-17'::date < '1997-12-18'::date ;

 RETURN '1997-12-17'::date >= '1997-12-17'::date ;
 RETURN '1997-12-17'::date >= '1997-12-16'::date ;
 RETURN '1997-12-17'::date >= '1997-12-18'::date ;

 RETURN '1997-12-17'::date <= '1997-12-17'::date ;
 RETURN '1997-12-17'::date <= '1997-12-16'::date ;
 RETURN '1997-12-17'::date <= '1997-12-18'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamp = '2023-06-23'::date ;
--
-- Timestamp and Date Comparison
--
 RETURN '2023-06-23 0:0:00.00'::timestamp = '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp = '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamp <> '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <> '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamp > '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp > '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamp < '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp < '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamp >= '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp >= '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamp <= '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamp <= '2023-05-23'::date ;

 RETURN '2023-06-23'::date = '2023-06-23 0:0:00.00'::timestamp ;
 RETURN '2023-07-23'::date = '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-05-23'::date = '2023-06-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23'::date <> '2023-06-23 0:0:00.00'::timestamp ;
 RETURN '2023-07-23'::date <> '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-05-23'::date <> '2023-06-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23'::date > '2023-06-23 0:0:00.00'::timestamp ;
 RETURN '2023-07-23'::date > '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-05-23'::date > '2023-06-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23'::date < '2023-06-23 0:0:00.00'::timestamp ;
 RETURN '2023-07-23'::date < '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-05-23'::date < '2023-06-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23'::date >= '2023-06-23 0:0:00.00'::timestamp ;
 RETURN '2023-07-23'::date >= '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-05-23'::date >= '2023-06-23 13:39:40.00'::timestamp ;

 RETURN '2023-06-23'::date <= '2023-06-23 0:0:00.00'::timestamp ;
 RETURN '2023-07-23'::date <= '2023-06-23 13:39:40.00'::timestamp ;
 RETURN '2023-05-23'::date <= '2023-06-23 13:39:40.00'::timestamp ;

--
-- Timestamp With TimeZone and Date Comparison
--
 RETURN '2023-06-23 0:0:00.00'::timestamptz = '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz = '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamptz <> '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <> '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamptz > '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz > '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamptz < '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz < '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamptz >= '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz >= '2023-05-23'::date ;

 RETURN '2023-06-23 0:0:00.00'::timestamptz <= '2023-06-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-07-23'::date ;
 RETURN '2023-06-23 13:39:40.00'::timestamptz <= '2023-05-23'::date ;

 RETURN '2023-06-23'::date = '2023-06-23 0:0:00.00'::timestamptz ;
 RETURN '2023-07-23'::date = '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-05-23'::date = '2023-06-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23'::date <> '2023-06-23 0:0:00.00'::timestamptz ;
 RETURN '2023-07-23'::date <> '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-05-23'::date <> '2023-06-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23'::date > '2023-06-23 0:0:00.00'::timestamptz ;
 RETURN '2023-07-23'::date > '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-05-23'::date > '2023-06-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23'::date < '2023-06-23 0:0:00.00'::timestamptz ;
 RETURN '2023-07-23'::date < '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-05-23'::date < '2023-06-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23'::date >= '2023-06-23 0:0:00.00'::timestamptz ;
 RETURN '2023-07-23'::date >= '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-05-23'::date >= '2023-06-23 13:39:40.00'::timestamptz ;

 RETURN '2023-06-23'::date <= '2023-06-23 0:0:00.00'::timestamptz ;
 RETURN '2023-07-23'::date <= '2023-06-23 13:39:40.00'::timestamptz ;
 RETURN '2023-05-23'::date <= '2023-06-23 13:39:40.00'::timestamptz ;


--
-- Time Comparison
--
 RETURN '07:37:16.00'::time = '07:37:16.00'::time ;
 RETURN '07:37:16.00'::time = '06:37:16.00'::time ;
 RETURN '07:37:16.00'::time = '08:37:16.00'::time ;

 RETURN '07:37:16.00'::time <> '07:37:16.00'::time ;
 RETURN '07:37:16.00'::time <> '06:37:16.00'::time ;
 RETURN '07:37:16.00'::time <> '08:37:16.00'::time ;

 RETURN '07:37:16.00'::time > '07:37:16.00'::time ;
 RETURN '07:37:16.00'::time > '06:37:16.00'::time ;
 RETURN '07:37:16.00'::time > '08:37:16.00'::time ;

 RETURN '07:37:16.00'::time < '07:37:16.00'::time ;
 RETURN '07:37:16.00'::time < '06:37:16.00'::time ;
 RETURN '07:37:16.00'::time < '08:37:16.00'::time ;

 RETURN '07:37:16.00'::time >= '07:37:16.00'::time ;
 RETURN '07:37:16.00'::time >= '06:37:16.00'::time ;
 RETURN '07:37:16.00'::time >= '08:37:16.00'::time ;

 RETURN '07:37:16.00'::time <= '07:37:16.00'::time ;
 RETURN '07:37:16.00'::time <= '06:37:16.00'::time ;
 RETURN '07:37:16.00'::time <= '08:37:16.00'::time ;

--
-- Time With Timezone Comparison
--
 RETURN '07:37:16.00'::timetz = '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz = '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz = '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::timetz <> '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz <> '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz <> '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::timetz > '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz > '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz > '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::timetz < '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz < '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz < '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::timetz >= '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz >= '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz >= '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::timetz <= '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz <= '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::timetz <= '08:37:16.00'::timetz ;


 RETURN '07:37:16.00'::timetz = '07:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz = '06:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz = '08:37:16.00'::time ;

 RETURN '07:37:16.00'::timetz <> '07:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz <> '06:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz <> '08:37:16.00'::time ;

 RETURN '07:37:16.00'::timetz > '07:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz > '06:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz > '08:37:16.00'::time ;

 RETURN '07:37:16.00'::timetz < '07:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz < '06:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz < '08:37:16.00'::time ;

 RETURN '07:37:16.00'::timetz >= '07:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz >= '06:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz >= '08:37:16.00'::time ;

 RETURN '07:37:16.00'::timetz <= '07:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz <= '06:37:16.00'::time ;
 RETURN '07:37:16.00'::timetz <= '08:37:16.00'::time ;

 RETURN '07:37:16.00'::time = '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time = '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time = '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::time <> '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time <> '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time <> '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::time > '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time > '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time > '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::time < '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time < '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time < '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::time >= '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time >= '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time >= '08:37:16.00'::timetz ;

 RETURN '07:37:16.00'::time <= '07:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time <= '06:37:16.00'::timetz ;
 RETURN '07:37:16.00'::time <= '08:37:16.00'::timetz ;

--
-- Interval Comparison
--
 RETURN '30 Seconds'::interval = '30 Seconds'::interval;
 RETURN '30 Seconds'::interval = '20 Seconds'::interval;
 RETURN '30 Seconds'::interval = '40 Seconds'::interval;

 RETURN '30 Seconds'::interval <> '30 Seconds'::interval;
 RETURN '30 Seconds'::interval <> '20 Seconds'::interval;
 RETURN '30 Seconds'::interval <> '40 Seconds'::interval;

 RETURN '30 Seconds'::interval > '30 Seconds'::interval;
 RETURN '30 Seconds'::interval > '20 Seconds'::interval;
 RETURN '30 Seconds'::interval > '40 Seconds'::interval;

 RETURN '30 Seconds'::interval < '30 Seconds'::interval;
 RETURN '30 Seconds'::interval < '20 Seconds'::interval;
 RETURN '30 Seconds'::interval < '40 Seconds'::interval;

 RETURN '30 Seconds'::interval >= '30 Seconds'::interval;
 RETURN '30 Seconds'::interval >= '20 Seconds'::interval;
 RETURN '30 Seconds'::interval >= '40 Seconds'::interval;

 RETURN '30 Seconds'::interval <= '30 Seconds'::interval;
 RETURN '30 Seconds'::interval <= '20 Seconds'::interval;
 RETURN '30 Seconds'::interval <= '40 Seconds'::interval;


--
-- + Operator
--
-- Timestamp + Interval Operator
 RETURN '2023-06-23 13:39:40.00'::timestamp + '10 Days'::interval ;
-- Timestamp With Timezone + Interval Operator
 RETURN '2023-06-23 13:39:40.00'::timestamptz + '10 Days'::interval ;
-- Date + Interval Operator
 RETURN '2023-06-23'::date + '10 Days'::interval ;
-- Time + Interval Operator
 RETURN '13:39:40.00'::time + '8 Hours'::interval ;
-- Time With Timezone + Interval Operator
 RETURN '13:39:40.00'::timetz + '8 Hours'::interval ;
-- Interval + Interval Operator
 RETURN '10 Days'::interval + '8 Hours'::interval ;
-- Timestamp - Interval Operator
 RETURN '2023-06-23 13:39:40.00'::timestamp - '10 Days'::interval ;
-- Timestamp With Timezone - Interval Operator
 RETURN '2023-06-23 13:39:40.00'::timestamptz - '10 Days'::interval ;

--
-- - Operator
--
-- Date - Interval Operator
 RETURN '2023-06-23'::date - '10 Days'::interval ;
-- Time - Interval Operator
 RETURN '13:39:40.00'::time - '8 Hours'::interval ;
-- Time With Timezone - Interval Operator
 RETURN '13:39:40.00'::timetz - '8 Hours'::interval ;
-- Interval - Interval Operator
 RETURN '10 Days'::interval - '8 Hours'::interval ;
--
-- - Interval Operator
--
 RETURN - '8 Hours'::interval ;
 RETURN - '8 Hours Ago'::interval ;


--
-- * Interval Operator
--
 RETURN '8 Hours'::interval * 8.0 ;
 RETURN '8 Hours'::interval * 8 ;
 RETURN 8 * '8 Hours'::interval ;
 RETURN 8.0 * '8 Hours'::interval ;

--
-- / Interval Operator
--
 RETURN '8 Hours'::interval / 8.0 ;
 RETURN '8 Hours'::interval / 8 ;

--
-- Extract
--
 RETURN EXTRACT(day FROM TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(day FROM TIMESTAMP WITHOUT TIME ZONE '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(day FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(hour FROM TIME '07:37:16.00+00') ;
 RETURN EXTRACT(hour FROM TIME WITH TIME ZONE '07:37:16.00+00') ;
 RETURN EXTRACT(hour FROM TIME WITHOUT TIME ZONE '07:37:16.00+00') ;
 RETURN EXTRACT(day FROM DATE '12/17/1997') ;
 RETURN EXTRACT(day FROM INTERVAL '6 Years 11 Months 24 Days 5 Hours 23 Minutes') ;
 RETURN EXTRACT(CENTURY FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(DECADE FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(DOW FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(EPOCH FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(ISODOW FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(ISOYEAR FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(JULIAN FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(MICROSECONDS FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(MILLISECONDS FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(ISOYEAR FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(MINUTE FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(MONTH FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(QUARTER FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(SECOND FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(TIMEZONE FROM TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00') ;
 RETURN EXTRACT(YEAR FROM TIMESTAMP '12/17/1997 07:37:16.00+00') ;

--
-- Date Part
--
 RETURN date_part('day', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('day', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('hour', TIME '07:37:16.00+00') ;
 RETURN date_part('hour', TIME WITH TIME ZONE '07:37:16.00+00') ;
 RETURN date_part('day', DATE '12/17/1997') ;
 RETURN date_part('day', INTERVAL '6 Years 11 Months 24 Days 5 Hours 23 Minutes') ;
 RETURN date_part('CENTURY', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('DECADE', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('DOW', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('EPOCH', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('ISODOW', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('ISOYEAR', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('JULIAN', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('MICROSECONDS', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('MILLISECONDS', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('ISOYEAR', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('MINUTE', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('MONTH', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('QUARTER', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('SECOND', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('TIMEZONE', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00') ;
 RETURN date_part('YEAR', TIMESTAMP '12/17/1997 07:37:16.00+00') ;

--
-- date_bin
--
 RETURN date_bin(INTERVAL '15 minutes', TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01') ;
 RETURN date_bin(INTERVAL '15 minutes', TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01') ;
 RETURN date_bin('15 minutes'::interval, '2020-02-11 15:44:17+08'::timestamptz, '2001-01-01'::timestamptz) ;
 RETURN date_bin('15 minutes'::interval, '2020-02-11 15:44:17+08'::timestamp, '2001-01-01'::date) ;
 RETURN date_bin('15 minutes'::interval, '2001-01-01'::date, '2020-02-11 15:44:17+08'::timestamp) ;

--
-- date_trunc
--
 RETURN date_trunc('day', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00') ;
 RETURN date_trunc('day', TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00', 'Australia/Sydney') ;
 RETURN date_trunc('day', TIMESTAMP '12/17/1997 07:37:16.00+00') ;
 RETURN date_trunc('day', INTERVAL '6 Years 11 Months 24 Days 5 Hours 23 Minutes') ;

--
-- Temporal Functions
--
 RETURN age('12/17/1997 07:37:16.00+00'::timestamptz, '6/12/2007 12:45:19.89+00'::timestamptz) ;
 RETURN age('12/17/1997 07:37:16.00+00'::timestamp, '6/12/2007 12:45:19.89+00'::timestamp) ;
 RETURN age('12/17/1997 07:37:16.00+00'::timestamptz, '6/12/2007 12:45:19.89+00'::timestamp) ;


--
-- Overlap
--

-- date date date date
 RETURN ('2001-02-16'::date,'2001-10-29'::date) overlaps ('2001-10-30'::date,'2002-10-30'::date) ;
 RETURN ('2001-02-16'::date,'2001-10-31'::date) overlaps ('2001-10-30'::date,'2002-10-30'::date) ;
-- date interval date interval
 RETURN ('2001-02-16'::date,'10 days'::interval) overlaps ('2001-10-30'::date,'10 days'::interval) ;
 RETURN ('2001-02-16'::date,'365 days'::interval) overlaps ('2001-10-30'::date,'10 days'::interval) ;
-- date interval date date
 RETURN ('2001-02-16'::date,'10 days'::interval) overlaps ('2001-10-30'::date,'2001-11-09'::date) ;
 RETURN ('2001-02-16'::date,'365 days'::interval) overlaps ('2001-10-30'::date,'2001-11-09'::date) ;
-- date date date interval
 RETURN ('2001-02-16'::date,'2001-10-28'::date) overlaps ('2001-10-30'::date,'-2 days'::interval) ;
 RETURN ('2001-02-16'::date,'2001-10-28'::date) overlaps ('2001-10-30'::date,'-3 days'::interval) ;
-- timestamp timestamp timestamp timestamp
 RETURN ('2001-02-16'::timestamp,'2001-10-29'::timestamp) overlaps ('2001-10-30'::timestamp,'2002-10-30'::timestamp) ;
 RETURN ('2001-02-16'::timestamp,'2001-10-31'::timestamp) overlaps ('2001-10-30'::timestamp,'2002-10-30'::timestamp) ;
-- timestamp interval timestamp interval
 RETURN ('2001-02-16'::timestamp,'10 days'::interval) overlaps ('2001-10-30'::timestamp,'10 days'::interval) ;
 RETURN ('2001-02-16'::timestamp,'365 days'::interval) overlaps ('2001-10-30'::timestamp,'10 days'::interval) ;
-- timestamp interval timestamp timestamp
 RETURN ('2001-02-16'::timestamp,'10 days'::interval) overlaps ('2001-10-30'::timestamp,'2001-11-09'::timestamp) ;
 RETURN ('2001-02-16'::timestamp,'365 days'::interval) overlaps ('2001-10-30'::timestamp,'2001-11-09'::timestamp) ;
-- timestamp timestamp timestamp interval
 RETURN ('2001-02-16'::timestamp,'2001-10-28'::timestamp) overlaps ('2001-10-30'::timestamp,'-2 days'::interval) ;
 RETURN ('2001-02-16'::timestamp,'2001-10-28'::timestamp) overlaps ('2001-10-30'::timestamp,'-3 days'::interval) ;
-- time time time time
 RETURN ('20:00:00'::time,'21:00:00'::time) overlaps ('21:00:00'::time,'22:00:00'::time) ;
 RETURN ('20:00:00'::time,'21:01:00'::time) overlaps ('21:00:00'::time,'22:00:00'::time) ;
-- time interval time interval
 RETURN ('20:00:00'::time,'10 minutes'::interval) overlaps ('20:10:00'::time,'10 minutes'::interval) ;
 RETURN ('20:00:00'::time,'11 minutes'::interval) overlaps ('20:10:00'::time,'10 minutes'::interval) ;
-- time interval time time
 RETURN ('20:00:00'::time,'10 minutes'::interval) overlaps ('20:10:00'::time,'20:20:00'::time) ;
 RETURN ('20:00:00'::time,'11 minutes'::interval) overlaps ('20:10:00'::time,'20:20:00'::time) ;
-- time time time interval
 RETURN ('20:00:00'::time,'20:08:00'::time) overlaps ('20:10:00'::time,'-2 minutes'::interval) ;
 RETURN ('20:00:00'::time,'20:08:00'::time) overlaps ('20:10:00'::time,'-3 minutes'::interval) ;
-- timetz timetz timetz timetz
 RETURN ('20:00:00+00'::timetz,'21:00:00+00'::timetz) overlaps ('21:00:00+00'::timetz,'22:00:00+00'::timetz) ;
 RETURN ('20:00:00+00'::timetz,'21:01:00+00'::timetz) overlaps ('21:00:00+00'::timetz,'22:00:00+00'::timetz) ;
-- timetz interval timetz interval
 RETURN ('20:00:00+00'::timetz,'10 minutes'::interval) overlaps ('20:10:00+00'::timetz,'10 minutes'::interval) ;
 RETURN ('20:00:00+00'::timetz,'11 minutes'::interval) overlaps ('20:10:00+00'::timetz,'10 minutes'::interval) ;
-- timetz interval timetz timetz
 RETURN ('20:00:00+00'::timetz,'10 minutes'::interval) overlaps ('20:10:00+00'::timetz,'20:20:00+00'::timetz) ;
 RETURN ('20:00:00+00'::timetz,'11 minutes'::interval) overlaps ('20:10:00+00'::timetz,'20:20:00+00'::timetz) ;
-- timetz timetz timetz interval
 RETURN ('20:00:00+00'::timetz,'20:08:00+00'::timetz) overlaps ('20:10:00+00'::timetz,'-2 minutes'::interval) ;
 RETURN ('20:00:00+00'::timetz,'20:08:00+00'::timetz) overlaps ('20:10:00+00'::timetz,'-3 minutes'::interval) ;

--
-- justify functions
--
-- justify_interval
 RETURN justify_interval('1 month -1 hours'::interval) ;
 RETURN justify_interval('1 month 33 days 1 hours'::interval) ;
 RETURN justify_interval('1 week 6 days 27 hours'::interval) ;
 RETURN justify_interval('27 hours'::interval) ;
-- justify_days
 RETURN justify_days('5 weeks'::interval) ;
 RETURN justify_days('35 days'::interval) ;
 RETURN justify_days('4 weeks 8 days'::interval) ;
 RETURN justify_days('1 month 5 weeks'::interval) ;
-- justify_hours
 RETURN justify_hours('27 hours'::interval) ;
 RETURN justify_hours('1 week 27 hours'::interval) ;
 RETURN justify_hours('2 days 30 hours'::interval) ;

--
-- isfinite
--
 RETURN isfinite('infinity'::date) ;
 RETURN isfinite('2001-02-16'::date) ;
 RETURN isfinite('2001-02-16 23:40:00'::timestamp) ;
 RETURN isfinite('infinity'::timestamp) ;
 RETURN isfinite('15 minutes'::interval) ;

--
-- Temporal Make Functions
--
RETURN make_date(0, 7, 15);
RETURN make_date(2013, 2, 30);
RETURN make_date(2013, 13, 1);
RETURN make_date(-44, 3, 15);
RETURN make_date(2013, 11, -1);
RETURN make_date(2013, 7, 15);

RETURN make_time(8, 20, 0.0);
RETURN make_time(10, 55, 100.1);
RETURN make_time(24, 0, 2.1);

RETURN make_timestamp(2023, 2, 14, 5, 30, 0.0);
RETURN make_timestamptz(2023, 2, 14, 5, 30, 0.0);
RETURN make_timestamptz(2023, 2, 14, 5, 30, 0.0, 'KST');
RETURN make_timestamptz(2023, 2, 14, 5, 30, 0.0, 'GMT');

--
-- Typecasting
--
--Interval
 RETURN '30 Seconds'::interval ;
--timestamp
 RETURN TIMESTAMP '2020-02-11 15:44:17' ;
 RETURN '2020-02-11 15:44:17' ;
--timestamptz
 RETURN TIMESTAMP WITH TIME ZONE '12/17/1997 07:37:16.00+00' ;
 RETURN '12/17/1997 07:37:16.00+00' ;
-- date
RETURN make_date(2000, 7, 15);
-- time 
RETURN make_time(8, 20, 0.0);
-- timetz
 RETURN TIME WITH TIME ZONE '07:37:16.00+00' ;
 RETURN '07:37:16.00+00' ;
--interval
 RETURN '7 Hours 37 Minutes 16 Seconds'::interval ;
SELECT '7 Hours 37 Minutes 16 Seconds'::interval::gtype;

--
-- Clean up
--
DROP GRAPH temporal CASCADE;