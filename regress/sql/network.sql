/*
 * Copyright (C) 2023 PostGraphDB
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
 */

LOAD 'postgraph';
SET search_path TO postgraph;

SELECT create_graph('network');

--
-- inet
--
SELECT * FROM cypher('network', $$ RETURN toinet('192.168.1.5') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN toinet('192.168.1/24') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN toinet('::ffff:fff0:1') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '192.168.1.5'::inet $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '192.168.1/24'::inet $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '::ffff:fff0:1'::inet $$) as (i gtype);


--
-- cidr
--
SELECT * FROM cypher('network', $$ RETURN tocidr('192.168.1.5') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN tocidr('192.168.1/24') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN tocidr('::ffff:fff0:1') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '192.168.1.5'::cidr $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '192.168.1/24'::cidr $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '::ffff:fff0:1'::cidr $$) as (i gtype);

--
-- macaddr
--
SELECT * FROM cypher('network', $$ RETURN tomacaddr('12:34:56:78:90:ab') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '12:34:56:78:90:ab'::macaddr $$) as (i gtype);

--
-- macaddr8
--
SELECT * FROM cypher('network', $$ RETURN tomacaddr8('12:34:56:78:90:ab:cd:ef') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '12:34:56:78:90:ab:cd:ef'::macaddr8 $$) as (i gtype);

--
-- abbrev
--
SELECT * FROM cypher('network', $$ RETURN abbrev('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN abbrev('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN abbrev('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN abbrev('10.1.0.0/32'::inet) $$) as (i gtype);

SELECT * FROM cypher('network', $$ RETURN abbrev('192.168.1.5'::cidr) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN abbrev('192.168.1/24'::cidr) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN abbrev('::ffff:fff0:1'::cidr) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN abbrev('10.1.0.0/32'::cidr) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN abbrev('10.1.0.0/16'::cidr) $$) as (i gtype);

--
-- broadcast
--
SELECT * FROM cypher('network', $$ RETURN broadcast('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN broadcast('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN broadcast('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN broadcast('10.1.0.0/32'::inet) $$) as (i gtype);


SELECT drop_graph('network', true);
