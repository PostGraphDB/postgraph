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
-- inet + integer
--
SELECT * FROM cypher('network', $$ RETURN toinet('192.168.1.5') + 10 $$) as (i gtype);

--
-- inet - integer
--
SELECT * FROM cypher('network', $$ RETURN toinet('192.168.1.5') - 10 $$) as (i gtype);

--
-- integer + inet
--
SELECT * FROM cypher('network', $$ RETURN 10 + toinet('192.168.1.5') $$) as (i gtype);


--
-- inet - inet
--
SELECT * FROM cypher('network', $$ RETURN toinet('192.168.1.5') - toinet('192.168.1.0') $$) as (i gtype);

--
-- ~ inet
--
SELECT * FROM cypher('network', $$ RETURN ~ toinet('192.168.1.5') $$) as (i gtype);

--
-- inet & inet
--
SELECT * FROM cypher('network', $$ RETURN '10.1.0.0/32'::inet & toinet('192.168.1.5') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '192.168.1.5'::inet & toinet('192.168.1.5') $$) as (i gtype);

--
-- inet | inet
--
SELECT * FROM cypher('network', $$ RETURN '10.1.0.0/32'::inet | toinet('192.168.1.5') $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN '192.168.1.5'::inet | toinet('192.168.1.5') $$) as (i gtype);

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

--
-- family
--
SELECT * FROM cypher('network', $$ RETURN family('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN family('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN family('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN family('10.1.0.0/32'::inet) $$) as (i gtype);

--
-- host
--
SELECT * FROM cypher('network', $$ RETURN host('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN host('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN host('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN host('10.1.0.0/32'::inet) $$) as (i gtype);

--
-- hostmask
--
SELECT * FROM cypher('network', $$ RETURN hostmask('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN hostmask('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN hostmask('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN hostmask('10.1.0.0/32'::inet) $$) as (i gtype);

--
-- inet_merge
--
SELECT * FROM cypher('network', $$ RETURN inet_merge('192.168.1.5/24'::inet, '192.168.2.5/24'::inet) $$) as (i gtype);

--
-- inet_same_family
--
SELECT * FROM cypher('network', $$ RETURN inet_same_family('192.168.1.5/24'::inet, '192.168.2.5/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN inet_same_family('192.168.1.5/24'::inet, '::1'::inet) $$) as (i gtype);

--
-- masklen
--
SELECT * FROM cypher('network', $$ RETURN masklen('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN masklen('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN masklen('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN masklen('10.1.0.0/32'::inet) $$) as (i gtype);

--
-- netmask
--
SELECT * FROM cypher('network', $$ RETURN netmask('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN netmask('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN netmask('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN netmask('10.1.0.0/32'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN netmask('192.168.1/8'::inet) $$) as (i gtype);

--
-- network
--
SELECT * FROM cypher('network', $$ RETURN network('192.168.1.5'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN network('192.168.1/24'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN network('::ffff:fff0:1'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN network('10.1.0.0/32'::inet) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN network('192.168.1/8'::inet) $$) as (i gtype);


--
-- set_masklen
--
SELECT * FROM cypher('network', $$ RETURN set_masklen('192.168.1.5'::inet, 24) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('192.168.1/24'::inet, 16) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('::ffff:fff0:1'::inet, 24) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('10.1.0.0/32'::inet, 8) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('192.168.1/8'::inet, 24) $$) as (i gtype);

SELECT * FROM cypher('network', $$ RETURN set_masklen('192.168.1.5'::cidr, 24) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('192.168.1/24'::cidr, 16) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('::ffff:fff0:1'::cidr, 24) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('10.1.0.0/32'::cidr, 8) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('192.168.1/8'::cidr, 24) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('192.168.1.0/24'::cidr, 16) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN set_masklen('10.1.0.0/32'::cidr, 4) $$) as (i gtype);

--
-- abbrev
--
SELECT * FROM cypher('network', $$ RETURN trunc('12:34:56:78:90:ab'::macaddr) $$) as (i gtype);

SELECT * FROM cypher('network', $$ RETURN trunc('12:34:56:78:90:ab:cd:ef'::macaddr8) $$) as (i gtype);

--
-- macaddr8_set7bit
--
SELECT * FROM cypher('network', $$ RETURN macaddr8_set7bit('12:34:56:78:90:ab:cd:ef'::macaddr8) $$) as (i gtype);
SELECT * FROM cypher('network', $$ RETURN macaddr8_set7bit('00:34:56:ab:cd:ef'::macaddr8) $$) as (i gtype);

SELECT drop_graph('network', true);
