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



SELECT drop_graph('network', true);
