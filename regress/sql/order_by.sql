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

CREATE GRAPH order_by;
USE GRAPH order_by;

CREATE ();
CREATE ({i: '1', j: 1});
CREATE ({i: 1});
CREATE ({i: 1.0});
CREATE ({i: 1::numeric});
CREATE ({i: true});
CREATE ({i: false});
CREATE ({i: {key: 'value'}});
CREATE ({i: [1]});

MATCH (u) RETURN u.i ORDER BY u.i;

MATCH (u) RETURN u.i ORDER BY u.i DESC;

MATCH (u) RETURN u.i ORDER BY u.i ASC;

MATCH (x) RETURN x.j ORDER BY x.j NULLS FIRST;

MATCH (x) RETURN x.j ORDER BY x.j NULLS LAST;

MATCH (x) RETURN x.i ORDER BY x.i USING <;

DROP GRAPH order_by CASCADE;