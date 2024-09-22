-- Regression tests don't preload extensions, gotta load it first
LOAD 'postgraph';

-- Basic Graph creation
CREATE GRAPH new_cypher;

-- Assign Graph to use
USE GRAPH new_cypher;

-- Reuse Name, should throw error
CREATE GRAPH new_cypher;

-- Graph Does not exist, should throw error
USE GRAPH new_cypher;

CREATE GRAPH new_cypher_2;
USE GRAPH new_cypher_2;
USE GRAPH new_cypher;

-- Old Grammar is at least partially plugged in.
RETURN 1 as a;

CREATE (n);

CREATE (n) RETURN n;

--CREATE (n) RETURN *;

MATCH (n) RETURN n;

MATCH (n) RETURN *;