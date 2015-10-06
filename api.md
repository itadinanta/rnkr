---
title: API usage and reference
---

### HTTP service API

Leaderbord functionality is accessible from the HTTP service by default. Functionality

All HTTP entry points follow the basic schema:

URL: http://HOST:PORT/VERSION/PARTITION/ID[/ACTION]
Method: GET | PUT | POST
Authentication: HTTP Basic, required

#### HOST:PORT

Tcp coordinates of the frontend server. These can be configured in application.conf. Each node can listen to one HOST and PORT.

### VERSION

HTTP Protocol version. This is currently set to the reserved word ```rnkr``` TODO: set it to ```v0```. It will be set to v1 on release 1.0 and then updated if and when breaking changes are introduced.
A node can refuse a request if the protocol major version is not available (for instance, requiring version v2 when a node can only respond to version v1).

#### PARTITION

Each leaderboard ID is unique per partitions. Partitions can be thought as logical namespaces.
Parititioning can be used to isolate development stages (dev, staging, production) behind the same front end, or even different clients, for instance if cutting costs by sharing environments across multiple games.
Partitions can be set up to share: 

- nothing: each partition has its own separate backend. 
Example: partition A could be backed by Cassandra cluster A, partition B is backed by Cassandra cluster B, 
partition C is transient (blackhole backend).
- database: partitions use the same backend but separate schemas.
Example: partition A is backed by Cassandra cluster A, keyspace A1, 
partition B is backed by Cassandra cluster A, keyspace A2
- schema: partitions use the same backend and schema but separate tables.
Example: partition A andd B share the keyspace A1 on cluster A,
but all table names in partition are prefixed by "A_", all tables in partition "B_" have prefix B_
- all: partitions share all tables, internally all leaderboard keys are remapped by f name prefix.
Example: partition A and B share everything, all leaderboard keys in partition 
A are transparently prefixed by "A:", all leaderboard keys in partition B are transparently prefixed by "B:"

#### ID

The unique ID of a leaderboard. In general, an identifier is valid as a leaderboard ID if it is valid as a Unix filename (not full path) "/" and "\" are not allowed.

#### ACTION

Query, function, command or method to apply to the specify leaderboard.

### Actions

#### PUT http://HOST:PORT/rnkr/PARTITION/ID
#### POST http://HOST:PORT/rnkr/PARTITION/ID

Posts a signed long SCORE as the ENTRANT in the leaderboard ID, with optional ATTACHMENTS. If ENTRANT has not a score in the board, or the FORCE flat is true, or the posted score is better than the existing one, the new score and attachment replace the existing ones.

###### Request:
Content-Type: x-http-form-urlencoded)
Parameters:
	score=SCORE
	entrant=ENTRANT
	(optional) attachments=ATTACHMENTS
	(optional) force=FORCE

###### Response:
Content-Type: application/json
	{}

#### DELETE http://HOST:PORT/rnkr/PARTITION/ID

Deletes all entrants or one single entrant from the leaderboard identified by ID. If ENTRANT exists, it is removed. If ENTRANT does not exist, it is ignored. If ENTRANT is not specified, all ENTRANTS are removed.

###### Request:
Content-Type: x-http-form-urlencoded)
Parameters:
	(optional) entrant=ENTRANT

###### Response:
Content-Type: application/json
	{}




