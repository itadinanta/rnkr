---
title: API usage and reference
---

### HTTP service API

Leaderbord functionality is accessible from the HTTP service by default.

All HTTP entry points follow the basic schema:

- URL: http://HOST:PORT/VERSION/PARTITION/ID[/ACTION]
- Method: GET \| PUT \| POST
- Authentication: HTTP Basic, required

### URL format

#### HOST:PORT

Tcp coordinates of the frontend server. These can be configured in application.conf. Each node can listen to one HOST and PORT.

#### VERSION

HTTP Protocol version.  It is currently ```v0```, and planned to stay like that for the whole lifecycle of release series 0.x. 
It will be set to v1 on release 1.0 and then updated if and when breaking changes are introduced.
A node can refuse a request if the protocol major version is not available (for instance, requiring version v2 when a node can only respond to version v1).

#### PARTITION

Each leaderboard ID is unique per partitions. Partitions can be thought as logical namespaces.
Partitioning can be used to isolate development stages (dev, staging, production) behind the same front end, or even different clients, for instance if cutting costs by sharing environments across multiple games.
Partitions can be set up to share: 

- **nothing**: each partition has its own separate backend. 
Example: partition A could be backed by Cassandra cluster A, partition B is backed by Cassandra cluster B, 
partition C is transient (blackhole backend).
- **database**: partitions use the same backend but separate schemas.
Example: partition A is backed by Cassandra cluster A, keyspace A1, 
partition B is backed by Cassandra cluster A, keyspace A2
- **schema**: partitions use the same backend and schema but separate tables.
Example: partition A andd B share the keyspace A1 on cluster A,
but all table names in partition are prefixed by "A_", all tables in partition "B_" have prefix B_
- **all**: partitions share all tables, internally all leaderboard keys are remapped by f name prefix.
Example: partition A and B share everything, all leaderboard keys in partition 
A are transparently prefixed by "A:", all leaderboard keys in partition B are transparently prefixed by "B:"

Here's how the "default" partition is configured:

	partitions {
		"default" {
			persistence {
				// type = "blackhole"
				type = "cassandra"
			}
			
			cassandra {
				hosts = [ "127.0.0.1" ]
				port = 9042
				keyspace = "rnkr"
				// prefix = null
			}
			
			auth {
				"user": "pass"
			}
		}
	}

#### ID

The unique ID of a leaderboard. In general, an identifier is valid as a leaderboard ID if it is valid as a Unix filename (not full path) "/" and "\" are not allowed.

#### ACTION

Query, function, command or method to apply to the specify leaderboard.

### Update Actions

Update actions change the state of the leaderboard. These actions are `ScorePost` and `Delete`. Update operations are guaranteed to be **serialized**. 

#### ScorePost

	PUT http://HOST:PORT/VERSION/PARTITION/ID
	POST http://HOST:PORT/VERSION/PARTITION/ID

Posts a signed long SCORE as the ENTRANT in the leaderboard ID, with optional ATTACHMENTS. If ENTRANT has not a score in the board, or the FORCE flat is true, or the posted score is better than the existing one, the new score and attachment replace the existing ones.

A leaderboard is automagically created with its first ScorePost.

###### Request:
	Content-Type: x-http-form-urlencoded
	Parameters:
		score=SCORE
		entrant=ENTRANT
		(optional) attachments=ATTACHMENTS
		(optional) force=FORCE (default false)

###### Response:
	Content-Type: application/json
		{}

#### Delete

	DELETE http://HOST:PORT/VERSION/PARTITION/ID

Deletes one single entrant **or all** of them from the leaderboard identified by ID. If ENTRANT exists, it is deleted. If ENTRANT does not exist, it is ignored.

**If ENTRANT is not specified, all ENTRANTS are deleted and the leaderboard is wiped clean.**

###### Request:
	Content-Type: x-http-form-urlencoded
	Parameters:
		(optional) entrant=ENTRANT

###### Response:
	Content-Type: application/json
		{}

### Query Actions

Query actions can be sent to the service to retrieve segments off the current state. Query actions are served **concurrently**. Query actions requested while an update action is being fulfilled are queued.

#### Nearby
	
	GET http://HOST:PORT/VERSION/PARTITION/ID/nearby?count=COUNT&entrant=ENTRANT

Retrieves the segment of the leaderboard centered on the entrant's key, with COUNT items above and COUNT items below.
If the entrant does not exist, it returns an empty sequence.
If COUNT is not specify, it returns only the ENTRANT row.

###### Request:
	Parameters:
		(optional) count=COUNT (default 0)
		entrant=ENTRANT

###### Response:
	Content-Type: application/json
		{}

#### Lookup

	GET http://HOST:PORT/VERSION/PARTITION/ID/lookup?entrant=ENTRANT&entrant=ENTRANT...

Retrieves multiple entrants in the same order as they're requested, ignoring the ones which aren't present.

###### Request:
	Parameters:
		entrant=ENTRANT

###### Response:
	Content-Type: application/json
		{}

#### Rank (Estimated)

	GET http://HOST:PORT/VERSION/PARTITION/ID/rank?score=SCORE

Return an the estimated rank for a given score without posting it to the leaderboard. Entrant is unspecified.

###### Request:
	Parameters:
		score=SCORE

###### Response:
	Content-Type: application/json
		{}

#### Size

	GET http://HOST:PORT/VERSION/PARTITION/ID/size

Returns the size of the leaderboard.

###### Response:
	Content-Type: application/json
		{}

#### Around

	GET http://HOST:PORT/VERSION/PARTITION/ID/around?score=SCORE&count=COUNT

Retrieves a segment of the leaderboard centered around the SCORE, with COUNT items above and COUNT items below.

###### Request:
	Parameters:
		score=SCORE
		(optional) count=COUNT (default 0)

###### Response:
	Content-Type: application/json
		{}

#### Page

	GET http://HOST:PORT/VERSION/PARTITION/ID/page?start=START&length=LENGTH

Retrieves a segment of length LENGTH of the leaderboard starting from RANK (inclusive)

###### Request:
	Parameters:
		(optional) start=START (default 1)
		(optional) length=LENGTH (default 10)

###### Response:
	Content-Type: application/json
		{}
