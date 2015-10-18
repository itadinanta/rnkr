---
title: Getting started
---
### Release notes

Latest release is **0.5.1**

- 0.5.1: configurable partitions
- 0.5.0: embedded Cassandra, Akka 2.4.0, ddata clustering
- 0.4.0: Initial release


### Download from Bintray 

	wget https://bintray.com/artifact/download/itadinanta/maven/\
	net/itadinanta/rnkr-app_2.11/0.5.1/rnkr-app_2.11-0.5.1-package.tar.gz
	tar xvf rnkr-app_2.11-0.5.1-package.tar.gz

Or manually [download](https://bintray.com/artifact/download/itadinanta/maven/net/itadinanta/rnkr-app_2.11/0.5.1/rnkr-app_2.11-0.5.1-package.tar.gz) and unpack

### Persistence

Default installation comes with embedded Cassandra, so you get persistence out of the box. Embedded Cassandra is not recommended in production. Set up a separate Cassandra
cluster and change the `partitions` setting to connect to the external cluster. See example below.

### Requirements

- Java 8 JRE or JDK

### Run the server

	cd rnkr-app-0.5.1
	bin/rnkr-app

### Use the REST API

The service listens on 127.0.0.1:8080 by default. See settings below. HTTP auth user and password are required.

	curl -uuser:pass -XPUT "http://localhost:8080/v0/default/test" -d"score=101&entrant=Nico"
	{
	  "score": 101,
	  "timestamp": 1443315052812000000,
	  "entrant": "Nico",
	  "rank": 1
	}

	curl -uuser:pass -XPUT "http://localhost:8080/v0/default/test" -d"score=100&entrant=Itadinanta"
	{
	  "score": 100,
	  "timestamp": 1443315493293000000,
	  "entrant": "Itadinanta",
	  "rank": 0
	}

	curl -uuser:pass -XPUT "http://localhost:8080/v0/default/test" -d"score=110&entrant=Nico"
	{
	  "score": 101,
	  "timestamp": 1443315052812000000,
	  "entrant": "Nico",
	  "rank": 1
	}

	curl -uuser:pass -GET "http://localhost:8080/v0/default/test/page"
	[{
	  "score": 100,
	  "timestamp": 1443315493293000000,
	  "entrant": "Itadinanta",
	  "rank": 0
	}, {
	  "score": 101,
	  "timestamp": 1443315052812000000,
	  "entrant": "Nico",
	  "rank": 1
	}]

### Configure

A number of options is available. To get started, you could edit `conf/application.conf` and copy/paste to experiment with the following options.
Service needs restarting if the settings change.

	net.itadinanta.rnkr {
		listen {
			host = "127.0.0.1"
			port = 8080
		}
		
		cassandra {
			embedded = true
			config = "conf/cassandra.yaml"
		}
		
		
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
				}
				
				auth {
					"user": "pass"
				}
			}
		}
	}
	
	akka {
		cluster {
			seed-nodes = [
				"akka.tcp://rnkr@127.0.0.1:2551"
			]
		}
	}