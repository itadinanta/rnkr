---
title: Getting started
---

### Download from Bintray 

```bash
wget https://bintray.com/artifact/download/itadinanta/maven/net/itadinanta/rnkr-app_2.11/0.4.0/rnkr-app_2.11-0.4.0-package.tar.gz
tar xvf rnkr-app_2.11-0.4.0-package.tar.gz
```
Or manually download and unpack from [Bintray|https://bintray.com/artifact/download/itadinanta/maven/net/itadinanta/rnkr-app_2.11/0.4.0/rnkr-app_2.11-0.4.0-package.tar.gz]

### Run the server

```bash
cd rnkr-app-0.4.0
bin/rnkr-app
```
### Use the REST API

The service listens on 127.0.0.1:8080 by default. See settings below. HTTP auth user and password are required.

```
curl -uuser:pass -XPUT "http://localhost:8080/rnkr/leaderboard/test" -d"score=101&entrant=Nico"
{
  "score": 101,
  "timestamp": 1443315052812000000,
  "entrant": "Nico",
  "rank": 1
}

curl -uuser:pass -XPUT "http://localhost:8080/rnkr/leaderboard/test" -d"score=100&entrant=Itadinanta"
{
  "score": 100,
  "timestamp": 1443315493293000000,
  "entrant": "Itadinanta",
  "rank": 0
}

curl -uuser:pass -XPUT "http://localhost:8080/rnkr/leaderboard/test" -d"score=110&entrant=Nico"
{
  "score": 101,
  "timestamp": 1443315052812000000,
  "entrant": "Nico",
  "rank": 1
}

curl -uuser:pass -GET "http://localhost:8080/rnkr/leaderboard/test/page"
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
```

### Configure

Edit ```conf/application.conf```

A number of options is available. For instance, to change binding host/port, edit ```conf/application.conf```:

```hocon
net.itadinanta.rnkr {
	listen {
		host = "127.0.0.1"
		port = 8080
	}
}
```

### Persistence

Out of the box the service doesn't persist the data. Cassandra persistence is provided but requires setup. Watch this space.


