import Versions._
libraryDependencies ++= Seq(
	"com.typesafe.akka"			%% "akka-actor"		% akkaV,

	// backend
	"com.datastax.cassandra"	% "cassandra-driver-core" % "2.1.4",

	// compression
	"net.jpountz.lz4"			% "lz4"				% "1.3.0",
	"org.xerial.snappy"			% "snappy-java"		% "1.1.1.6",

	// test
	"com.typesafe.akka"			%% "akka-testkit"	% akkaV		% "test"
)