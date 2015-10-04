libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-actor" % V.akka,
	"com.typesafe.akka" %% "akka-slf4j" % V.akka,

	// backend
	"com.datastax.cassandra" % "cassandra-driver-core" % V.Cassandra.driver,

	// compression
	"net.jpountz.lz4" % "lz4" % V.Cassandra.lz4,
	"org.xerial.snappy" % "snappy-java" % V.Cassandra.snappy,

	// test
	"com.typesafe.akka" %% "akka-testkit" % V.akka % "test"
)