resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-contrib" % V.akka,
	"com.typesafe.akka" %% "akka-cluster-sharding" % V.akka,
	"com.typesafe.akka" %% "akka-distributed-data-experimental" % V.akka,

	"com.typesafe.akka" %% "akka-multi-node-testkit" % V.akka % "test"
)
