import Versions._

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies ++= Seq(
//	"com.typesafe.akka" %% "akka-cluster" % akkaV,
//	"com.typesafe.akka" %% "akka-cluster-sharding" % akkaV,
//	"com.typesafe.akka" %% "akka-persistence" % akkaV,
	"com.typesafe.akka" %% "akka-contrib" % akkaV,
	"com.typesafe.akka" %% "akka-multi-node-testkit" % akkaV % "test",

	"com.github.krasserm" %% "akka-persistence-cassandra" % "0.3.9",
	"com.github.jdgoldie" %% "akka-persistence-shared-inmemory" % "1.0.15"

//	"org.iq80.leveldb" % "leveldb" % "0.7" % "runtime",
//	"org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % "runtime"
)
