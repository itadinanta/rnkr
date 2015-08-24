import Versions._
libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-cluster" % akkaV,
	"com.typesafe.akka" %% "akka-cluster-sharding" % akkaV,
    "com.typesafe.akka" %% "akka-persistence" % akkaV,
    
    "org.iq80.leveldb" % "leveldb" % "0.7" % "runtime",
    "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % "runtime"
)
