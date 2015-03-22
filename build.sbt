
organization := "net.itadinanta" 

name := "rnkr"

version := "0.1"

scalaVersion := "2.11.6"

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
	"repository.springsource.milestone" at "http://repo.springsource.org/libs",
	"Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
)

EclipseKeys.classpathTransformerFactories := Seq(ClasspathentryTransformer)

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.eclipseOutput := Some("target")

retrieveManaged := false

val akkaV = "2.3.8"
val sprayV = "1.3.2"

libraryDependencies ++= Seq(
	// logging
	"org.slf4j" 				% "slf4j-api"		% "1.7.7",
	"org.slf4j" 				% "jcl-over-slf4j"	% "1.7.7",
	"org.clapper"				%% "grizzled-slf4j"	% "1.0.2",
	"ch.qos.logback" 			% "logback-classic" % "1.1.2",

	// framework
	"org.springframework.scala" %% "spring-scala"	% "1.0.0.RC3",
	"org.scalaz"				%% "scalaz-core"	% "7.1.1",
	"com.typesafe.akka"			%% "akka-actor"		% akkaV,

	// frontend
	"io.spray"					%% "spray-can"		% sprayV,
	"io.spray"					%% "spray-routing"	% sprayV,
	"io.spray"					%% "spray-json"		% "1.3.1",

	// backend
	"com.datastax.cassandra"	% "cassandra-driver-core" % "2.1.4",

	// compression
	"net.jpountz.lz4"			% "lz4"				% "1.3.0",
	"org.xerial.snappy"			% "snappy-java"		% "1.1.1.6",

	// test
	"org.scalatest"				%% "scalatest"		% "2.1.3"	% "test",
	"io.spray"					%% "spray-testkit"	% sprayV	% "test",
	"com.typesafe.akka"			%% "akka-testkit"	% akkaV		% "test"
)