
organization := "net.itadinanta" 

name := "rnkr"

version := "0.1"

scalaVersion := "2.11.6"

EclipseKeys.classpathTransformerFactories := Seq(ClasspathentryTransformer)

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.eclipseOutput := Some("target")

retrieveManaged := false

val akkaV = "2.3.8"
val sprayV = "1.3.2"

libraryDependencies ++= Seq(
	"org.easytesting"			% "fest-assert"		% "1.4" % "test",
	"org.slf4j" 				% "slf4j-api"		% "1.6.6",
	"ch.qos.logback" 			% "logback-classic" % "1.0.7",
	"com.typesafe.akka"			%% "akka-actor"		% akkaV,
	"io.spray"					%% "spray-can"		% sprayV,
	"io.spray"					%% "spray-routing"	% sprayV,
	"io.spray"					%% "spray-json"		% "1.3.1",
	"io.spray"					%% "spray-testkit"	% sprayV	% "test",
	"com.typesafe.akka"			%% "akka-testkit"	% akkaV		% "test",
	"com.datastax.cassandra"	% "cassandra-driver-core" % "2.1.4",
	"org.scalatest"				%% "scalatest"		% "2.1.3"	% "test",
	"org.specs2"				%% "specs2-core"	% "2.3.11"	% "test"
)