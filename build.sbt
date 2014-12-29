
organization := "uk.co.itadinanta" 

name := "rnkr"

version := "0.1"

scalaVersion := "2.11.4"

EclipseKeys.classpathTransformerFactories := Seq(ClasspathentryTransformer)

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.eclipseOutput := Some("target")

retrieveManaged := false

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.1.3" % "test",
  "org.easytesting" % "fest-assert" % "1.4" % "test",
  "org.slf4j" % "slf4j-api" % "1.6.6",
  "ch.qos.logback" % "logback-classic" % "1.0.7",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.3"
)