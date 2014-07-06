
organization := "uk.co.itadinanta" 

name := "rnkr"

version := "0.1"

scalaVersion := "2.10.4"

EclipseKeys.relativizeLibs := true

retrieveManaged := false

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test",
  "org.easytesting" % "fest-assert" % "1.4" % "test",
  "org.slf4j" % "slf4j-api" % "1.6.6",
  "ch.qos.logback" % "logback-classic" % "1.0.7",
  "ch.qos.logback" % "logback-core" % "1.0.7"
)