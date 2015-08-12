
organization := "net.itadinanta" 
name := "rnkr"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.7"

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
	"repository.springsource.milestone" at "http://repo.springsource.org/libs",
	"jitpack.io" at "https://jitpack.io",
	"Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
)

EclipseKeys.classpathTransformerFactories := Seq(ClasspathentryTransformer)
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
EclipseKeys.eclipseOutput := Some("target")

unmanagedSourceDirectories in Compile := (scalaSource in Compile).value :: Nil
unmanagedSourceDirectories in Test := (scalaSource in Test).value :: Nil

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
	"com.github.norru" 			%  "spring-scala"	% "1.0.0",
	"org.scalaz"				%% "scalaz-core"	% "7.1.1",
	"com.typesafe.akka"			%% "akka-actor"		% akkaV,
	"com.typesafe.akka"			%% "akka-cluster"	% akkaV,

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

enablePlugins(DockerPlugin)

docker <<= docker.dependsOn(Keys.`package`.in(Compile, packageBin))

dockerfile in docker := {
  val jarFile = artifactPath.in(Compile, packageBin).value
  val classpath = (managedClasspath in Compile).value
  val mainclass = mainClass.in(Compile, packageBin).value.getOrElse(sys.error("Expected exactly one main class"))
  val jarTarget = s"/app/${jarFile.getName}"

  // Make a colon separated classpath with the JAR file
  val classpathString = classpath.files.map("/app/" + _.getName).mkString(":") + ":" + jarTarget

  new Dockerfile {
    // Base image
    from("java")
    // Add all files on the classpath
    add(classpath.files, "/app/")
    // Add the JAR file
    add(jarFile, jarTarget)
    // On launch run Java with the classpath and the main class
    entryPoint("java", "-cp", classpathString, mainclass)
  }
}