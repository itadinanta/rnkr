import Versions._

organization := "net.itadinanta"
version := "0.2.0-SNAPSHOT"

net.virtualvoid.sbt.graph.Plugin.graphSettings

val commonSettings = Seq(
	scalaVersion := "2.11.7",
	resolvers ++= Seq(
		"Springsource" at "http://repo.springsource.org/libs",
		"JitPack.io" at "https://jitpack.io",
		"Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
	),

	EclipseKeys.classpathTransformerFactories := Seq(ClasspathentryTransformer),
	EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource,
	EclipseKeys.eclipseOutput := Some("target"),

	unmanagedSourceDirectories in Compile := (scalaSource in Compile).value :: Nil,
	unmanagedSourceDirectories in Test := (scalaSource in Test).value :: Nil,
	
	retrieveManaged := false,

	libraryDependencies ++= Seq(
		// logging
		"org.slf4j" 				% "slf4j-api"		% "1.7.7",
		"org.slf4j" 				% "jcl-over-slf4j"	% "1.7.7",
		"org.clapper"				%% "grizzled-slf4j"	% "1.0.2",
		"ch.qos.logback" 			% "logback-classic" % "1.1.2" % "test",
	
		// test
		"org.scalatest"				%% "scalatest"		% "2.1.3"	% "test"
	)
)

lazy val rnkr = project.in( file(".") ).settings(commonSettings: _*)
	.settings(
		EclipseKeys.createSrc := EclipseCreateSrc.Unmanaged + EclipseCreateSrc.Resource
	)
	.aggregate(
		`rnkr-core`,
		`rnkr-engine`,
		`rnkr-cluster`,
		`rnkr-frontend`,
		`rnkr-app`
	)

lazy val `rnkr-app` = project.in( file("rnkr-app") ).settings(commonSettings: _*)
	.dependsOn(
		`rnkr-core`,
		`rnkr-engine`,
		`rnkr-cluster`,
		`rnkr-frontend`
	)


lazy val `rnkr-frontend` = project.in( file("rnkr-frontend") ).settings(commonSettings: _*)
	.dependsOn(
		`rnkr-core`,
		`rnkr-engine`,
		`rnkr-cluster`
	)

lazy val `rnkr-cluster` = project.in( file("rnkr-cluster") ).settings(commonSettings: _*)

lazy val `rnkr-engine` = project.in( file("rnkr-engine") ).settings(commonSettings: _*)
	.dependsOn(`rnkr-core`)

lazy val `rnkr-testlib` = project.in( file("rnkr-testlib") ).settings(commonSettings: _*)
lazy val `rnkr-test` = project.in( file("rnkr-test") ).settings(commonSettings: _*)
	
lazy val `rnkr-support` = project.in( file("rnkr-support") ).settings(commonSettings: _*)
lazy val `rnkr-core` = project.in( file("rnkr-core") ).settings(commonSettings: _*)

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

