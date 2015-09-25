import Versions._

net.virtualvoid.sbt.graph.Plugin.graphSettings

name := "rnkr"

val commonSettings = Seq(
	organization := "net.itadinanta",
	startYear := Some(2015),
	crossPaths := true,
	licenses += ("GPL-2.0", url("http://opensource.org/licenses/GPL-2.0")),
	sbtVersion := "0.13.9",
	scalaVersion := "2.11.7",
	scalacOptions += "-target:jvm-1.8",
	fork := false,
	javaOptions in test += "-Xmx8G",

	releaseVersionBump := sbtrelease.Version.Bump.Minor,
	
	resolvers ++= Seq(
		Resolver.jcenterRepo,
		"Springsource" at "http://repo.springsource.org/libs",
		"Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
	),

	// bintray
	bintrayOrganization := Some("itadinanta"),
	bintrayReleaseOnPublish in ThisBuild := false,
	bintrayPackageLabels := Seq("scala", "rnkr", "leaderboard", "games", "akka", "sharding", "scala-2.11"),

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
		"org.scalatest"				%% "scalatest"		% "2.2.5" % "test"
	),
	
	// for Maven Central
	homepage := Some(url("http://itadinanta.net")),
	pomExtra :=
		<scm>
			<url>git@github.com:itadinanta/{name.value}.git</url>
			<developerConnection>scm:git:git@github.com:itadinanta/{name.value}.git</developerConnection>
			<connection>scm:git:git@github.com:itadinanta/{name.value}.git</connection>
		</scm>
		<developers>
			<developer>
				<name>Nico Orru (norru)</name>
				<email>nigu.orru@gmail.com</email>
				<organization>Itadinanta</organization>
				<organizationUrl>http://itadinanta.net</organizationUrl>
			</developer>
		</developers>
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
	.enablePlugins(JavaServerAppPackaging)


lazy val `rnkr-frontend` = project.in( file("rnkr-frontend") ).settings(commonSettings: _*)
	.dependsOn(
		`rnkr-core`,
		`rnkr-engine`,
		`rnkr-cluster`
	)

lazy val `rnkr-cluster` = project.in( file("rnkr-cluster") ).settings(commonSettings: _*)
	.dependsOn(
		`rnkr-engine`
	)

lazy val `rnkr-engine` = project.in( file("rnkr-engine") ).settings(commonSettings: _*)
	.dependsOn(
		`rnkr-core`,
		`rnkr-support`
	)

lazy val `rnkr-testlib` = project.in( file("rnkr-testlib") ).settings(commonSettings: _*)
lazy val `rnkr-test` = project.in( file("rnkr-test") ).settings(commonSettings: _*)
	
lazy val `rnkr-support` = project.in( file("rnkr-support") ).settings(commonSettings: _*)
lazy val `rnkr-core` = project.in( file("rnkr-core") ).settings(commonSettings: _*)
