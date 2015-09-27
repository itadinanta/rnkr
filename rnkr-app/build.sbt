import com.typesafe.sbt.packager.SettingsHelper._

enablePlugins(JavaServerAppPackaging)

libraryDependencies ++= Seq(
	"ch.qos.logback" 			% "logback-classic" % "1.1.2",
	
	"net.itadinanta" 			%%  "spring-scala"	% "1.0.0"
)

makeDeploymentSettings(Universal, packageBin in Universal, "tgz")

val packageTar = taskKey[File]("package-tar")

publish <<= publish dependsOn (packageZipTarball in Universal)
publishM2 <<= publishM2 dependsOn (packageZipTarball in Universal)
publishLocal <<= publishLocal dependsOn (packageZipTarball in Universal)

packageTar := (baseDirectory in Compile).value / "target" / "universal" / (name.value + "-" + version.value + ".tgz")
artifact in (Universal, packageTar) ~= { _.copy(`type` = "arch", extension = "tar.gz", classifier = Some("package")) }
addArtifact(artifact in (Universal, packageTar), packageTar in Universal)

