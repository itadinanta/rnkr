libraryDependencies ++= Seq(
	// frontend
	"io.spray" %% "spray-can" % V.spray,
	"io.spray" %% "spray-routing" % V.spray,
	"io.spray" %% "spray-json" % V.spray,

	// compression	// test
	"io.spray" %% "spray-testkit" % V.spray % "test"
)