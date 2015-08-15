import Versions._
libraryDependencies ++= Seq(
	// frontend
	"io.spray"					%% "spray-can"		% sprayV,
	"io.spray"					%% "spray-routing"	% sprayV,
	"io.spray"					%% "spray-json"		% "1.3.1",

	// compression
	"net.jpountz.lz4"			% "lz4"				% "1.3.0",
	"org.xerial.snappy"			% "snappy-java"		% "1.1.1.6",

	// test
	"io.spray"					%% "spray-testkit"	% sprayV	% "test"
)