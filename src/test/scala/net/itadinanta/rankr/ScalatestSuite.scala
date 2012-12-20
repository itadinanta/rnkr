package net.itadinanta.rankr;

import org.scalatest.Spec
import org.fest.assertions.Assertions._
import org.scalatest.FlatSpec

class ScalatestSuite extends FlatSpec {
	"A truism" should "always pass" in {
		assertThat(true).isTrue
	}
}