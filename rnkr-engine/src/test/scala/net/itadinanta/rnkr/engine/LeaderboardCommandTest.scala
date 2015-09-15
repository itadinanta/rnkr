package net.itadinanta.rnkr.engine

import org.scalatest.Matchers
import org.scalatest.FunSuite
import Leaderboard._
import grizzled.slf4j.Logging
import scala.reflect._
import scala.collection.Seq
class LeaderboardCommandTest extends FunSuite with Matchers with Logging {

	test("Verify lb message type tags") {
		Size().tag shouldBe classTag[Int]
		IsEmpty().tag shouldBe classTag[Boolean]
		Lookup("").tag shouldBe classTag[Seq[Entry]]
		Get(0, 0).tag shouldBe classTag[Option[Entry]]
		At(0).tag shouldBe classTag[Option[Entry]]
		EstimatedRank(0).tag shouldBe classTag[Long]
		Nearby("", 0).tag shouldBe classTag[Seq[Entry]]
		Around(0, 0).tag shouldBe classTag[Seq[Entry]]
		Page(0, 0).tag shouldBe classTag[Seq[Entry]]

		Export().tag shouldBe classTag[Snapshot]

		PostScore(Post(0, "", None)).tag shouldBe classTag[Update]
		Remove("").tag shouldBe classTag[Update]
		Clear().tag shouldBe classTag[Update]
	}
}