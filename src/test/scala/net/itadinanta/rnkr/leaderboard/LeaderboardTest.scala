package net.itadinanta.rnkr.leaderboard

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import UpdateMode._

class LeaderboardTest extends FlatSpec with Matchers {
	"TimedScore" should "be value identical" in {
		val v1 = TimedScore(0, 1)
		val v2 = TimedScore(0, 1)

		v1 should be(v2)
		v1.hashCode should be(v2.hashCode)
	}

	"An empty leaderboard" should "contain no entries" in {
		Leaderboard().size should be === 0
	}

	"A leaderboard" should "contain one entry after insert" in {
		val lb = Leaderboard()

		val posted = lb.post(Post(0, "Me", None))
		lb.size should be(1)

		posted.oldEntry should be(None)

		posted.newEntry should not be (None)
		posted.newEntry.get.timestamp should be > 0L
		posted.newEntry foreach { e =>
			lb.at(0) foreach { _ should be(Entry(e.score, e.timestamp, e.entrant, 0, None)) }
		}

		lb.get("Me").headOption should be(posted.newEntry)
		lb.get("You").headOption should be(None)

		lb.at(0) should be(posted.newEntry)
		lb.at(1) should be(None)
	}

	"A leaderboard" should "contain one entry after insert and update" in {
		val lb = Leaderboard()

		val posted = lb.post(Post(10, "Me", None))
		lb.size should be(1)
		val updated = lb.post(Post(9, "Me", None))

		updated.oldEntry should be(posted.newEntry)
		updated.newEntry should not be (None)
		updated.newEntry foreach { e =>
			lb.at(0) foreach { _ should be(Entry(e.score, e.timestamp, e.entrant, 0, None)) }
		}

		lb.get("Me").headOption should be(updated.newEntry)
		lb.at(0) should be(updated.newEntry)
	}

	"A leaderboard" should "be empty after deletes" in {
		val lb = Leaderboard()

		val posted = lb.post(Post(0, "Me", None))
		lb.size should be(1)

		val notDeleted = lb.delete("You")
		notDeleted.oldEntry should be(None)
		notDeleted.newEntry should be(None)

		val deleted = lb.delete("Me")
		deleted.oldEntry should be(posted.newEntry)
		deleted.newEntry should be(None)

		lb.get("Me").headOption should be(None)
		lb.size should be(0)
		lb.isEmpty should be(true)

	}

}