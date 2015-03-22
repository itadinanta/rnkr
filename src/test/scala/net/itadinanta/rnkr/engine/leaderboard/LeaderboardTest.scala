package net.itadinanta.rnkr.engine.leaderboard

import org.scalatest.Matchers
import org.scalatest.FunSuite

class LeaderboardTest extends FunSuite with Matchers {

	test("empty leaderboard") {
		Leaderboard().size should be === 0
	}

	test("TimedScore as key") {
		val v1 = TimedScore(0, 1)
		val v2 = TimedScore(0, 1)

		v1 should be(v2)
		v1.hashCode should be(v2.hashCode)
	}

	test("Simple insert and retrieval") {
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

	test("Simple insert and update") {
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

	test("Simple insert and delete") {
		val lb = Leaderboard()

		val posted = lb.post(Post(0, "Me", None))
		lb.size should be(1)

		val notDeleted = lb.remove("You")
		notDeleted.oldEntry should be(None)
		notDeleted.newEntry should be(None)

		val deleted = lb.remove("Me")
		deleted.oldEntry should be(posted.newEntry)
		deleted.newEntry should be(None)

		lb.get("Me").headOption should be(None)
		lb.size should be(0)
		lb.isEmpty should be(true)
	}

	val lb = Leaderboard()
	val posted = (for {
		i <- 1 to 100
		post <- lb.post(Post(i, s"User${i}", None)).newEntry
	} yield post).toList

	test("Query: size") {
		lb.size should be(100)
	}

	test("Query: around (entrant)") {
		lb.around("User10", 2) should be(posted.drop(7).take(5).toList)
		lb.around("UserNone", 2) should be('empty)
	}

	test("Query: around (score)") {
		lb.around(10, 2) should be(posted.drop(7).take(5).toList)
	}

	test("Query: at") {
		lb.at(10) should be(Some(posted(10)))
	}

	test("Query: page") {
		lb.page(10, 5) should be(posted.drop(10).take(5).toList)
	}

	test("Query: estimatedRank") {
		lb.estimatedRank(10) should be(9)
		lb.estimatedRank(0) should be(0)
		lb.estimatedRank(100) should be(99)
		lb.estimatedRank(101) should be(100)
		lb.estimatedRank(110) should be(100)

	}

}