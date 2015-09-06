package net.itadinanta.rnkr.engine.leaderboard

import org.scalatest.Matchers
import org.scalatest.FunSuite
import UpdateMode._
import scala.util.Random
import scala.collection.mutable._
import grizzled.slf4j.Logging

class LeaderboardTest extends FunSuite with Matchers with Logging {

	test("empty leaderboard") {
		LeaderboardBuffer().size shouldBe 0
	}

	test("TimedScore as key") {
		val v1 = TimedScore(0, 1)
		val v2 = TimedScore(0, 1)

		v1 should be(v2)
		v1.hashCode should be(v2.hashCode)
	}

	test("Simple insert and retrieval") {
		val lb = LeaderboardBuffer()

		val posted = lb.post(Post(0, "Me", None))
		lb.size should be(1)

		posted.oldEntry should be(None)

		posted.newEntry should not be (None)
		posted.newEntry.get.timestamp should be > 0L
		posted.newEntry foreach { e =>
			lb.at(0) foreach { _ should be(Entry(e.score, e.timestamp, e.entrant, 0, None)) }
		}

		lb.lookup("Me").headOption should be(posted.newEntry)
		lb.lookup("You").headOption should be(None)

		lb.at(0) should be(posted.newEntry)
		lb.at(1) should be(None)
	}

	test("Simple insert and update") {
		val lb = LeaderboardBuffer()

		val posted = lb.post(Post(10, "Me", None))
		val updated = lb.post(Post(9, "Me", None))

		lb.size should be(1)

		updated.oldEntry should be(posted.newEntry)
		updated.newEntry should be(lb.at(0))
		updated.newEntry should not be (None)

		lb.lookup("Me").headOption should be(updated.newEntry)
		lb.at(0) should be(updated.newEntry)

		val p1 = lb.post(Post(10, "Me", None))
		p1 should be(Update(p1.timestamp, false, updated.newEntry, updated.newEntry))

		val p2 = lb.post(Post(10, "Me", None), LastWins)
		p2 should be(Update(p2.timestamp, true, updated.newEntry, lb.at(0)))
	}

	test("Simple insert and delete") {
		val lb = LeaderboardBuffer()

		val posted = lb.post(Post(0, "Me", None))
		lb.size should be(1)

		val notDeleted = lb.remove("You")
		notDeleted.oldEntry should be(None)
		notDeleted.newEntry should be(None)

		val deleted = lb.remove("Me")
		deleted.oldEntry should be(posted.newEntry)
		deleted.newEntry should be(None)

		lb.lookup("Me").headOption should be(None)
		lb.size should be(0)
		lb.isEmpty should be(true)
	}

	val lb = LeaderboardBuffer()
	val posted = (for {
		i <- 1 to 100
		post <- lb.post(Post(i, s"User${i}", None)).newEntry
	} yield post).toList

	test("Query: size") {
		lb.size should be(100)
	}

	test("Query: around (entrant)") {
		lb.nearby("User10", 2) should be(posted.drop(7).take(5).toList)
		lb.nearby("UserNone", 2) should be('empty)
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

	test("After 1000000 sequential appends should contain 1000000 entries in order") {
		val large = LeaderboardBuffer()

		large.append(for (i <- 1 to 1000000) yield Entry(i, i, "Item" + i, i, None))

		large.size should be(1000000)
	}

	test("After 1000000 sequential insertions should contain 1000000 entries in order") {
		val large = LeaderboardBuffer()
		for (i <- 1 to 1000000) {
			large.post(Post(i, "Item" + i, None))
		}
		large.size should be(1000000)
	}

	test("After 1000000 random insertions should contain 1000000 entries in order") {
		val large = LeaderboardBuffer()
		val ordered = new TreeSet[Int]
		Random.setSeed(1234L)
		Random.shuffle(1 to 1000000 map { i => i }) foreach { i =>
			large.post(Post(i, "Item" + i, None))
			ordered += i
		}
		debug(large)
		large.size should be(1000000)
	}

	test("After 1000000 insertions of the same value it should contain 1000000 entries in order") {
		val large = LeaderboardBuffer()
		val ordered = new TreeSet[Int]
		Random.setSeed(1234L)
		Random.shuffle(1 to 1000000 map { i => i }) foreach { i =>
			large.post(Post(1, "Item" + i, None))
			ordered += i
		}
		debug(large)
		large.size should be(1000000)
	}

}