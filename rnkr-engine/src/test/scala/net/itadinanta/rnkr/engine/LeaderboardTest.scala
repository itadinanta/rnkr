package net.itadinanta.rnkr.engine

import org.scalatest.Matchers
import org.scalatest.FunSuite
import Leaderboard._
import Leaderboard.UpdateMode._
import scala.util.Random
import scala.collection.mutable._
import grizzled.slf4j.Logging
import akka.actor.ActorSystem
import akka.actor.ActorRefFactory
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.Future
import org.scalatest.time.Span
import org.scalatest.time.Millis

class LeaderboardTest extends FunSuite with Matchers with ScalaFutures with Logging with LeaderboardBuffer.Factory {

	val actorSystem = ActorSystem("rnkr")
	implicit val executionContext = actorSystem.dispatcher
	implicit val actorRefFactory: ActorRefFactory = actorSystem
	implicit val config: PatienceConfig = PatienceConfig(scaled(Span(10000, Millis)), scaled(Span(50, Millis)))
	def build(name: String): Leaderboard = ConcurrentLeaderboard(build(), name)

	test("empty leaderboard") {
		val e = build("empty")
		whenReady(e -> Size()) { _ shouldBe 0 }
	}

	test("Simple insert and retrieval") {
		val lb = build("insert")

		val posted = lb -> PostScore(Post(0, "Me", None))
		whenReady(lb -> Size()) { _ shouldBe (1) }

		whenReady(posted) { posted =>
			posted.oldEntry should be(None)
			posted.newEntry should not be (None)
			posted.newEntry.get.timestamp should be > 0L
			posted.newEntry foreach { e =>
				whenReady(lb -> At(0)) { _ should be(Some(Entry(e.score, e.timestamp, e.entrant, 0, None))) }
			}
		}

		whenReady(for { f1 <- lb -> Lookup("Me"); f2 <- posted } yield (f1, f2)) {
			case (f1, f2) => f1.headOption shouldBe f2.newEntry
		}
		whenReady(lb -> Lookup("You")) {
			_.headOption should be(None)
		}

		whenReady(for { f1 <- lb -> Lookup("Me"); f2 <- posted } yield (f1, f2)) {
			case (f1, f2) => f1.headOption shouldBe f2.newEntry
		}
		whenReady(lb -> At(1)) {
			_ shouldBe (None)
		}
	}

	test("Simple insert and update") {
		val lb = build("update")

		val posted = lb -> PostScore(Post(10, "Me", None))
		val updated = lb -> PostScore(Post(9, "Me", None))

		whenReady(for { f1 <- posted; f2 <- updated } yield (f1, f2)) {
			case (posted, updated) =>
				whenReady(for {
					s <- lb -> Size()
					f3 <- lb -> Lookup("Me")
					f4 <- lb -> At(0)
				} yield (s, f3, f4)) {
					case (s, me, at0) =>
						s shouldBe 1
						updated.oldEntry should be(posted.newEntry)
						updated.newEntry should be(at0)
						updated.newEntry should not be (None)
						me.headOption should be(updated.newEntry)
						at0 should be(updated.newEntry)
						whenReady(lb -> PostScore(Post(10, "Me", None))) { p1 =>
							p1 should be(Update(p1.timestamp, false, updated.newEntry, updated.newEntry))
						}
						whenReady(for {
							f1 <- lb -> PostScore(Post(10, "Me", None), LastWins)
							f2 <- lb -> At(0)
						} yield (f1, f2)) {
							case (p2, at0) =>
								p2 should be(Update(p2.timestamp, true, updated.newEntry, at0))
						}
				}
		}
	}

	test("Simple insert and delete") {
		val lb = build("delete")

		val posted = lb -> PostScore(Post(0, "Me", None))
		whenReady(lb -> Size()) { _ should be(1) }

		whenReady(lb -> Remove("You")) { notDeleted =>
			notDeleted.oldEntry should be(None)
			notDeleted.newEntry should be(None)
		}

		whenReady(for {
			d <- lb -> Remove("Me")
			p <- posted
		} yield (d, p)) {
			case (deleted, posted) =>
				deleted.oldEntry should be(posted.newEntry)
				deleted.newEntry should be(None)
				whenReady(lb -> Lookup("Me")) { _.headOption should be(None) }
				whenReady(lb -> Size()) { _ should be(0) }
				whenReady(lb -> IsEmpty()) { _ should be(true) }
		}

	}

	val lb = build("hundred")
	val posted = Future.sequence(for (i <- 1 to 100) yield lb -> PostScore(Post(i, s"User${i}", None)) map { _.newEntry.get })

	test("Query: size") {
		whenReady(posted) { posted =>
			whenReady(lb -> Size()) { _ should be(100) }
		}
	}

	test("Query: around (entrant)") {
		whenReady(posted) { posted =>
			whenReady(lb -> Nearby("User10", 2)) { _ should be(posted.drop(7).take(5).toList) }
			whenReady(lb -> Nearby("UserNone", 2)) { _ should be('empty) }
		}
	}

	test("Query: around (score)") {
		whenReady(posted) { posted =>
			whenReady(lb -> Around(10, 2)) { _ should be(posted.drop(7).take(5).toList) }
		}
	}

	test("Query: at") {
		whenReady(posted) { posted =>
			whenReady(lb -> At(10)) { _ should be(Some(posted(10))) }
		}
	}

	test("Query: page") {
		whenReady(posted) { posted =>
			whenReady(lb -> Page(10, 5)) { _ should be(posted.drop(10).take(5).toList) }
		}
	}

	test("Query: estimatedRank") {
		whenReady(posted) { posted =>
			whenReady(lb -> EstimatedRank(10)) { _ should be(9) }
			whenReady(lb -> EstimatedRank(0)) { _ should be(0) }
			whenReady(lb -> EstimatedRank(100)) { _ should be(99) }
			whenReady(lb -> EstimatedRank(101)) { _ should be(100) }
			whenReady(lb -> EstimatedRank(110)) { _ should be(100) }
		}
	}

	val largeCount = 10000

	test(s"After ${largeCount} sequential insertions should contain ${largeCount} entries in order") {
		val large = build("largeInsert")
		val posts = for (i <- 1 to largeCount) yield { large -> PostScore(Post(i, "Item" + i, None)) }
		whenReady(Future.sequence(posts)) { posts =>
			whenReady(large -> Size()) { _ should be(largeCount) }
		}
	}

	test(s"After ${largeCount} random insertions should contain ${largeCount} entries in order") {
		val large = build("largeRandomInsert")
		val ordered = new TreeSet[Int]
		Random.setSeed(1234L)
		val posts = Random.shuffle(1 to largeCount map { i => i }) map { i =>
			ordered += i
			large -> PostScore(Post(i, "Item" + i, None))
		}
		whenReady(Future.sequence(posts)) { posts =>
			debug(large)
			whenReady(large -> Size()) { _ should be(largeCount) }
		}
	}

	test(s"After ${largeCount} insertions of the same value it should contain ${largeCount} entries in order") {
		val large = build("largeSameInsert")
		val ordered = new TreeSet[Int]
		Random.setSeed(1234L)
		val posts = Random.shuffle(1 to largeCount map { i => i }) map { i =>
			ordered += i
			large -> PostScore(Post(1, "Item" + i, None))
		}
		whenReady(Future.sequence(posts)) { posts =>
			debug(large)
			whenReady(large -> Size()) { _ should be(largeCount) }
		}
	}

}