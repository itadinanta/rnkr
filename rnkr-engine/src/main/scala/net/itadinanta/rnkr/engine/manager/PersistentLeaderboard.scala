package net.itadinanta.rnkr.engine.manager

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import net.itadinanta.rnkr.backend.cassandra.Cassandra
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBuffer
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardArbiter
import scala.concurrent.Promise
import net.itadinanta.rnkr.core.arbiter.Gate
import net.itadinanta.rnkr.core.tree.Row
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard
import net.itadinanta.rnkr.backend.Load
import akka.actor.PoisonPill
import akka.actor.ActorRef
import net.itadinanta.rnkr.backend.Load
import net.itadinanta.rnkr.engine.leaderboard.UpdateMode
import net.itadinanta.rnkr.engine.leaderboard.Post
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardDecorator
import net.itadinanta.rnkr.backend.WriteAheadLog
import scala.concurrent.duration._
import akka.pattern.ask
import akka.pattern.pipe
import net.itadinanta.rnkr.engine.leaderboard.Update
import net.itadinanta.rnkr.backend.ReplayMode
import net.itadinanta.rnkr.backend.Storage
import net.itadinanta.rnkr.backend.Flush
import net.itadinanta.rnkr.backend.Save
import net.itadinanta.rnkr.backend.Metadata
import scala.reflect.ClassTag
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBufferFactory
import net.itadinanta.rnkr.backend.Datastore

class PersistentLeaderboard(name: String, datastore: Datastore, actorRefFactory: ActorRefFactory)
		extends LeaderboardBufferFactory {
	implicit val executionContext = actorRefFactory.dispatcher
	val arbiter = Promise[Leaderboard]
	def leaderboard: Future[Leaderboard] = arbiter.future

	class LifecycleActor extends Actor {
		var metadata = Metadata()
		var lastFlush = System.currentTimeMillis()
		val target = build()
		var writer: ActorRef = _

		val reader = context.actorOf(datastore.readerProps(name, target), "reader_" + name)
		reader ! Load

		def receive = {
			case Load(watermark, walLength, metadata) =>
				import UpdateMode._
				this.metadata = metadata
				this.writer = context.actorOf(datastore.writerProps(name, watermark, metadata), "writer_" + name)
				val leaderboard = new LeaderboardDecorator(LeaderboardArbiter.wrap(context.actorOf(Gate.props(target), "gate_" + name))) {
					var flushCount: Int = walLength
					import scala.concurrent.duration._
					implicit val timeout = Timeout(DurationInt(1).minute)

					import Leaderboard._

					def writeAheadLog(cmd: Write, replayMode: ReplayMode.ReplayMode, post: Post) =
						super.->(cmd) flatMap { update =>
							if (update.hasChanged) {
								writer ask WriteAheadLog(replayMode, update.timestamp, post) map { _ =>
									flushCount += 1
									if (flushCount > metadata.walSizeLimit) {
										flush()
										flushCount = 0
									}
									update
								}
							} else {
								// no changes, don't bother updating
								Future.successful(update)
							}
						}

					override def ->[T](cmd: Command[T]) = cmd match {
						case c @ PostScore(post, updateMode) => writeAheadLog(c, ReplayMode(updateMode), post)
						case c @ Remove(entrant) => writeAheadLog(c, ReplayMode.Delete, Storage.tombstone(entrant))
						case c @ Clear() => writeAheadLog(c, ReplayMode.Clear, Storage.tombstone())

						case c => super.->(c)
					}

					def flush() = (super.->(Export())) onSuccess { case snapshot => self ! Flush(snapshot) }
				}

				arbiter.success(leaderboard)

			case Flush(snapshot) =>
				writer ! Save(snapshot)

		}
	}

	val lifecycle = actorRefFactory.actorOf(Props(new LifecycleActor), "lifecycle_" + name)
}