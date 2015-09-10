package net.itadinanta.rnkr.engine

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.Promise
import akka.actor._
import akka.pattern._
import scala.concurrent.duration._
import net.itadinanta.rnkr.backend._
import Leaderboard._
import net.itadinanta.rnkr.core.arbiter.Gate

class PersistentLeaderboard(name: String, datastore: Datastore, actorRefFactory: ActorRefFactory)
		extends LeaderboardBuffer.Factory {
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
						target -> cmd flatMap { update =>
							if (update.hasChanged) {
								(writer ? WriteAheadLog(replayMode, update.timestamp, post)) map { _ =>
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

					override def decorate[T] = {
						case c @ PostScore(post, updateMode) => writeAheadLog(c, ReplayMode(updateMode), post)
						case c @ Remove(entrant) => writeAheadLog(c, ReplayMode.Delete, Storage.tombstone(entrant))
						case c @ Clear() => writeAheadLog(c, ReplayMode.Clear, Storage.tombstone())
					}

					def flush() = (target -> Export()) onSuccess { case snapshot => self ! Flush(snapshot) }
				}

				arbiter.success(leaderboard)

			case Flush(snapshot) =>
				writer ! Save(snapshot)

		}
	}

	val lifecycle = actorRefFactory.actorOf(Props(new LifecycleActor), "lifecycle_" + name)
}