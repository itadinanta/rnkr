package net.itadinanta.rnkr.engine

import scala.concurrent.Future
import akka.util.Timeout
import scala.concurrent.Promise
import akka.actor._
import akka.pattern._
import scala.concurrent.duration._
import net.itadinanta.rnkr.backend._
import Leaderboard._
import net.itadinanta.rnkr.core.arbiter.Gate
import scala.concurrent.duration._

object PersistentLeaderboard {
	case class Get()

	sealed abstract class LeaderboardDecorator(protected[this] val target: Leaderboard) extends Leaderboard {
		def decorate[T]: PartialFunction[Leaderboard.Command[T], Future[T]]
		override final def ->[T](cmd: Leaderboard.Command[T]): Future[T] = {
			val d = decorate[T]
			if (d.isDefinedAt(cmd)) d.apply(cmd) else target -> cmd
		}
	}

	class PersistentLeaderboardActor(
		name: String,
		datastore: Datastore) extends Actor
			with LeaderboardBuffer.Factory {
		implicit val executionContext = context.dispatcher
		val target = build()
		var writer: Option[ActorRef] = None
		var receivers: Set[ActorRef] = Set()

		val reader = context.actorOf(datastore.readerProps(name, target), "reader_" + name)
		reader ! Load

		def receive = {
			case Load(watermark, walLength, metadata) =>
				import UpdateMode._
				this.writer = Some(context.actorOf(datastore.writerProps(name, watermark, metadata), "writer_" + name))

				val concurrentBuffer = LeaderboardArbiter.wrap(context.actorOf(Gate.props(target), "gate_" + name))

				val leaderboard = new LeaderboardDecorator(concurrentBuffer) {
					var flushCount: Int = walLength
					import scala.concurrent.duration._
					implicit val timeout = Timeout(DurationInt(1).minute)

					import Leaderboard._

					def writeAheadLog(cmd: Write, replayMode: ReplayMode.ReplayMode, post: Post) =
						target -> cmd flatMap { update =>
							if (update.hasChanged) {
								(writer.get ? WriteAheadLog(replayMode, update.timestamp, post)) map { _ =>
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
					def flush() = (target -> Export()) onSuccess { case snapshot => self ! Flush(snapshot) }

					override def decorate[T] = {
						case c @ PostScore(post, updateMode) => writeAheadLog(c, ReplayMode(updateMode), post)
						case c @ Remove(entrant) => writeAheadLog(c, ReplayMode.Delete, Storage.tombstone(entrant))
						case c @ Clear() => writeAheadLog(c, ReplayMode.Clear, Storage.tombstone())
					}

				}
				receivers foreach (_ ! leaderboard)

			case Flush(snapshot) =>
				writer foreach (_ ! Save(snapshot))

			case Get() => if (writer.isDefined)
				writer foreach { sender() ! _ }
			else
				receivers += sender()

		}
	}

	def props(name: String, datastore: Datastore) = Props(new PersistentLeaderboardActor(name: String, datastore: Datastore))
}

class PersistentLeaderboard(name: String, datastore: Datastore, actorRefFactory: ActorRefFactory)
		extends LeaderboardBuffer.Factory {
	implicit val timeout = Timeout(DurationInt(1).minute)
	implicit val executionContext = actorRefFactory.dispatcher
	val lifecycle = actorRefFactory.actorOf(PersistentLeaderboard.props(name, datastore), "persistent_" + name)
	val leaderboard: Future[Leaderboard] = (lifecycle ? PersistentLeaderboard.Get()).mapTo[Leaderboard]

}