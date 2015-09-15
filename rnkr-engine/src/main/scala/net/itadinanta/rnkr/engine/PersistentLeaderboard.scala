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
import scala.concurrent.ExecutionContext
import net.itadinanta.rnkr.util.SetOnce

object PersistentLeaderboard {
	case class Get()

	private class PersistentLeaderboardDecorator(
			override val target: Leaderboard,
			walLength: Int,
			metadata: Metadata,
			writer: ActorRef,
			parent: ActorRef,
			implicit val executionContext: ExecutionContext) extends Leaderboard.Decorator {
		var flushCount: Int = walLength
		import scala.concurrent.duration._
		implicit val timeout = Timeout(DurationInt(1).minute)

		private def writeAheadLog(cmd: Write, replayMode: ReplayMode.Value, post: Post) =
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
		def flush() = (target -> Export()) onSuccess { case snapshot => parent ! Flush(snapshot) }

		override def decorate[T] = {
			case c @ PostScore(post, updateMode) => writeAheadLog(c, ReplayMode(updateMode), post)
			case c @ Remove(entrant) => writeAheadLog(c, ReplayMode.Delete, Storage.tombstone(entrant))
			case c @ Clear() => writeAheadLog(c, ReplayMode.Clear, Storage.tombstone())
		}

	}

	class PersistentLeaderboardActor(name: String, datastore: Datastore) extends Actor
			with LeaderboardBuffer.Factory {

		val buffer = build()
		val writer = SetOnce[ActorRef]
		val leaderboard = SetOnce[Leaderboard]
		var receivers = Seq[ActorRef]()

		context.actorOf(datastore.readerProps(name, buffer), "reader_" + name) ! Load

		def receive = {
			case Load(watermark, walLength, metadata) =>
				writer := context.actorOf(datastore.writerProps(name, watermark, metadata), "writer_" + name)
				leaderboard := new PersistentLeaderboardDecorator(
					LeaderboardArbiter(buffer, "gate_" + name),
					walLength,
					metadata,
					writer.get,
					self,
					context.dispatcher)
				receivers foreach (_ ! leaderboard.get)

			case Flush(snapshot) =>
				if (writer.isSet) writer.get ! Save(snapshot)

			case Get() =>
				if (leaderboard.isSet) sender() ! leaderboard.get
				else receivers = sender() +: receivers
		}
	}

	def props(name: String, datastore: Datastore) = Props(new PersistentLeaderboardActor(name: String, datastore: Datastore))
}

class PersistentLeaderboard(name: String, datastore: Datastore, actorRefFactory: ActorRefFactory)
		extends LeaderboardBuffer.Factory {
	implicit val timeout = Timeout(DurationInt(1).minute)
	implicit val executionContext = actorRefFactory.dispatcher
	val persistentLeaderboard = actorRefFactory.actorOf(PersistentLeaderboard.props(name, datastore), "persistent_" + name)
	val leaderboard: Future[Leaderboard] = (persistentLeaderboard ? PersistentLeaderboard.Get()).mapTo[Leaderboard]

}