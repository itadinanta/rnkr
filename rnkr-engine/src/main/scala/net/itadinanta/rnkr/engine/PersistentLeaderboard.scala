package net.itadinanta.rnkr.engine

import scala.concurrent.Future
import scala.concurrent.Promise
import akka.actor._
import akka.pattern._
import net.itadinanta.rnkr.backend._
import Leaderboard._
import net.itadinanta.rnkr.core.arbiter.Gate
import scala.concurrent.ExecutionContext
import net.itadinanta.rnkr.util.SetOnce
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.language.implicitConversions

object PersistentLeaderboard {
	case class Get()
	implicit val timeout = Timeout(1 minute)

	private class PersistentLeaderboardDecorator(
			override val target: Leaderboard,
			walLength: Int,
			metadata: Metadata,
			writer: ActorRef,
			parent: ActorRef,
			implicit val executionContext: ExecutionContext) extends Leaderboard.Decorator {
		var flushCount: Int = walLength
		/**
		 * Writes changes to the WriteAheadLog (event log)
		 *
		 * Once an update has been applied in memory we persist it onto the WriteAheadLog (event log)
		 * once the write ahead log overflows its size limit, we make a snapshot and flush the log.
		 * Snapshot writing is done in the background and we can keep on other operations once
		 * the snapshot of the state was taken.
		 *
		 * Failure to save the snapshot is not catastrophic, as the old snapshot is deleted
		 * and the new one marked as valid only on success.
		 */
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
		def flush() = target -> Export() onSuccess { case snapshot => parent ! Flush(snapshot) }

		override def decorate[T] = {
			case c @ PostScore(post, updateMode) => writeAheadLog(c, ReplayMode(updateMode), post)
			case c @ Remove(entrant) => writeAheadLog(c, ReplayMode.Delete, Storage.tombstone(entrant))
			case c @ Clear() => writeAheadLog(c, ReplayMode.Clear, Storage.tombstone())
		}

	}
	/**
	 * Kernel of the persistence engine.
	 *
	 * In the constructor we fire up a reader and delegate the loading of an existing state
	 * (snapshot + log) to the loader.
	 *
	 * Once the loader is done, it will send back a Loaded(...) message containing a pre-populated
	 * leaderboard buffer and its storage metadata.
	 *
	 * The data is then used to instantiate an a writer and a persistent Leaderboard.
	 * The PersistentLeaderboard adds WAL (event) logging and snapshotting functionality on top
	 * of the transient ConcurrentLeaderboard
	 *
	 * In order to retrieve the Leaderboard instance this actor responds to Get() messages.
	 * While the leaderboard is being created we are unable to respond to the message straight away
	 * so we accumulate the incoming requests into a temporary list which is consumed
	 * once the leaderboard has been created and loaded and becomes available.
	 *
	 * TODO: timeout management and error recovering are nonexistent
	 */
	private class PersistentLeaderboardManager(name: String, datastore: Datastore) extends Actor {
		val writer = SetOnce[ActorRef]
		val leaderboard = SetOnce[Leaderboard]
		var receivers: List[ActorRef] = Nil

		context.actorOf(datastore.readerProps(name), "reader_" + name) ! Load

		// TODO timeout if read doesn't complete
		def receive = {
			case Loaded(buffer, watermark, walLength, metadata) =>
				writer := context.actorOf(datastore.writerProps(name, watermark, metadata), "writer_" + name)
				leaderboard := new PersistentLeaderboardDecorator(
					ConcurrentLeaderboard(buffer, "gate_" + name),
					walLength,
					metadata,
					writer.get,
					self,
					context.dispatcher)
				receivers.reverse foreach (_ ! leaderboard.get)
				receivers = Nil

			case Flush(snapshot) =>
				if (writer.isSet) writer.get ! Save(snapshot)

			case Get() =>
				if (leaderboard.isSet) sender ! leaderboard.get
				else receivers = sender :: receivers
		}
	}
	/** Cameo for the persistent leaderboard manager */
	private def managerProps(name: String, datastore: Datastore) =
		Props(new PersistentLeaderboardManager(name: String, datastore: Datastore))
	/**
	 * Creates a persistent "name" leaderboard using the given datastore to persist its state
	 *
	 * @param name the leaderboard's name in this partition. Must be unique within the datastore.
	 * @param datastore where to store the leaderboard's state (snapshot + wal)
	 * @param context parent actor context
	 */
	def apply(name: String, datastore: Datastore)(implicit context: ActorRefFactory): Future[Leaderboard] = {
		implicit val executionContext = context.dispatcher
		(context.actorOf(managerProps(name, datastore), "persistent_" + name) ? Get()).mapTo[Leaderboard]
	}
}