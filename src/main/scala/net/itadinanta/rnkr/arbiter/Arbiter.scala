package net.itadinanta.rnkr.arbiter

import akka.actor.{ ActorRef, Actor, FSM }
import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration._
import akka.pattern.{ ask, pipe }
import net.itadinanta.rnkr.tree.Row
import net.itadinanta.rnkr.tree.Tree
import net.itadinanta.rnkr.tree.Rank.Position
import akka.actor.PoisonPill
import scala.reflect.ClassTag

object Arbiter {
	def create[T](t: T)(implicit system: ActorSystem) = new ActorArbiter(t)
}

trait Arbiter[T] {
	val target: T
	def wqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R]
	def rqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R]
	def shutdown()
}

trait TreeArbiter[K, V] extends Arbiter[Tree[K, V]] {
	def put(k: K, v: V): Future[Row[K, V]] = wqueue((target: Tree[K, V]) => target.put(k, v))
	def append(k: K, v: V): Future[Row[K, V]] = wqueue((target: Tree[K, V]) => target.append(k, v))
	def remove(k: K): Future[Option[Row[K, V]]] = wqueue((target: Tree[K, V]) => target.remove(k))

	def version: Future[Long] = rqueue((target: Tree[K, V]) => target.version)
	def get(k: K): Future[Option[Row[K, V]]] = rqueue((target: Tree[K, V]) => target.get(k))
	def keys(): Future[Seq[K]] = rqueue((target: Tree[K, V]) => target.keys())
	def keysReverse(): Future[Seq[K]] = rqueue((target: Tree[K, V]) => target.keysReverse())
	def rank(k: K): Future[Position] = rqueue((target: Tree[K, V]) => target.rank(k))
	def range(k: K, length: Int): Future[Seq[Row[K, V]]] = rqueue((target: Tree[K, V]) => target.range(k, length))
	def page(start: Position, length: Int): Future[Seq[Row[K, V]]] = rqueue((target: Tree[K, V]) => target.page(start, length))
}

object TreeArbiter {
	def create[K, V](t: Tree[K, V])(implicit system: ActorSystem) = new ActorArbiter(t) with TreeArbiter[K, V]
}

class ActorArbiter[T](val target: T)(implicit val system: ActorSystem) extends Arbiter[T] {
	implicit lazy val executionContext = system.dispatcher
	implicit val timeout = Timeout.intToTimeout(Int.MaxValue)
	val gate = system.actorOf(Props(new Gate(target)))

	sealed trait Response
	case object ReadResponse extends Response
	case object WriteResponse extends Response

	sealed trait Request[R] { val f: T => R }
	case class ReadRequest[R](f: T => R) extends Request[R]
	case class WriteRequest[R](f: T => R) extends Request[R]

	sealed trait QueuedRequest[R] extends Request[R] { val replyTo: ActorRef }
	case class QueuedReadRequest[R](f: T => R, replyTo: ActorRef) extends QueuedRequest[R]
	case class QueuedWriteRequest[R](f: T => R, replyTo: ActorRef) extends QueuedRequest[R]

	override def wqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R] = ask(gate, WriteRequest(f)).mapTo[R]
	override def rqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R] = ask(gate, ReadRequest(f)).mapTo[R]
	override def shutdown() { gate ! PoisonPill }

	class Gate(val target: T) extends Actor {
		import scala.collection.immutable.Queue
		case class State(val rc: Int, val wc: Int, val q: Queue[QueuedRequest[_]])
		var state = State(0, 0, Queue[QueuedRequest[_]]())

		@tailrec private def flush(s: State): State = if (s.q.isEmpty) s else
			s.q.dequeue match {
				case (QueuedReadRequest(f, replyTo), tail) if (s.wc == 0) => {
					Future { replyTo ! f(target); ReadResponse } pipeTo self
					flush(State(s.rc + 1, s.wc, tail))
				}
				case (QueuedWriteRequest(f, replyTo), tail) if (s.wc == 0 && s.rc == 0) => {
					Future { replyTo ! f(target); WriteResponse } pipeTo self
					State(s.rc, s.wc + 1, tail)
				}
				case _ => s
			}

		def next(s: State) { state = flush(s) }
		def receive() = {
			case ReadRequest(f) => next(state.copy(q = state.q.enqueue(QueuedReadRequest(f, sender))))
			case WriteRequest(f) => next(state.copy(q = state.q.enqueue(QueuedWriteRequest(f, sender))))
			case ReadResponse => next(state.copy(rc = state.rc - 1))
			case WriteResponse => next(state.copy(wc = state.wc - 1))
		}
	}
}

