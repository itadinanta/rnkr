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

object Arbiter {
	def create[K, V](t: Tree[K, V])(implicit system: ActorSystem) = new ActorArbiter(t)
}

trait Arbiter[K, V] {
	type Type = Tree[K, V]

	def wqueue[R](f: (Type) => R): Future[R]
	def rqueue[R](f: (Type) => R): Future[R]

	def put(k: K, v: V): Future[Row[K, V]] = wqueue((target: Type) => target.put(k, v))
	def append(k: K, v: V): Future[Row[K, V]] = wqueue((target: Type) => target.append(k, v))
	def remove(k: K): Future[Option[Row[K, V]]] = wqueue((target: Type) => target.remove(k))

	def version: Future[Long] = rqueue((target: Type) => target.version)
	def get(k: K): Future[Option[Row[K, V]]] = rqueue((target: Type) => target.get(k))
	def keys(): Future[Seq[K]] = rqueue((target: Type) => target.keys())
	def keysReverse(): Future[Seq[K]] = rqueue((target: Type) => target.keysReverse())
	def rank(k: K): Future[Position] = rqueue((target: Type) => target.rank(k))
	def range(k: K, length: Int): Future[Seq[Row[K, V]]] = rqueue((target: Type) => target.range(k, length))
	def page(start: Position, length: Int): Future[Seq[Row[K, V]]] = rqueue((target: Type) => target.page(start, length))

	def shutdown()
}

class ActorArbiter[K, V](val target: Tree[K, V])(implicit val system: ActorSystem) extends Arbiter[K, V] {
	implicit lazy val executionContext = system.dispatcher

	sealed trait Response[R] { val result: R; val replyTo: ActorRef; val id: Long }
	case class ReadResponse[R](result: R, replyTo: ActorRef, id: Long) extends Response[R]
	case class WriteResponse[R](result: R, replyTo: ActorRef, id: Long) extends Response[R]

	sealed trait Request[R] { val f: Type => R }
	case class ReadRequest[R](f: Type => R) extends Request[R]
	case class WriteRequest[R](f: Type => R) extends Request[R]

	sealed trait QueuedRequest[R] extends Request[R] { val replyTo: ActorRef; val id: Long }
	case class QueuedReadRequest[R](f: Type => R, replyTo: ActorRef, id: Long) extends QueuedRequest[R]
	case class QueuedWriteRequest[R](f: Type => R, replyTo: ActorRef, id: Long) extends QueuedRequest[R]

	implicit val timeout = Timeout(5 seconds)

	val gate = system.actorOf(Props(new Gate(target)))

	def wqueue[R](f: Type => R): Future[R] = gate ? WriteRequest[R](f) map { _.asInstanceOf[R] }
	def rqueue[R](f: Type => R): Future[R] = gate ? ReadRequest[R](f) map { _.asInstanceOf[R] }
	def shutdown() { gate ! PoisonPill }

	import scala.collection.immutable.Queue

	class Gate[A](val target: Type) extends Actor {
		var _id = 0L
		case class State(val rc: Int, val wc: Int, val q: Queue[QueuedRequest[_]])
		var state = State(0, 0, Queue[QueuedRequest[_]]())

		@tailrec private def flush(s: State): State = if (s.q.isEmpty) s else {
			val (r, tail) = s.q.dequeue
			r match {
				case QueuedReadRequest(f, replyTo, id) => if (s.wc == 0) {
					Future { ReadResponse(r.f(target), replyTo, id) } pipeTo self
					flush(State(s.rc + 1, s.wc, tail))
				} else s

				case QueuedWriteRequest(f, replyTo, id) => if (s.wc == 0 && s.rc == 0) {
					Future { WriteResponse(r.f(target), replyTo, id) } pipeTo self
					State(s.rc, s.wc + 1, tail)
				} else s
			}
		}

		def id() = { _id += 1; _id }
		def enqueue(s: State, r: QueuedRequest[_]) = s.copy(q = s.q.enqueue(r))

		def receive() = {
			case r: ReadRequest[_] => state = flush(enqueue(state, QueuedReadRequest(r.f, sender, id())))
			case w: WriteRequest[_] => state = flush(enqueue(state, QueuedWriteRequest(w.f, sender, id())))
			case r: ReadResponse[_] =>
				r.replyTo ! r.result
				state = flush(State(state.rc - 1, state.wc, state.q))
			case w: WriteResponse[_] =>
				w.replyTo ! w.result
				state = flush(State(state.rc, state.wc - 1, state.q))
		}
	}
}

