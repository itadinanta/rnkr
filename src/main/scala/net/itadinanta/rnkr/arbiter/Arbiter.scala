package net.itadinanta.rnkr.arbiter

import akka.actor.{ ActorRef, Actor, FSM }
import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
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

	sealed trait Response[R] { val result: R; val replyTo: ActorRef }
	case class ReadResponse[R](result: R, replyTo: ActorRef) extends Response[R]
	case class WriteResponse[R](result: R, replyTo: ActorRef) extends Response[R]

	sealed trait Request[R] { val f: Type => R }
	case class ReadRequest[R](f: Type => R) extends Request[R]
	case class WriteRequest[R](f: Type => R) extends Request[R]

	sealed trait QueuedRequest[R] extends Request[R] { val replyTo: ActorRef }
	case class QueuedReadRequest[R](f: Type => R, replyTo: ActorRef) extends QueuedRequest[R]
	case class QueuedWriteRequest[R](f: Type => R, replyTo: ActorRef) extends QueuedRequest[R]

	implicit val timeout = Timeout(5 seconds)

	val gate = system.actorOf(Props(new Gate(target)))

	def wqueue[R](f: Type => R): Future[R] = gate ? WriteRequest(f) map { _.asInstanceOf[R] }
	def rqueue[R](f: Type => R): Future[R] = gate ? ReadRequest(f) map { _.asInstanceOf[R] }
	def shutdown() { gate ! PoisonPill }

	sealed trait GateState
	case object Idle extends GateState
	case object Reading extends GateState
	case object Writing extends GateState

	import scala.collection.immutable.Queue
	case class GateData(val rc: Int, val wc: Int, val q: Queue[QueuedRequest[_]])

	class Gate[A](val target: Type) extends Actor with FSM[GateState, GateData] {
		startWith(Idle, GateData(0, 0, Queue[QueuedRequest[_]]()))

		def current(rc: Int, wc: Int, q: Queue[QueuedRequest[_]]) =
			GateData(rc, wc, q)

		def next(rc: Int, wc: Int, q: Queue[QueuedRequest[_]]) =
			if (q.isEmpty) {
				GateData(rc, wc, q)
			} else {
				val (r, tail) = q.dequeue
				val (future, data) = r match {
					case QueuedReadRequest(f, replyTo) =>
						val result = r.f(target)
						replyTo ! result
						(Future { ReadResponse(result, replyTo) }, GateData(rc + 1, wc, tail))
					case QueuedWriteRequest(f, replyTo) =>
						val result = r.f(target)
						replyTo ! result
						(Future { WriteResponse(result, replyTo) }, GateData(rc, wc + 1, tail))
				}
				future pipeTo self
				data
			}

		when(Idle) {
			case Event(r: ReadRequest[_], GateData(0, 0, q)) => goto(Reading) using next(0, 0, q.enqueue(QueuedReadRequest(r.f, sender)))
			case Event(r: WriteRequest[_], GateData(0, 0, q)) => goto(Writing) using next(0, 0, q.enqueue(QueuedWriteRequest(r.f, sender)))
		}

		when(Reading) {
			case Event(r: ReadRequest[_], GateData(rc, 0, q)) => stay using next(rc, 0, q.enqueue(QueuedReadRequest(r.f, sender)))
			case Event(r: WriteRequest[_], GateData(rc, 0, q)) => stay using current(rc, 0, q.enqueue(QueuedWriteRequest(r.f, sender)))

			case Event(r: ReadResponse[_], GateData(1, 0, q)) => if (q.isEmpty) {
				goto(Idle) using next(0, 0, q)
			} else q.front match {
				case qr: QueuedReadRequest[_] => goto(Reading) using next(0, 0, q)
				case qw: QueuedWriteRequest[_] => goto(Writing) using next(0, 0, q)
			}
			case Event(r: ReadResponse[_], GateData(rc, 0, q)) => stay using next(rc - 1, 0, q)
		}

		when(Writing) {
			case Event(r: ReadRequest[_], GateData(0, 1, q)) => stay using current(0, 1, q.enqueue(QueuedReadRequest(r.f, sender)))
			case Event(r: WriteRequest[_], GateData(0, 1, q)) => stay using current(0, 1, q.enqueue(QueuedWriteRequest(r.f, sender)))

			case Event(r: WriteResponse[_], GateData(0, 1, q)) => if (q.isEmpty) {
				goto(Idle) using next(0, 0, q)
			} else q.front match {
				case qr: QueuedReadRequest[_] => goto(Reading) using next(0, 0, q)
				case qw: QueuedWriteRequest[_] => goto(Writing) using next(0, 0, q)
			}
		}

		initialize
	}
}

