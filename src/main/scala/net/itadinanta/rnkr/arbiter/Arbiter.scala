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

	sealed trait Response[R] { val result: R; val replyTo: ActorRef }
	case class ReadResponse[R](result: R, replyTo: ActorRef) extends Response[R]
	case class WriteResponse[R](result: R, replyTo: ActorRef) extends Response[R]

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
		case class GateData(val rc: Int, val wc: Int, val q: Queue[QueuedRequest[_]])
		var _id = 0L
		var state = GateData(0, 0, Queue[QueuedRequest[_]]())

		def current(rc: Int, wc: Int, q: Queue[QueuedRequest[_]]) =
			GateData(rc, wc, q)

		def next(rc: Int, wc: Int, q: Queue[QueuedRequest[_]]) =
			if (q.isEmpty) {
				GateData(rc, wc, q)
			} else {
				val (r, tail) = q.dequeue
				val (future, data) = r match {
					case QueuedReadRequest(f, replyTo, id) =>
						(Future {
							println(s"Executing ${r}")
							ReadResponse(r.f(target), replyTo)
						}, GateData(rc + 1, wc, tail))
					case QueuedWriteRequest(f, replyTo, id) =>
						(Future {
							println(s"Executing ${r}")
							WriteResponse(r.f(target), replyTo)
						}, GateData(rc, wc + 1, tail))
				}
				future pipeTo self
				data
			}

		def enqueue(s: GateData, r: QueuedRequest[_]) = {
			println(s"Queuing ${r} -> ${s}")
			s.q.enqueue(r)
		}

		def id() = { _id += 1 ; _id }
		
		def receive() = {
			case r: ReadRequest[_] => {
				println(s"${r} ${state}")
				state = if (state.wc > 0)
					current(state.rc, state.wc, enqueue(state, QueuedReadRequest(r.f, sender, id())))
				else
					next(state.rc, state.wc, enqueue(state, QueuedReadRequest(r.f, sender, id())))

			}

			case w: WriteRequest[_] => {
				println(s"${w} ${state}")
				state = if (state.wc > 0 || state.rc > 0)
					current(state.rc, state.wc, enqueue(state, QueuedWriteRequest(w.f, sender, id())))
				else
					next(state.rc, state.wc, enqueue(state, QueuedWriteRequest(w.f, sender, id())))
			}

			case r: ReadResponse[_] => {
				println(s"${r} ${state}")
				r.replyTo ! r.result
				state = next(state.rc - 1, state.wc, state.q)
			}

			case w: WriteResponse[_] => {
				println(s"${w} ${state}")
				w.replyTo ! w.result
				state = next(state.rc, state.wc - 1, state.q)
			}
		}
	}
}

