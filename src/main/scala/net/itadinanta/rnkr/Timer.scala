package net.itadinanta.rnkr

import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent._
import akka.pattern.after
import akka.pattern.pipe
import scala.collection.mutable.ListBuffer
import scala.util.Success
import scala.util.Failure

object Timer extends App {
	implicit val system = ActorSystem("node")
	implicit val executionContext = system.dispatchers.lookup("main-app-dispatcher")
	case class Message(val n: String) {
		printts(n)
		def apply(s: String) = copy(n = s)
	}
	val scheduler = system.scheduler

	val t0 = System.currentTimeMillis()
	def printts(s: Any) { println((System.currentTimeMillis() - t0) + ": " + s) }
	def after[F](d: FiniteDuration)(f: => Future[F]) = akka.pattern.after(d, using = scheduler)(f)

	val dummy = system.actorOf(Props(new Actor {
		def receive = {
			case m => printts(s"Message ${m} received")
		}
	}))

	case class Step[T](f: () => Future[T], delay: Option[FiniteDuration])
	case object Step {
		def sequentially[T](x: Seq[() => Future[T]]): Future[Seq[T]] = {
			val p = Promise[Seq[T]]
			val accum = new ListBuffer[T]
			def sequentially(x: List[() => Future[T]]): Unit = x match {
				case head :: tail => head() onComplete {
					case Success(result) => {
						accum += result
						sequentially(tail)
					}
					case Failure(t) => p.failure(t)
				}
				case Nil => p.success(accum.toSeq)
			}
			sequentially(x.to[List])
			return p.future
		}

		def apply[T](x: Step[T]*): Future[Seq[T]] =
			sequentially(x map {
				case Step(f, Some(delay)) => () => after(delay) { f() }
				case Step(f, None) => f
			})

		def pause[T](delay: FiniteDuration)(f: => Future[T]) = new Step(() => { f }, Some(delay))
		def apply[T](f: => Future[T]) = new Step(() => { f }, None)
	}

	val delay = (1 seconds)

	val f1 = after(delay) {
		successful(Message("andThen0"))
	} andThen {
		case Success(m) => after(delay) { successful(m("andThen1")) }
	} andThen {
		case Success(m) => after(delay) { successful(m("andThen2")) }
	} andThen {
		case Success(m) => after(delay) { successful(m("andThen3")) pipeTo dummy }
	} onComplete {
		case _ =>
			Step(
				Step.pause(delay) { successful(Message("step0")) },
				Step.pause(delay) { successful(Message("step1")) },
				Step { after(delay) { successful(Message("step2")) } },
				Step.pause(delay) { successful(Message("step3")) pipeTo dummy }) onSuccess {
					case _ =>
						after(delay) {
							successful(Message("flatMap0"))
						} flatMap { m => after(delay) { successful(m("flatMap1")) }
						} flatMap { m => after(delay) { successful(m("flatMap2")) }
						} flatMap { m => after(delay) { successful(m("flatMap3")) pipeTo dummy }
						} onComplete {
							case _ => {
								system.shutdown
							}
						}
				}
	}
}