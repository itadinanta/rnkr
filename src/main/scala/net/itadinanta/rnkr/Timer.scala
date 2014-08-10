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
	case class Message(val n: Int)
	val scheduler = system.scheduler

	val t0 = System.currentTimeMillis()
	def printts(s: Any) { println((System.currentTimeMillis() - t0) + ": " + s) }
	def after[F](d: FiniteDuration)(f: => Future[F]) = akka.pattern.after(d, using = scheduler)(f)
	//def after[F](d: FiniteDuration)(f: => F) = akka.pattern.after(d, using = scheduler)(f)

	def f1(doPrint: Boolean, x: Int) = if (doPrint) println(x) else println("-")
	f1(true, f(1) + f(1))

	def f2(doPrint: Boolean, x: => Int) = if (doPrint) println(x) else println("-")
	f2(true, f(3) + f(3))

	def f3(doPrint: Boolean, x: () => Int) = if (doPrint) println(x()) else println("-")
	f3(true, () => { f(5) + f(5) })

	def f(i: Int) = { println("Evaluated " + i); i }

	f1(false, f(2) + f(2))
	f2(false, f(4) + f(4))
	f3(false, () => { f(6) + f(6) })

	val dummy = system.actorOf(Props(new Actor {
		def receive = {
			case m => printts(s"Message ${m} received")
		}
	}))

	printts("Before schedule")
	
	// andThen does not work the way you think it works
	after(5 seconds) {
		printts("In scheduler")
		after(5 seconds) {
			successful(Message(0)) pipeTo dummy
		} andThen {
			case Success(m) =>
				printts("Flatmap1")
				after(5 seconds) {
					printts(s"Message ${m} mapped");
					successful(m.copy(n = 1))
				}
		} andThen {
			case Success(m) =>
				after(5 seconds) {
					printts("Flatmap2")
					successful(m)
				}
		} andThen {
			case Success(m) =>
				after(5 seconds) {
					printts("Flatmap3")
					successful(m)
				}
		}
	} onComplete {
		case Success(t) => {
			printts(s"Success: ${t}")
		}
	}

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

		def seq[T](x: Step[T]*): Future[Seq[T]] =
			sequentially(x map {
				case Step(f, Some(delay)) => () => after(delay) { f() }
				case Step(f, None) => f
			})

		def pause[T](delay: FiniteDuration)(f: => Future[T]) = new Step(() => { f }, Some(delay))
	}

	Step.seq(Step.pause(5 seconds) { printts("one"); successful(Message(0)) },
		Step.pause(5 seconds) { printts("two"); successful(Message(1)) },
		Step.pause(5 seconds) { printts("three"); successful(Message(2)) }) onComplete (printts)

	after(5 seconds) {
		printts("In scheduler")
		after(5 seconds) {
			successful(Message(0)) pipeTo dummy
		} flatMap { m =>
			printts("Flatmap1")
			after(5 seconds) {
				printts(s"Message ${m} mapped");
				successful(m.copy(n = 1))
			}
		} flatMap { m =>
			after(5 seconds) {
				printts("Flatmap2")
				successful(m)
			}
		} flatMap { m =>
			after(5 seconds) {
				printts("Flatmap3")
				successful(m)
			}
		}
	} onComplete {
		case Success(t) => {
			printts(s"Success: ${t}")
			system.shutdown
		}
	}
}