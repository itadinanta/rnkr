package net.itadinanta.rnkr

import net.itadinanta.rnkr.arbiter.Arbiter
import net.itadinanta.rnkr.tree.SeqBPlusTree
import net.itadinanta.rnkr.tree.Tree
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.dispatch.Futures
import scala.concurrent.Future
import scala.concurrent.Promise

object Main extends App {
	implicit val system = ActorSystem("node")
	implicit val executionContext = system.dispatcher

	val a = Arbiter.create(Tree.intStringTree())
	val done = Promise[Boolean]()

	Future.sequence(Seq(a.append(1, "1"), a.append(2, "2"), a.append(3, "3"))) map { r =>
		a.keys().map { r => print(r); done.success(true) }
	}

	done.future map {
		case true =>
			system.shutdown
	}
}
