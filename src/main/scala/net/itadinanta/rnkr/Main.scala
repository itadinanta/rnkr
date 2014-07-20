package net.itadinanta.rnkr

import net.itadinanta.rnkr.arbiter.Arbiter
import net.itadinanta.rnkr.tree.SeqTree
import net.itadinanta.rnkr.tree.Tree
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.dispatch.Futures
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent.Promise
import scala.collection.mutable.ListBuffer
import net.itadinanta.rnkr.tree.Row

object Main extends App {
	implicit val system = ActorSystem("node")
	implicit val executionContext = system.dispatcher

	val a = Arbiter.create(Tree.intStringTree())
	val done = Promise[Boolean]

	val b = new ListBuffer[Future[Row[Int, String]]]

	for (i <- 1 to 50) {
		Future { b += a.put(i, i.toString()) }
	}

	sequence(b.toList) map { r =>
		a.keys().map { r => print(r); done.success(true) }
	}

	done.future map {
		case _ => system.shutdown
	}
}
