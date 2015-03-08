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
import net.itadinanta.rnkr.arbiter.TreeArbiter

object Main extends App {
	implicit val system = ActorSystem("node")
	implicit val executionContext = system.dispatchers.lookup("main-app-dispatcher")

//	val a = TreeArbiter.create(Tree.intStringTree())
//	val done = Promise[Boolean]
//
//	val b = new ListBuffer[Future[Option[Row[Int, String]]]]
//
//	for (i <- 1 to 1000) {
//		Future { b += a.put(i, i.toString()) map (Some(_)) }
//		for (j <- 1 to 1000) {
//			Future { b += a.get(i) }
//		}
//	}
//	
//	sequence(b.toList) map { r =>
//		//		a.page(40, 10).map { r => println(r); done.success(true) }
//		a.get(990).map { r => println(r); done.success(true) }
//	}
//
//	done.future map {
//		case _ => system.shutdown
//	}
}
