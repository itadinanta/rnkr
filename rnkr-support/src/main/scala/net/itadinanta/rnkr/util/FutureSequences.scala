package net.itadinanta.rnkr.util

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.collection.mutable.ListBuffer
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext

object FutureSequences {
	def sequentially[T](x: Seq[() => Future[T]])(implicit executionContext: ExecutionContext): Future[Seq[T]] = {
		val p = Promise[Seq[T]]
		val accum = new ListBuffer[T]
		def sequentially(x: List[() => Future[T]]): Unit = x match {
			case Nil => p.success(accum.toSeq)
			case head :: tail => try {
				head() onComplete {
					case Success(result) => {
						accum += result
						sequentially(tail)
					}
					case Failure(t) => p.failure(t)
				}
			} catch {
				case t: Throwable => p.failure(t)
			}
		}
		sequentially(x.to[List])
		return p.future
	}
}