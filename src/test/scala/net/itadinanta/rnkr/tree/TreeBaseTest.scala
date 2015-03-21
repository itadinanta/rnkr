package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.scalatest.FunSuite
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.tree._

abstract class TreeBaseTest extends FunSuite with Logging {
	def createTreeWithFanout(fanout: Int) = RankedTreeMap.intStringTree(IntAscending, fanout)
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Tuple2[Int, String]*) = {
		val RankedTreeMap = createTreeWithFanout(4)
		items foreach { i => RankedTreeMap.append(i._1, i._2) }
		RankedTreeMap
	}
}