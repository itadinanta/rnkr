package net.itadinanta.rnkr.core.tree

import net.itadinanta.rnkr.core.tree._
import org.scalatest.FunSuite
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.core.tree._
import org.scalatest.Matchers

abstract class TreeBaseTest extends FunSuite with Matchers with Logging {
	val smallCount = 100
	val largeCount = 1000000

	def createTreeWithFanout(fanout: Int) = RankedTreeMap.withStringValues(IntAscending, fanout)
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Tuple2[Int, String]*) = {
		val m = createTreeWithFanout(4)
		for (i <- items) { m.append(i._1, i._2) }
		m
	}
}