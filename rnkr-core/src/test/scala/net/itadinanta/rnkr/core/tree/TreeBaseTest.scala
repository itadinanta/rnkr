package net.itadinanta.rnkr.core.tree

import net.itadinanta.rnkr.core.tree._
import org.scalatest.FunSuite
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.core.tree._
import org.scalatest.Matchers

trait TreeBaseTest extends FunSuite with Matchers with Logging {
	val smallCount = 100
	val largeCount = 1000
	val count = 500

	def treeWithFanout(fanout: Int) = RankedTreeMap.withStringValues(IntAscending, fanout)
	def testTree() = treeWithFanout(4)
	def testTree(items: Tuple2[Int, String]*) = {
		val m = treeWithFanout(4)
		for (i <- items) { m.append(i._1, i._2) }
		m
	}
}