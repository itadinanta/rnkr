package net.itadinanta.rnkr.core.tree

import net.itadinanta.rnkr.core.tree._
import org.scalatest.FunSuite
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.core.tree._
import org.scalatest.Matchers

abstract class TreeBaseTest extends FunSuite with Matchers with Logging {
	def createTreeWithFanout(fanout: Int) = RankedTreeMap.withStringValues(IntAscending, fanout)
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Tuple2[Int, String]*) = {
		val RankedTreeMap = createTreeWithFanout(4)
		items foreach { i => RankedTreeMap.append(i._1, i._2) }
		RankedTreeMap
	}
}