package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.scalatest.FunSuite

abstract class TreeBaseTest extends FunSuite {
	val log = LoggerFactory.getLogger(this.getClass)
	def createTreeWithFanout(fanout: Int) = Tree.intStringTree(IntAscending, fanout)
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Pair[Int, String]*) = {
		val tree = createTreeWithFanout(4)
		items foreach { i => tree.append(i._1, i._2) }
		tree
	}
}