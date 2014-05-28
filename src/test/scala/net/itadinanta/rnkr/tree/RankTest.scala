package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory

class RankTest extends FlatSpec with ShouldMatchers {
	val log = LoggerFactory.getLogger(classOf[RankTest])
	def createTreeWithFanout(fanout: Int) = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntAscending, fanout))
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Pair[Int, String]*) = {
		val tree = createTreeWithFanout(4)
		items foreach { i => tree.append(i._1, i._2) }
		tree
	}

	"A tree with one entry" should "have one rank 0" in {
		val tree = createTestTree()
		tree.append(1, "Item")
		tree.size should be(1)
		tree.rank(1) should be (0)
	}
}