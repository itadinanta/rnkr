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
		tree.rank(1) should be(0)
	}

	"A tree with few entries" should "have low ranks" in {
		val tree = createTestTree()
		tree.append(1, "Item1")
		tree.append(2, "Item2")
		tree.append(3, "Item3")

		tree.size should be(3)
		tree.rank(1) should be(0)
		tree.rank(2) should be(1)
		tree.rank(3) should be(2)

		tree.page(1, 1) should be(Seq(Row(2, "Item2", 1)))
	}

	"A tree with more entries" should "have higher ranks" in {
		val tree = createTestTree((1, "Item"), (2, "Item"), (3, "Item"), (4, "Item"), (5, "Item"), (6, "Item"))
		log.debug("{}", tree)
		tree.size should be(6)
		tree.consistent should be(true)

		tree.rank(1) should be(0)
		tree.rank(2) should be(1)
		tree.rank(3) should be(2)
		tree.rank(4) should be(3)
		tree.rank(5) should be(4)
		tree.rank(6) should be(5)

		tree.page(1, 1) should be(Seq(Row(2, "Item", 1)))
	}

}