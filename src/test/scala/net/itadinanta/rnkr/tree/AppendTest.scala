package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory

class AppendTest extends FlatSpec with ShouldMatchers {
	val log = LoggerFactory.getLogger(classOf[InsertTest])
	def createTreeWithFanout(fanout: Int) = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntAscending, fanout))
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Pair[Int, String]*) = {
		val tree = createTreeWithFanout(4)
		items foreach { i => tree.append(i._1, i._2) }
		tree
	}

	"An empty tree" should "contain no entries" in {
		createTestTree().size should be(0)
	}

	"A tree with one entry" should "have a head and some items in" in {
		val tree = createTestTree()
		tree.append(1, "Item")
		tree.size should be(1)
		tree.head should not be (null)
	}

	"A tree with less than fanout entries" should "have one leaf and no index" in {
		val tree = createTestTree()
		1 to 3 foreach { i => tree.append(i, "Item" + i) }
		tree.size should be(3)
		tree.root.keys.size should be(3)
		tree.head.keys.size should be(3)
		tree.tail.keys.size should be(3)
		tree.leafCount should be(1)
		tree.indexCount should be(0)
	}

	"After 7 insertions" should "contain 7 entries" in {
		val tree = createTestTree()
		1 to 7 foreach { i => tree.append(i, "Item" + i); log.debug("Added {} to {}", i, tree) }
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.keys.size should be(2)
		tree.head.keys.size should be(2)
	}

	"After 100 insertions with String keys" should "contain 100 entries" in {
		val tree = new SeqBPlusTree[String, String](new SeqNodeFactory[String, String](StringAscending, 4))
		1 to 100 foreach { i =>
			tree.append("Key%03d".format(i), "Item" + i)

		}
		log.debug("Tree with Strings: {}", tree)
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
		tree.consistent should be(true)
	}

	"After 7 insertions in reverse" should "fail with exception" in {
		val tree = createTestTree()
		evaluating {
			7 to 1 by -1 foreach { i => tree.append(i, "Item" + i); log.debug("Added {} to {}", i, tree) }
		} should produce[IllegalArgumentException]
	}

	"After 100 insertion" should "contain 100 entries in order" in {
		val tree = createTestTree()
		1 to 100 foreach {
			i =>
				tree.append(i, "Item" + i);
				tree.keys() should be(1 to i)
				log.debug("{}", tree)
				tree.consistent should be(true)
		}
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	"After 1000000 appends" should "contain 1000000 entries in order" in {
		val tree = this.createTreeWithFanout(100)
		1 to 1000000 foreach { i => tree.append(i, "Item" + i); }
		tree.size should be(1000000)
		tree.factory.fanout should be(100)
		tree.level should be(4)
	}

	"After 100 insertions in reverse" should "fail with exception" in {
		val tree = createTestTree()
		evaluating {
			100 to 1 by -1 foreach { i => tree.append(i, "Item" + i) }
		} should produce[IllegalArgumentException]
	}

	"An ordered range" should "count N keys forward from a given pivot" in {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.append(2 * i, "Item" + i) }
		log.debug("{}", tree)
		tree.get(20) should be(Some("Item10"))
		tree.range(2, 100) map (_._1) should be(2 to 200 by 2)
		tree.range(0, 200) map (_._1) should be(2 to 200 by 2)
		tree.range(20, 10) map (_._1) should be(20 to 38 by 2)
		tree.range(19, 10) map (_._1) should be(20 to 38 by 2)
		tree.range(21, 10) map (_._1) should be(22 to 40 by 2)
		tree.range(200, 0) map (_._1) should be(Seq())
		tree.range(200, 1) map (_._1) should be(Seq(200))
		tree.range(200, 100) map (_._1) should be(Seq(200))
		tree.range(201, 100) map (_._1) should be(Seq())
	}

	"An ordered range" should "count N keys backwards from a given pivot" in {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.append(2 * i, "Item" + i) }

		tree.range(1, -1) map (_._1) should be(Seq())
		tree.range(2, -1) map (_._1) should be(Seq(2))
		tree.range(20, -5) map (_._1) should be(20 to 12 by -2)
		tree.range(21, -5) map (_._1) should be(20 to 12 by -2)
	}
}