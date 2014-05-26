package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import net.itadinanta.rnkr.tree.IntAscending

class DeleteTest extends FlatSpec with ShouldMatchers {
	val log = LoggerFactory.getLogger(classOf[DeleteTest])
	def createTreeWithFanout(fanout: Int): SeqBPlusTree[Int, String] = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntAscending, fanout))

	def createTestTree(items: Pair[Int, String]*): SeqBPlusTree[Int, String] = {
		val tree = createTreeWithFanout(4)
		items foreach { i => tree.put(i._1, i._2) }
		tree
	}

	"After 3 insertions and 1 deletions" should "contain 2 entries in the root" in {
		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			log.debug("{}", t)
			t.remove(1)
			log.debug("{}", t)
			t.size should be(2)
			t.keys should be(Seq(2, 3))
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			log.debug("{}", t)
			t.remove(2)
			log.debug("{}", t)
			t.size should be(2)
			t.keys should be(Seq(1, 3))
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			log.debug("{}", t)
			t.remove(3)
			log.debug("{}", t)
			t.size should be(2)
			t.keys should be(Seq(1, 2))
		}
	}

	"A after 5 insertions and 1 deletion" should "contain 1 entry" in {
		val tree = createTestTree(
			(1, "Item"),
			(2, "Item"),
			(3, "Item"),
			(4, "Item"),
			(5, "Item"))
		tree.size should be(5)
		log.debug("{}", tree)

		tree.remove(3)
		log.debug("{}", tree)
		tree.size should be(4)
		tree.keys should be(Seq(1, 2, 4, 5))
		tree.keysReverse should be(Seq(5, 4, 2, 1))
		tree.consistent should be(true)

		tree.remove(2)
		log.debug("{}", tree)
		tree.size should be(3)
		tree.keys should be(Seq(1, 4, 5))
		tree.keysReverse should be(Seq(5, 4, 1))
		tree.root should be(tree.head)
		tree.consistent should be(true)

		tree.remove(1)
		log.debug("{}", tree)
		tree.size should be(2)
		tree.keys should be(Seq(4, 5))
		tree.keysReverse should be(Seq(5, 4))
		tree.root should be(tree.head)
		tree.consistent should be(true)

	}

	"After 100 insertions and 100 deletions" should "be empty" in {
		val tree = createTestTree()
		val n = 100
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			tree.keys() should be(1 to i)
		}
		log.debug("{}", tree)
		1 to n foreach { i =>
			tree.remove(i);
			log.debug("{}", tree)
			tree.keys() should be((i + 1) to n)
		}
		tree.level should be(1)
		tree.size should be(0)
		tree.indexCount should be(0)
	}

	"After 100 insertions and 100 deletions in reverse" should "be empty" in {
		val tree = createTestTree()
		val n = 100
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			tree.keys() should be(1 to i)
		}
		log.debug("{}", tree)
		n to 1 by -1 foreach { i =>
			tree.remove(i);
			log.debug("{}", tree)
			tree.keys() should be(1 to (i - 1))
		}
		tree.level should be(1)
		tree.indexCount should be(0)
		tree.size should be(0)
	}

	"After 100 random insertions and 100 random deletions" should "be empty" in {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = 100
		Random.setSeed(1234L)
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			log.debug("Removing {} from {}", i, tree)
			tree.remove(i)
			tree.consistent should be(true)
			ordered -= i
			tree.keys() should be(ordered.toList)
		}
		log.debug("Result {}", tree)
		tree.size should be(0)
		tree.indexCount should be(0)
		tree.consistent should be(true)
	}

	"Deleting an item from the leaf" should "propagate to the root if necessary" in {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = 13
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		tree.leafCount should be(6)

		Seq(11, 7, 5, 9, 1, 6, 12) foreach { i =>
			log.debug("Removing {} from {}", i, tree)
			tree.remove(i)
			ordered -= i
			tree.keys() should be(ordered.toList)
			tree.consistent should be(true)
		}
		log.debug("Result {}", tree)
		tree.size should be(ordered.size)
		tree.leafCount should be(2)
		tree.indexCount should be(1)
	}

	"After 1000 random insertions and 10000 random deletions" should "be empty" in {
		val tree = createTreeWithFanout(32)
		val ordered = new mutable.TreeSet[Int]
		val n = 1000
		Random.setSeed(1234L)
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.remove(i)
			tree.consistent should be(true)
			ordered -= i
			tree.keys() should be(ordered.toList)
		}
		log.debug("Result {}", tree)
		tree.size should be(0)
		tree.indexCount should be(0)
		tree.consistent should be(true)
	}

}