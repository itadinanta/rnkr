package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory

object TreeTest {
	val log = LoggerFactory.getLogger(classOf[TreeTest])
}
class TreeTest extends FlatSpec with ShouldMatchers {
	import TreeTest.log
	def createTestTree(items: Pair[Int, String]*): SeqBPlusTree[Int, String] = {
		val tree = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntAscending, 4))
		items foreach { i => tree.put(i._1, i._2) }
		tree
	}

	"An empty tree" should "contain no entries" in {
		createTestTree().size should be(0)
	}

	"A tree with one entry" should "have a head and some items in" in {
		val tree = createTestTree()
		tree.put(1, "Item")
		tree.size should be(1)
		tree.head should not be (null)
	}

	"A tree with less than fanout entries" should "have one leaf and no index" in {
		val tree = createTestTree()
		1 to 3 foreach { i => tree.put(i, "Item" + i) }
		tree.size should be(3)
		tree.root.size should be(3)
		tree.head.size should be(3)
		tree.tail.size should be(3)
		tree.leafCount should be(1)
		tree.indexCount should be(0)
	}

	"After 7 insertions" should "contain 7 entries" in {
		val tree = createTestTree()
		1 to 7 foreach { i => tree.put(i, "Item" + i); log.debug("{}", tree) }
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.size should be(2)
		tree.head.size should be(2)
	}

	"After 7 insertions in reverse" should "contain 7 entries" in {
		val tree = createTestTree()
		7 to 1 by -1 foreach { i => tree.put(i, "Item" + i); log.debug("{}", tree) }
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.size should be(1)
		tree.head.size should be(4)
	}

	"After 100 insertion" should "contain 100 entries in order" in {
		val tree = createTestTree()
		1 to 100 foreach {
			i =>
				tree.put(i, "Item" + i);
				tree.keys() should be(1 to i)
				log.debug("{}", tree)
				tree.consistent should be(true)
		}
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	"After 100 insertions in reverse" should "contain 100 entries in order" in {
		val tree = createTestTree()
		100 to 1 by -1 foreach { i => tree.put(i, "Item" + i) }
		log.debug("{}", tree)
		tree.keys() should be(1 to 100)
		tree.keysReverse() should be(100 to 1 by -1)
		tree.size should be(100)
	}

	"After 1000 random insertions" should "contain 1000 entries in order" in {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		Random.shuffle(1 to 1000 map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		log.debug("{}", tree)
		tree.keysReverse() should be(1000 to 1 by -1)
		tree.size should be(1000)
	}

	"After 100 insertions in reverse" should "contain 100 entries in reverse" in {
		val tree = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntDescending, 9))
		1 to 100 foreach { i => tree.put(i, "Item" + i) }
		log.debug("{}", tree)
		tree.keysReverse() should be(1 to 100)
		tree.keys() should be(100 to 1 by -1)
		tree.size should be(100)
	}

	"An ordered range" should "count N keys forward from a given pivot" in {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i) }
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
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i) }

		tree.range(1, -1) map (_._1) should be(Seq())
		tree.range(2, -1) map (_._1) should be(Seq(2))
		tree.range(20, -5) map (_._1) should be(20 to 12 by -2)
		tree.range(21, -5) map (_._1) should be(20 to 12 by -2)
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
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			log.debug("Removing {} from {}", i, tree)
			tree.remove(i)
			tree.consistent should be(true)			
			log.debug("{}", " - " + tree)
			ordered -= i
			tree.keys() should be(ordered.toList)
		}
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
			log.debug("{}", " - " + tree)
			ordered -= i
			tree.keys() should be(ordered.toList)
			tree.consistent should be(true)
		}
		tree.size should be(ordered.size)
		//		tree.leafCount should be(2)
		//		tree.indexCount should be(1)
	}

}