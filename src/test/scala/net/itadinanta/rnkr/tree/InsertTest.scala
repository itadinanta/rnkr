package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.junit.Test
import org.fest.assertions.Assertions._

class InsertJUnitTest {
	val log = LoggerFactory.getLogger(classOf[InsertTest])
	def createTreeWithFanout(fanout: Int) = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntAscending, fanout))
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Pair[Int, String]*) = {
		val tree = createTreeWithFanout(4)
		items foreach { i => tree.put(i._1, i._2) }
		tree
	}

	@Test
	def testMe() {
		assertThat(true).isTrue()
	}
}

class InsertTest extends FlatSpec with ShouldMatchers {
	val log = LoggerFactory.getLogger(classOf[InsertTest])
	def createTreeWithFanout(fanout: Int) = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntAscending, fanout))
	def createTestTree() = createTreeWithFanout(4)
	def createTestTree(items: Pair[Int, String]*) = {
		val tree = createTreeWithFanout(4)
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
		tree.root.keys.size should be(3)
		tree.head.keys.size should be(3)
		tree.tail.keys.size should be(3)
		tree.leafCount should be(1)
		tree.indexCount should be(0)
	}

	"After 7 insertions" should "contain 7 entries" in {
		val tree = createTestTree()
		1 to 7 foreach { i => tree.put(i, "Item" + i); log.debug("Added {} to {}", i, tree) }
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.keys.size should be(2)
		tree.head.keys.size should be(2)
		tree.consistent should be(true)
	}

	"After 100 insertions with String keys" should "contain 100 entries" in {
		val tree = new SeqBPlusTree[String, String](new SeqNodeFactory[String, String](StringAscending, 4))
		1 to 100 foreach { i => tree.put("Key" + i, "Item" + i) }
		log.debug("Tree with Strings: {}", tree)
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
		tree.consistent should be(true)
	}

	"After 7 insertions in reverse" should "contain 7 entries" in {
		val tree = createTestTree()
		7 to 1 by -1 foreach { i => tree.put(i, "Item" + i); log.debug("Added {} to {}", i, tree) }
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.keys.size should be(1)
		tree.head.keys.size should be(4)
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
		Random.setSeed(1234L)
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
		tree.get(20) map (_.value) should be(Some("Item10"))
		tree.range(2, 100) map (_.key) should be(2 to 200 by 2)
		tree.range(0, 200) map (_.key) should be(2 to 200 by 2)
		tree.range(20, 10) map (_.key) should be(20 to 38 by 2)
		tree.range(19, 10) map (_.key) should be(20 to 38 by 2)
		tree.range(21, 10) map (_.key) should be(22 to 40 by 2)
		tree.range(200, 0) map (_.key) should be(Seq())
		tree.range(200, 1) map (_.key) should be(Seq(200))
		tree.range(200, 100) map (_.key) should be(Seq(200))
		tree.range(201, 100) map (_.key) should be(Seq())
	}

	"An ordered range" should "count N keys backwards from a given pivot" in {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i) }

		tree.range(1, -1) map (_.key) should be(Seq())
		tree.range(2, -1) map (_.key) should be(Seq(2))
		tree.range(20, -5) map (_.key) should be(20 to 12 by -2)
		tree.range(21, -5) map (_.key) should be(20 to 12 by -2)
	}
}