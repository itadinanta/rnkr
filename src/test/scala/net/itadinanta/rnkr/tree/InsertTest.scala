package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory

class InsertTest extends TreeBaseTest {
	test("An empty tree should contain no entries") {
		createTestTree().size should be(0)
	}

	test("A tree with one entry should have a head and some items in") {
		val tree = createTestTree()
		tree.put(1, "Item")
		tree.size should be(1)
		tree.head should not be null
	}

	test("A tree with less than fanout entries should have one leaf and no index") {
		val tree = createTestTree()
		1 to 3 foreach { i => tree.put(i, "Item" + i) }
		tree.size should be(3)
		tree.root.keys.size should be(3)
		tree.head.keys.size should be(3)
		tree.tail.keys.size should be(3)
		tree.leafCount should be(1)
		tree.indexCount should be(0)
	}

	test("After 7 insertions should contain 7 entries") {
		val tree = createTestTree()
		1 to 7 foreach { i => tree.put(i, "Item" + i); debug(s"Added ${i} to ${tree}") }
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.keys.size should be(2)
		tree.head.keys.size should be(2)
		tree.consistent should be(true)
	}

	test("After 17 insertions in reverse should contain 17 entries and grow a level") {
		val tree = createTestTree()
		for (i <- 17 to 1 by -1) {
			tree.put(i, "Item" + i)
			debug(s"Added ${i} to ${tree}")
			tree.consistent should be(true)
		}
		tree.size should be(17)
		tree.factory.fanout should be(4)
		tree.level should be(3)
		tree.root.keys.size should be(1)
	}

	test("After 100 insertions with String keys should contain 100 entries") {
		val tree = new SeqTree[String, String](new SeqNodeFactory[String, String](StringAscending, 4))
		for (i <- 1 to 100) {
			tree.put("Key" + i, "Item" + i)
			debug(s"After adding ${i} to tree: {tree}")
			tree.consistent should be(true)
		}
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	test("After 7 insertions in reverse should contain 7 entries") {
		val tree = createTestTree()
		7 to 1 by -1 foreach { i => tree.put(i, "Item" + i); debug(s"Added ${i} to ${tree}") }
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.keys.size should be(1)
		tree.head.keys.size should be(4)
	}

	test("After 100 insertion should contain 100 entries in order") {
		val tree = createTestTree()
		for (i <- 1 to 100) {
			tree.put(i, "Item" + i);
			tree.keys() should be((1 to i))
			debug(tree)
			tree.consistent should be(true)
		}
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	test("After 100 insertions in reverse should contain 100 entries in order") {
		val tree = createTestTree()
		100 to 1 by -1 foreach { i => tree.put(i, "Item" + i) }
		debug(tree)
		tree.keys() should be((1 to 100))
		tree.keysReverse() should be((100 to 1 by -1))
		tree.size should be(100)
	}

	test("After 1000 random insertions should contain 1000 entries in order") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		Random.setSeed(1234L)
		Random.shuffle(1 to 1000 map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		debug(tree)
		tree.consistent should be(true)
		tree.keysReverse() should be((1000 to 1 by -1))
		tree.size should be(1000)
	}

	test("After 100 insertions in reverse should contain 100 entries in reverse") {
		val tree = RankedTreeMap.intStringTree(IntDescending, 9)
		1 to 100 foreach { i => tree.put(i, "Item" + i) }
		debug(tree)
		tree.keysReverse() should be((1 to 100))
		tree.keys() should be((100 to 1 by -1))
		tree.size should be(100)
	}

	test("An ordered range should count N keys forward from a given pivot") {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i) }
		debug(tree)
		tree.get(20) map (_.value) should be(Some("Item10"))
		tree.range(2, 100) map (_.key) should be((2 to 200 by 2))
		tree.range(0, 200) map (_.key) should be((2 to 200 by 2))
		tree.range(20, 10) map (_.key) should be((20 to 38 by 2))
		tree.range(19, 10) map (_.key) should be((20 to 38 by 2))
		tree.range(21, 10) map (_.key) should be((22 to 40 by 2))
		tree.range(200, 0) map (_.key) should be(Seq())
		tree.range(200, 1) map (_.key) should be(Seq(200))
		tree.range(200, 100) map (_.key) should be(Seq(200))
		tree.range(201, 100) map (_.key) should be(Seq())
	}

	test("An ordered range should count N keys backwards from a given pivot") {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i) }

		tree.range(1, -1) map (_.key) should be(Seq())
		tree.range(2, -1) map (_.key) should be(Seq(2))
		tree.range(20, -5) map (_.key) should be((20 to 12 by -2))
		tree.range(21, -5) map (_.key) should be((20 to 12 by -2))
	}
}