package net.itadinanta.rnkr.core.tree

import net.itadinanta.rnkr.core.tree._
import scala.util.Random
import scala.collection.mutable

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
		for (i <- 1 to 7) tree.put(i, "Item" + i)
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
			tree.consistent should be(true)
		}
		tree.size should be(17)
		tree.factory.fanout should be(4)
		tree.level should be(3)
		tree.root.keys.size should be(1)
	}

	test(s"After ${smallCount} insertions with String keys should contain ${smallCount} entries") {
		val tree = new SeqTree[String, String](new SeqNodeFactory[String, String](StringAscending, 4))
		for (i <- 1 to smallCount) {
			tree.put("Key" + i, "Item" + i)
			tree.consistent should be(true)
		}
		tree.size should be(smallCount)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	test("After 7 insertions in reverse should contain 7 entries") {
		val tree = createTestTree()
		for (i <- 7 to 1 by -1) tree.put(i, "Item" + i)
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.keys.size should be(1)
		tree.head.keys.size should be(4)
	}

	test(s"After ${smallCount} insertion should contain ${smallCount} entries in order") {
		val tree = createTestTree()
		for (i <- 1 to smallCount) {
			tree.put(i, "Item" + i);
			tree.keys() should be((1 to i))
			tree.consistent should be(true)
		}
		tree.size should be(smallCount)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	test(s"After ${smallCount} insertions in reverse should contain ${smallCount} entries in order") {
		val tree = createTestTree()
		for (i <- smallCount to 1 by -1) tree.put(i, "Item" + i)
		tree.keys() should be((1 to smallCount))
		tree.keysReverse() should be((smallCount to 1 by -1))
		tree.size should be(smallCount)
	}

	test(s"After ${count} random insertions should contain ${count} entries in order") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val rnd = new Random
		rnd.setSeed(1234L)
		rnd.shuffle(1 to count map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		tree.consistent should be(true)
		tree.keysReverse() should be((count to 1 by -1))
		tree.size should be(count)
	}

	test(s"After ${smallCount} insertions in reverse should contain ${smallCount} entries in reverse") {
		val tree = RankedTreeMap.withIntKeys[String](IntDescending, 9)
		for (i <- 1 to smallCount) tree.put(i, "Item" + i)
		tree.keysReverse() should be((1 to smallCount))
		tree.keys() should be((smallCount to 1 by -1))
		tree.size should be(smallCount)
	}

	test("An ordered range should count N keys forward from a given pivot") {
		val doubleRange = 2 * smallCount
		val tree = createTestTree()
		for (i <- 1 to smallCount) tree.put(2 * i, "Item" + i)
		tree.get(20) map (_.value) should be(Some("Item10"))
		tree.range(2, smallCount) map (_.key) should be((2 to doubleRange by 2))
		tree.range(0, doubleRange) map (_.key) should be((2 to doubleRange by 2))
		tree.range(20, 10) map (_.key) should be((20 to 38 by 2))
		tree.range(19, 10) map (_.key) should be((20 to 38 by 2))
		tree.range(21, 10) map (_.key) should be((22 to 40 by 2))
		tree.range(doubleRange, 0) map (_.key) should be(Seq())
		tree.range(doubleRange, 1) map (_.key) should be(Seq(doubleRange))
		tree.range(doubleRange, smallCount) map (_.key) should be(Seq(doubleRange))
		tree.range(doubleRange + 1, smallCount) map (_.key) should be(Seq())
	}

	test("An ordered range should count N keys backwards from a given pivot") {
		val tree = createTestTree()
		for (i <- 1 to 100) tree.put(2 * i, "Item" + i)

		tree.range(1, -1) map (_.key) should be(Seq())
		tree.range(2, -1) map (_.key) should be(Seq(2))
		tree.range(20, -5) map (_.key) should be((20 to 12 by -2))
		tree.range(21, -5) map (_.key) should be((20 to 12 by -2))
	}
}