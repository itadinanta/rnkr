package net.itadinanta.rnkr.core.tree

class AppendTest extends TreeBaseTest {

	test("An empty tree should contain no entries") {
		testTree().size should be(0)
	}

	test("A tree with one entry should have a head and some items in") {
		val tree = testTree()
		tree.append(1, "Item")
		tree.size should be(1)
		tree.head should not be null
	}

	test("A tree with less than fanout entries should have one leaf and no index") {
		val tree = testTree()
		for (i <- 1 to 3) tree.append(i, "Item" + i)
		tree.size should be(3)
		tree.root.keys.size should be(3)
		tree.head.keys.size should be(3)
		tree.tail.keys.size should be(3)
		tree.leafCount should be(1)
		tree.indexCount should be(0)
	}

	test("After 7 insertions should contain 7 entries") {
		val tree = testTree()
		for (i <- 1 to 7) {
			tree.append(i, "Item" + i)
		}
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.keys.size should be(2)
		tree.head.keys.size should be(2)
		tree.consistent should be(true)
	}

	test(s"After ${smallCount} insertions with String keys should contain ${smallCount} entries") {
		val tree = RankedTreeMap[String, String](StringAscending, 4)
		for (i <- 1 to smallCount) tree.append("Key%03d".format(i), "Item" + i)
		tree.size should be(smallCount)
		tree.factory.fanout should be(4)
		tree.level should be(4)
		tree.consistent should be(true)
	}

	test("After 7 insertions in reverse should fail with exception") {
		val tree = testTree()
		intercept[IllegalArgumentException] {
			for (i <- 7 to 1 by -1) tree.append(i, "Item" + i)
		}
	}

	test(s"After ${smallCount} insertion should contain ${smallCount} entries in order") {
		val tree = testTree()
		for (i <- 1 to smallCount) {
			tree.append(i, "Item" + i);
			tree.keys() should be((1 to i))
			tree.consistent should be(true)
		}
		tree.size should be(smallCount)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	test(s"After ${largeCount} appends should contain ${largeCount} entries in order") {
		val tree = treeWithFanout(10)
		for (i <- 1 to largeCount) tree.append(i, "Item" + i)
		tree.size should be(largeCount)
		tree.factory.fanout should be(10)
		tree.level should be(4)
	}

	test(s"After ${smallCount} insertions in reverse should fail with exception") {
		val tree = testTree()
		intercept[IllegalArgumentException] {
			for (i <- smallCount to 1 by -1) tree.append(i, "Item" + i)
		}
	}

	test("An ordered range should count N keys forward from a given pivot") {
		val doubleRange = 2 * smallCount
		val tree = testTree()
		for (i <- 1 to smallCount) tree.append(2 * i, "Item" + i)
		tree.get(20) map (_.value) should be(Some("Item10"))
		tree.range(2, smallCount) map (_.key) should be((2 to doubleRange by 2))
		tree.range(0, doubleRange) map (_.key) should be((2 to doubleRange by 2))
		tree.range(20, 10) map (_.key) should be((20 to 38 by 2))
		tree.range(19, 10) map (_.key) should be((20 to 38 by 2))
		tree.range(21, 10) map (_.key) should be((22 to 40 by 2))
		tree.range(doubleRange, 0) map (_.key) should be((Seq()))
		tree.range(doubleRange, 1) map (_.key) should be((Seq(doubleRange)))
		tree.range(doubleRange, smallCount) map (_.key) should be((Seq(doubleRange)))
		tree.range(doubleRange + 1, smallCount) map (_.key) should be((Seq()))
	}

	test("An ordered range should count N keys backwards from a given pivot") {
		val tree = testTree()
		for (i <- 1 to smallCount) tree.append(2 * i, "Item" + i)

		tree.range(1, -1) map (_.key) should be(Seq())
		tree.range(2, -1) map (_.key) should be(Seq(2))
		tree.range(20, -5) map (_.key) should be((20 to 12 by -2))
		tree.range(21, -5) map (_.key) should be((20 to 12 by -2))
	}
}