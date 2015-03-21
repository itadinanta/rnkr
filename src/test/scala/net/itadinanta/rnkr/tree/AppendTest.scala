package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.fest.assertions.Assertions.assertThat

class AppendTest extends TreeBaseTest {

	test("An empty tree should contain no entries") {
		assertThat(createTestTree().size) isEqualTo 0
	}

	test("A tree with one entry should have a head and some items in") {
		val tree = createTestTree()
		tree.append(1, "Item")
		assertThat(tree.size) isEqualTo 1
		assertThat(tree.head) isNotNull
	}

	test("A tree with less than fanout entries should have one leaf and no index") {
		val tree = createTestTree()
		1 to 3 foreach { i => tree.append(i, "Item" + i) }
		assertThat(tree.size) isEqualTo 3
		assertThat(tree.root.keys.size) isEqualTo 3
		assertThat(tree.head.keys.size) isEqualTo 3
		assertThat(tree.tail.keys.size) isEqualTo 3
		assertThat(tree.leafCount) isEqualTo 1
		assertThat(tree.indexCount) isEqualTo 0
	}

	test("After 7 insertions should contain 7 entries") {
		val tree = createTestTree()
		for (i <- 1 to 7) { tree.append(i, "Item" + i); debug(s"Added ${i} to ${tree}") }
		assertThat(tree.size) isEqualTo 7
		assertThat(tree.factory.fanout) isEqualTo 4
		assertThat(tree.level) isEqualTo 2
		assertThat(tree.root.keys.size) isEqualTo 2
		assertThat(tree.head.keys.size) isEqualTo 2
		assertThat(tree.consistent) isTrue
	}

	test("After 100 insertions with String keys should contain 100 entries") {
		val tree = RankedTreeMap[String, String](StringAscending, 4)
		for (i <- 1 to 100) tree.append("Key%03d".format(i), "Item" + i)
		debug(s"RankedTreeMap with Strings: ${tree}")
		assertThat(tree.size) isEqualTo 100
		assertThat(tree.factory.fanout) isEqualTo 4
		assertThat(tree.level) isEqualTo 4
		assertThat(tree.consistent) isEqualTo true
	}

	test("After 7 insertions in reverse should fail with exception") {
		val tree = createTestTree()
		intercept[IllegalArgumentException] {
			7 to 1 by -1 foreach { i => tree.append(i, "Item" + i); debug(s"Added ${i} to ${tree}") }
		}
	}

	test("After 100 insertion should contain 100 entries in order") {
		val tree = createTestTree()
		1 to 100 foreach {
			i =>
				tree.append(i, "Item" + i);
				assertThat(tree.keys()) isEqualTo (1 to i)
				debug(tree)
				assertThat(tree.consistent) isEqualTo true
		}
		assertThat(tree.size) isEqualTo 100
		assertThat(tree.factory.fanout) isEqualTo 4
		assertThat(tree.level) isEqualTo 4
	}

	test("After 1000000 appends should contain 1000000 entries in order") {
		val tree = this.createTreeWithFanout(100)
		1 to 1000000 foreach { i => tree.append(i, "Item" + i); }
		assertThat(tree.size) isEqualTo 1000000
		assertThat(tree.factory.fanout) isEqualTo 100
		assertThat(tree.level) isEqualTo 4
	}

	test("After 100 insertions in reverse should fail with exception") {
		val tree = createTestTree()
		intercept[IllegalArgumentException] {
			100 to 1 by -1 foreach { i => tree.append(i, "Item" + i) }
		}
	}

	test("An ordered range should count N keys forward from a given pivot") {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.append(2 * i, "Item" + i) }
		debug(tree)
		assertThat(tree.get(20) map (_.value)) isEqualTo Some("Item10")
		assertThat(tree.range(2, 100) map (_.key)) isEqualTo (2 to 200 by 2)
		assertThat(tree.range(0, 200) map (_.key)) isEqualTo (2 to 200 by 2)
		assertThat(tree.range(20, 10) map (_.key)) isEqualTo (20 to 38 by 2)
		assertThat(tree.range(19, 10) map (_.key)) isEqualTo (20 to 38 by 2)
		assertThat(tree.range(21, 10) map (_.key)) isEqualTo (22 to 40 by 2)
		assertThat(tree.range(200, 0) map (_.key)) isEqualTo (Seq())
		assertThat(tree.range(200, 1) map (_.key)) isEqualTo (Seq(200))
		assertThat(tree.range(200, 100) map (_.key)) isEqualTo (Seq(200))
		assertThat(tree.range(201, 100) map (_.key)) isEqualTo (Seq())
	}

	test("An ordered range should count N keys backwards from a given pivot") {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.append(2 * i, "Item" + i) }

		assertThat(tree.range(1, -1) map (_.key)) isEqualTo Seq()
		assertThat(tree.range(2, -1) map (_.key)) isEqualTo Seq(2)
		assertThat(tree.range(20, -5) map (_.key)) isEqualTo (20 to 12 by -2)
		assertThat(tree.range(21, -5) map (_.key)) isEqualTo (20 to 12 by -2)
	}
}