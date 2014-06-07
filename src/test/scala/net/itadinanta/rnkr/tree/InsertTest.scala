package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.junit.Test
import org.fest.assertions.Assertions.assertThat

class InsertTest extends TreeBaseTest {
	test("An empty tree should contain no entries") {
		assertThat(createTestTree().size) isEqualTo 0
	}

	test("A tree with one entry should have a head and some items in") {
		val tree = createTestTree()
		tree.put(1, "Item")
		assertThat(tree.size) isEqualTo 1
		assertThat(tree.head) isNotNull
	}

	test("A tree with less than fanout entries should have one leaf and no index") {
		val tree = createTestTree()
		1 to 3 foreach { i => tree.put(i, "Item" + i) }
		assertThat(tree.size) isEqualTo 3
		assertThat(tree.root.keys.size) isEqualTo 3
		assertThat(tree.head.keys.size) isEqualTo 3
		assertThat(tree.tail.keys.size) isEqualTo 3
		assertThat(tree.leafCount) isEqualTo 1
		assertThat(tree.indexCount) isEqualTo 0
	}

	test("After 7 insertions should contain 7 entries") {
		val tree = createTestTree()
		1 to 7 foreach { i => tree.put(i, "Item" + i); log.debug("Added {} to {}", i, tree) }
		assertThat(tree.size) isEqualTo 7
		assertThat(tree.factory.fanout) isEqualTo 4
		assertThat(tree.level) isEqualTo 2
		assertThat(tree.root.keys.size) isEqualTo 2
		assertThat(tree.head.keys.size) isEqualTo 2
		assertThat(tree.consistent) isEqualTo true
	}

	test("After 100 insertions with String keys should contain 100 entries") {
		val tree = new SeqBPlusTree[String, String](new SeqNodeFactory[String, String](StringAscending, 4))
		1 to 100 foreach { i => tree.put("Key" + i, "Item" + i) }
		log.debug("Tree with Strings: {}", tree)
		assertThat(tree.size) isEqualTo 100
		assertThat(tree.factory.fanout) isEqualTo 4
		assertThat(tree.level) isEqualTo 4
		assertThat(tree.consistent) isEqualTo true
	}

	test("After 7 insertions in reverse should contain 7 entries") {
		val tree = createTestTree()
		7 to 1 by -1 foreach { i => tree.put(i, "Item" + i); log.debug("Added {} to {}", i, tree) }
		assertThat(tree.size) isEqualTo 7
		assertThat(tree.factory.fanout) isEqualTo 4
		assertThat(tree.level) isEqualTo 2
		assertThat(tree.root.keys.size) isEqualTo 1
		assertThat(tree.head.keys.size) isEqualTo 4
	}

	test("After 100 insertion should contain 100 entries in order") {
		val tree = createTestTree()
		1 to 100 foreach {
			i =>
				tree.put(i, "Item" + i);
				assertThat(tree.keys()) isEqualTo(1 to i)
				log.debug("{}", tree)
				assertThat(tree.consistent) isEqualTo true
		}
		assertThat(tree.size) isEqualTo 100
		assertThat(tree.factory.fanout) isEqualTo 4
		assertThat(tree.level) isEqualTo 4
	}

	test("After 100 insertions in reverse should contain 100 entries in order") {
		val tree = createTestTree()
		100 to 1 by -1 foreach { i => tree.put(i, "Item" + i) }
		log.debug("{}", tree)
		assertThat(tree.keys()) isEqualTo(1 to 100)
		assertThat(tree.keysReverse()) isEqualTo (100 to 1 by -1)
		assertThat(tree.size) isEqualTo 100
	}

	test("After 1000 random insertions should contain 1000 entries in order") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		Random.setSeed(1234L)
		Random.shuffle(1 to 1000 map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			assertThat(tree.keys()) isEqualTo ordered.toList
		}
		log.debug("{}", tree)
		assertThat(tree.keysReverse()) isEqualTo(1000 to 1 by -1)
		assertThat(tree.size) isEqualTo 1000
	}

	test("After 100 insertions in reverse should contain 100 entries in reverse") {
		val tree = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntDescending, 9))
		1 to 100 foreach { i => tree.put(i, "Item" + i) }
		log.debug("{}", tree)
		assertThat(tree.keysReverse()) isEqualTo(1 to 100)
		assertThat(tree.keys()) isEqualTo(100 to 1 by -1)
		assertThat(tree.size) isEqualTo 100
	}

	test("An ordered range should count N keys forward from a given pivot") {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i) }
		log.debug("{}", tree)
		assertThat(tree.get(20) map (_.value)) isEqualTo Some("Item10")
		assertThat(tree.range(2, 100) map (_.key)) isEqualTo(2 to 200 by 2)
		assertThat(tree.range(0, 200) map (_.key)) isEqualTo(2 to 200 by 2)
		assertThat(tree.range(20, 10) map (_.key)) isEqualTo(20 to 38 by 2)
		assertThat(tree.range(19, 10) map (_.key)) isEqualTo(20 to 38 by 2)
		assertThat(tree.range(21, 10) map (_.key)) isEqualTo(22 to 40 by 2)
		assertThat(tree.range(200, 0) map (_.key)) isEqualTo Seq()
		assertThat(tree.range(200, 1) map (_.key)) isEqualTo Seq(200)
		assertThat(tree.range(200, 100) map (_.key)) isEqualTo Seq(200)
		assertThat(tree.range(201, 100) map (_.key)) isEqualTo Seq()
	}

	test("An ordered range should count N keys backwards from a given pivot") {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i) }

		assertThat(tree.range(1, -1) map (_.key)) isEqualTo Seq()
		assertThat(tree.range(2, -1) map (_.key)) isEqualTo Seq(2)
		assertThat(tree.range(20, -5) map (_.key)) isEqualTo(20 to 12 by -2)
		assertThat(tree.range(21, -5) map (_.key)) isEqualTo(20 to 12 by -2)
	}
}