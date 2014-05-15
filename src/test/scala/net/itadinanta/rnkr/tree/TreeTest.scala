package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable

class TreeTest extends FlatSpec with ShouldMatchers {
	def createTestTree(): SeqBPlusTree[Int, String] =
		new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntAscending, 4))

	"An empty tree" should "contain no entries" in {
		createTestTree().size should be(0)
	}

	"A simple populated tree" should "contain 1 entry after 1 insertion" in {
		val tree = createTestTree()
		tree.put(1, "Item")
		tree.size should be(1)
		tree.head should not be (null)
	}

	"A simple populated tree" should "contain 3 entry after 3 insertions" in {
		val tree = createTestTree()
		1 to 3 foreach { i => tree.put(i, "Item" + i)}
		tree.size should be(3)
		tree.root.size should be(3)
		tree.head.size should be(3)
		tree.tail.size should be(3)
		tree.leafCount should be(1)
		tree.indexCount should be(0)
	}

	"An indexed populated tree" should "contain 7 after 7 insertions" in {
		val tree = createTestTree()
		1 to 7 foreach { i => tree.put(i, "Item" + i); println(tree)}
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.size should be(2)
		tree.head.size should be(2)
	}

	"An indexed populated tree" should "contain 7 entry after 7 insertions in reverse" in {
		val tree = createTestTree()
		7 to 1 by -1 foreach { i => tree.put(i, "Item" + i); println(tree)}
		tree.size should be(7)
		tree.factory.fanout should be(4)
		tree.level should be(2)
		tree.root.size should be(1)
		tree.head.size should be(4)
	}

	"An indexed populated tree" should "contain 100 entry after 100 insertions" in {
		val tree = createTestTree()
		1 to 100 foreach {
			i => tree.put(i, "Item" + i);
				tree.keys() should be(1 to i)
		}
		println(tree)
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
	}

	"An indexed populated tree" should "contain 100 entries after 100 insertions in reverse" in {
		val tree = createTestTree()
		100 to 1 by -1 foreach { i => tree.put(i, "Item" + i)}
		println(tree)
		tree.keys() should be(1 to 100)
		tree.keysReverse() should be(100 to 1 by -1)
		tree.size should be(100)
	}

	"An indexed populated tree" should "contain 1000 entries after 1000 random insertions" in {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		Random.shuffle(1 to 1000 map { i => i}) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		println(tree)
		tree.keysReverse() should be(1000 to 1 by -1)
		tree.size should be(1000)
	}

	"An indexed decreasing populated tree" should "contain 100 entries in reverse after 100 insertions" in {
		val tree = new SeqBPlusTree[Int, String](new SeqNodeFactory[Int, String](IntDescending, 9))
		1 to 100 foreach { i => tree.put(i, "Item" + i)}
		println(tree)
		tree.keysReverse() should be(1 to 100)
		tree.keys() should be(100 to 1 by -1)
		tree.size should be(100)
	}

	"An ordered range" should "count N keys forward from a given pivot" in {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i)}
		println(tree)
		tree.get(20) should be(Some("Item10"))
		tree.range(2,100) map(_._1) should be(2 to 200 by 2)
		tree.range(0,200) map(_._1) should be(2 to 200 by 2)
		tree.range(20,10) map(_._1) should be(20 to 38 by 2)
		tree.range(19,10) map(_._1) should be(20 to 38 by 2)
		tree.range(21,10) map(_._1) should be(22 to 40 by 2)
		tree.range(200,0) map(_._1) should be(Seq())
		tree.range(200,1) map(_._1) should be(Seq(200))
		tree.range(200,100) map(_._1) should be(Seq(200))
		tree.range(201,100) map(_._1) should be(Seq())
	}

	"An ordered range" should "count N keys backwards from a given pivot" in {
		val tree = createTestTree()
		1 to 100 foreach { i => tree.put(2 * i, "Item" + i)}

		tree.range(1, -1) map(_._1) should be(Seq())
		tree.range(2, -1) map(_._1) should be(Seq(2))
		tree.range(20,-5) map(_._1) should be(20 to 12 by -2)
		tree.range(21,-5) map(_._1) should be(20 to 12 by -2)
	}
}