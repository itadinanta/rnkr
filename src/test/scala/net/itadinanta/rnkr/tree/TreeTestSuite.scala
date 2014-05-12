package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import net.itadinanta.rnkr.tree._

class TreeTestSuite extends FlatSpec with ShouldMatchers {
	val factory = new SeqNodeFactory[Int, String](IntAscending, 4)

	def createTestTree(): SeqBPlusTree[Int, String] = new SeqBPlusTree[Int, String](factory)

	"An empty tree" should "contain no entries" in {
		createTestTree().size should be (0)
	}

	"A simple populated tree" should "contain 1 entry after 1 insertion" in {
		val tree = createTestTree()
		tree.put(1, "Item")
		tree.size should be (1)
		tree.head should not be (null)
	}

	"A simple populated tree" should "contain 3 entry after 3 insertions" in {
		val tree = createTestTree()
		1 to 3 foreach { i => tree.put(i, "Item" + i) }
		tree.size should be (3)
		tree.root.size should be (3)
		tree.head.size should be (3)
		tree.tail.size should be (3)
		tree.leafCount should be (1)
		tree.indexCount should be (0)
	}

	"An indexed populated tree" should "contain 7 after 7 insertions" in {
		val tree = createTestTree()
		1 to 7 foreach { i => tree.put(i, "Item" + i); println(tree) }
		tree.size should be (7)
		tree.factory.fanout should be (4)
		tree.level should be (2)
		tree.root.size should be (2)
		tree.head.size should be (2)
	}

	"An indexed populated tree" should "contain 7 entry after 7 insertions in reverse" in {
		val tree = createTestTree()
		7 to 1 by -1 foreach { i => tree.put(i, "Item" + i); println(tree) }
		tree.size should be (7)
		tree.factory.fanout should be (4)
		tree.level should be (2)
		tree.root.size should be (1)
		tree.head.size should be (4)
	}

	"An indexed populated tree" should "contain 100 entry after 100 insertions" in {
		val tree = createTestTree()
		1 to 100 foreach {
			i => tree.put(i, "Item" + i);
			println(tree)
			tree.keys() should be (1 to i)
		}
		tree.size should be (100)
		tree.factory.fanout should be (4)
		tree.level should be (4)
	}
}