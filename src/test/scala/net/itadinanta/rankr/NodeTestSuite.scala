package net.itadinanta.rankr;

import org.scalatest.Finders
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import net.itadinanta.rnkr.node.HelloWorld
import net.itadinanta.rnkr.node.NodeBuilder
import net.itadinanta.rnkr.node.DataNode

class NodeTestSuite extends FlatSpec with ShouldMatchers {
	"HelloWorld" should "work" in {
		HelloWorld.run();
	}
	
	"A NodeBuilder" should "compare keys" in {
		val builder = new NodeBuilder[Int, String](_ < _)
		builder.comparator(1, 1) should equal(0)
		builder.comparator(0, 1) should equal(1)
		builder.comparator(1, 0) should equal(-1)
	}

	"A NodeBuilder" should "add keys" in {
		val builder = new NodeBuilder[Int, String](_ < _)

		builder.updateKeyAndValue(1, "One").index should equal(0)
		builder.updateKeyAndValue(3, "Three").index should equal(1)
		builder.updateKeyAndValue(5, "Five").index should equal(2)

		val node1 = builder.newLeafNode;

		node1.keys.toSeq should equal(Seq(1, 3, 5));
		node1.values.toSeq should equal(Seq("One", "Three", "Five"));
		node1.size should equal(3)

		builder.updateKeyAndValue(7, "Seven").index should equal(3)

		val node2 = builder.newLeafNode
		node2.keys.toSeq should equal(Seq(1, 3, 5, 7));
		node2.values.toSeq should equal(Seq("One", "Three", "Five", "Seven"));
		node2.size should equal(4)
	}

	"A NodeBuilder" should "add nodes" in {
		val builder = new NodeBuilder[Int, String](_ < _)

		builder.updateNode(0, new DataNode("One")) should equal(0)
		builder.updateKeyAndValue(3, "Three").index should equal(1)
		builder.updateKeyAndValue(5, "Five").index should equal(2)

		val node1 = builder.newLeafNode;

		node1.keys.toSeq should equal(Seq(1, 3, 5));
		node1.values.toSeq should equal(Seq("One", "Three", "Five"));

		builder.updateKeyAndValue(7, "Seven").index should equal(3)

		val node2 = builder.newLeafNode;
		node2.keys.toSeq should equal(Seq(1, 3, 5, 7));
		node2.values.toSeq should equal(Seq("One", "Three", "Five", "Seven"));
	}

}