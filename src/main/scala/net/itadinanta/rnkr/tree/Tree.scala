package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node.{NodeBuilder, NodeRef, RootNode}

trait Tree[K, V] {
	val ordering: Ordering[K]
	val b: Int

	var root: RootNode[K, V]
	var head: NodeRef[K, V]
	var tail: NodeRef[K, V]

	val builder: NodeBuilder[K, V]
}
