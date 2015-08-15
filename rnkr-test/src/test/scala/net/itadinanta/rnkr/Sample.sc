package net.itadinanta.rnkr
import akka.util.ByteString

object Sample {
	class SimpleKey(val value: ByteString) extends AnyVal {
		override def toString = new String(value.toArray)
	}

	object SimpleKey {
		def apply(v: String) = new SimpleKey(ByteString(v, "UTF8"))
	}

	val a = SimpleKey("a")                    //> a  : net.itadinanta.rnkr.Sample.SimpleKey = a
	val b = SimpleKey("b")                    //> b  : net.itadinanta.rnkr.Sample.SimpleKey = b
	a == b                                    //> res0: Boolean = false
	val c = SimpleKey("a")                    //> c  : net.itadinanta.rnkr.Sample.SimpleKey = a
	a equals c                                //> res1: Boolean = true
	a ## ()                                   //> res2: Int = -960107790
	c ## ()                                   //> res3: Int = -960107790
	SimpleKey("a") == SimpleKey("a")          //> res4: Boolean = true
	println("Welcome to the Scala worksheet") //> Welcome to the Scala worksheet
}