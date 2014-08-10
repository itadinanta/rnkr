package net.itadinanta.rnkr.node

import org.scalatest.FunSuite

class SimpleLambdaTest extends FunSuite {
	test("Three flavours of function call") {
		def f1(doPrint: Boolean, x: Int) = if (doPrint) println(x) else println("-")
		f1(true, f(1) + f(1))

		def f2(doPrint: Boolean, x: => Int) = if (doPrint) println(x) else println("-")
		f2(true, f(3) + f(3))

		def f3(doPrint: Boolean, x: () => Int) = if (doPrint) println(x()) else println("-")
		f3(true, () => { f(5) + f(5) })

		def f(i: Int) = { println("Evaluated " + i); i }

		f1(false, f(2) + f(2))
		f2(false, f(4) + f(4))
		f3(false, () => { f(6) + f(6) })
	}
}