package net.itadinanta.rnkr.frontend

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FrontendServiceSpec extends Specification with Specs2RouteTest with Service {
	def actorRefFactory = system
	val executionContext = system.dispatcher
	"MyService" should {
		"process a count request" in {
			Get("/rnkr/tree/test/count") ~> rnkrRoute ~> check {
				handled must beTrue
				responseAs[String] === "0"
			}
		}
	}
}
