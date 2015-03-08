package net.itadinanta.rnkr.frontend

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._

class FrontendServiceSpec extends Specification with Specs2RouteTest with Service {
	def actorRefFactory = system

	"MyService" should {

		"return a greeting for GET requests to the root path" in {
			Get() ~> rnkrRoute ~> check {
				responseAs[String] must contain("Say hello")
			}
		}

		"leave GET requests to other paths unhandled" in {
			Get("/kermit") ~> rnkrRoute ~> check {
				handled must beFalse
			}
		}

		"return a MethodNotAllowed error for PUT requests to the root path" in {
			Put() ~> sealRoute(rnkrRoute) ~> check {
				status === MethodNotAllowed
				responseAs[String] === "HTTP method not allowed, supported methods: GET"
			}
		}
	}
}
