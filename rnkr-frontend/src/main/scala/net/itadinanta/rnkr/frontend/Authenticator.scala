package net.itadinanta.rnkr.frontend

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.language.postfixOps

import net.itadinanta.rnkr.engine.Partition
import spray.routing.authentication.BasicAuth
import spray.routing.authentication.UserPass
import spray.routing.directives.AuthMagnet
import spray.routing.directives.AuthMagnet.fromContextAuthenticator

protected trait Authenticator {
	case class Role(name: String)
	object Role {
		object Guest extends Role("guest")
	}
	def authenticator(partitionName: String)(implicit ec: ExecutionContext): AuthMagnet[Role]
}

trait PartitionBasedAuthenticator extends Authenticator {
	val partitions: Map[String, Partition]

	override def authenticator(partitionName: String)(implicit ec: ExecutionContext): AuthMagnet[Role] = {
		val credentials = partitions.get(partitionName).get.credentials
		def authenticator(userPass: Option[UserPass]): Future[Option[Role]] = {
			if (credentials.isEmpty)
				Future.successful(Some(Role.Guest))
			else userPass match {
				case Some(UserPass(user, pass)) if (credentials.get(user) == Some(pass)) =>
					Future.successful(Some(Role(user)))
				case _ =>
					Future.successful(None)
			}
		}
		BasicAuth(authenticator _, realm = "rnkr")
	}
}
