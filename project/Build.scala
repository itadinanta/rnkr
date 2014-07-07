import sbt._
import Keys._
import scalaz.Scalaz._
import com.typesafe.sbteclipse.core._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._
import scala.xml.transform._

object ClasspathentryRewriteRule extends RewriteRule {
	import scala.xml._
	override def transform(parent: Node): Seq[Node] = {
		val ivyHome = System.getProperty("user.home") + "/.ivy2"
		parent match {
			case c @ <classpathentry/> if (((c \ "@kind").toString() == "lib") && ((c \ "@path").toString() startsWith ivyHome)) => {
				<classpathentry kind="var" path={ (c \ "@path").toString.replaceAll(ivyHome, "IVY_HOME") }/>
			}
			case other => other
		}
	}
}

object ClasspathentryTransformer extends EclipseTransformerFactory[RewriteRule] {
	override def createTransformer(ref: ProjectRef, state: State): Validation[RewriteRule] = {
		ClasspathentryRewriteRule.success
	}
}
  