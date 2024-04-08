package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{List => jList, Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class InternationalAddressesCleanupLambdaFunction
  extends RequestHandler[jMap[String, Object], jMap[String, String]] {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): jMap[String, String] = {
    val country = input.get("country").asInstanceOf[String]
    val schemaName = input.get("schemaName").asInstanceOf[String]
    try {
      Await.ready(doCleanup(Repository().forIngest, schemaName, country), 15.minutes)
    } catch {
      case t: Throwable => println(s">>> t: ${t}")
    }
    Map("country" -> input.get("country").asInstanceOf[String], "schemaName" -> input.get("schemaName").asInstanceOf[String]).asJava
  }

  def doCleanup(repository: IngestRepository, schemaName: String, country: String): Future[Long] = {
    println(s"Beginning cleanup processing of international addresses")

    repository.expandJsonToFields(schemaName, country).unsafeToFuture()
  }
}
