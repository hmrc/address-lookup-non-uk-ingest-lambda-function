package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}
import cats.effect.unsafe.implicits.global
import util.Logging

import java.util.{List as jList, Map as jMap}
import scala.collection.JavaConverters.*
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class InternationalAddressesCleanupLambdaFunction
  extends RequestHandler[jMap[String, Object], jMap[String, String]] with Logging {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): jMap[String, String] = {
    val country = input.get("country").asInstanceOf[String]
    val schemaName = input.get("schemaName").asInstanceOf[String]
    try {
      Await.ready(doCleanup(Repository().forIngest, schemaName, country), 15.minutes)
    } catch {
      case t: Throwable => logger.error(s">>> t: ${t}")
    }
    Map("country" -> input.get("country").asInstanceOf[String], "schemaName" -> input.get("schemaName").asInstanceOf[String]).asJava
  }

  def doCleanup(repository: IngestRepository, schemaName: String, country: String): Future[Long] = {
    logger.info(s"Beginning cleanup processing of international addresses")

    repository.expandJsonToFields(schemaName, country).unsafeToFuture()
  }
}
