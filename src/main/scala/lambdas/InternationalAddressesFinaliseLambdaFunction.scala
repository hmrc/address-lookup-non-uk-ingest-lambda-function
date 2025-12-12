package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}
import cats.effect.unsafe.implicits.global
import util.Logging

import java.util.Map as jMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class InternationalAddressesFinaliseLambdaFunction
  extends RequestHandler[jMap[String, Object], Boolean] with Logging {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): Boolean = {
    val schemaName = input.get("schemaName").asInstanceOf[String]
    Await.result(doFinalise(Repository().forIngest, schemaName), 5.minutes)
  }

  def doFinalise(repository: IngestRepository, schemaName: String): Future[Boolean] = {
    logger.info(s"Beginning finalisation of international addresses")

    repository.finaliseSchema(schemaName).unsafeToFuture()
  }
}
