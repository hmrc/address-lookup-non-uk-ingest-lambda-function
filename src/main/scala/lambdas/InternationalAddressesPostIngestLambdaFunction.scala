package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}
import cats.effect.unsafe.implicits.global
import util.Logging

import java.util.{List as jList, Map as jMap}
import scala.collection.JavaConverters.*
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class InternationalAddressesPostIngestLambdaFunction
  extends RequestHandler[jMap[String, Object], jMap[String, String]] with Logging {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): jMap[String, String] = {
    val countries = input.get("countries").asInstanceOf[jList[jMap[String, String]]].asScala.map(_.asScala.toMap).toList
    val schemaName = input.get("schemaName").asInstanceOf[String]
    try {
      Await.ready(doPostIngest(Repository().forIngest, schemaName, countries), 15.minutes)
    } catch {
      case t: Throwable => logger.error(s">>> t: ${t}")
    }
    Map("schemaName" -> input.get("schemaName").asInstanceOf[String]).asJava
  }

  def doPostIngest(repository: IngestRepository, schemaName: String, countries: List[Map[String, String]]): Future[Long] = {
    logger.info(s"Beginning post-ingest processing of international addresses")

    repository.postIngestProcessing(schemaName, countries).unsafeToFuture()
  }
}
