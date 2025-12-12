package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}
import cats.effect.unsafe.implicits.global
import util.Logging

import java.util.Map as jMap
import scala.collection.JavaConverters.*
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class InternationalAddressesIngestLambdaFunction
  extends RequestHandler[jMap[String, Object], Long] with Logging {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): Long = {
    val (schemaName, country, file) = {
      val scalaInput = input.asScala
      val _schemaName = scalaInput.apply("schemaName").asInstanceOf[String]
      val _country = scalaInput.apply("country").asInstanceOf[String]
      val _file = scalaInput.apply("file").asInstanceOf[String]
      (_schemaName, _country, _file)
    }

    Await.result(doIngest(Repository().forIngest, schemaName, country, file), 15.minutes)
  }

  def doIngest(repository: IngestRepository, schemaName: String, country: String, fileToIngest: String): Future[Long] = {
    logger.info(s"Beginning ingest of international addresses")

    repository.ingest(schemaName, country, fileToIngest).unsafeToFuture()
  }
}
