package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// Designed to be run with a single country/schema combo, async
class InternationalAddressesPostIngestLambdaFunction
    extends RequestHandler[jMap[String, Object], Long] {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): Long = {
      println(s">>> input: ${input.asScala.mkString("\n")}")
      val (schemaName, country) = {
        val sinput = input.asScala
        (sinput("schemaName").asInstanceOf[String],
          sinput("country").asInstanceOf[String])
      }

    try {
      Await.result(doPostIngest(Repository().forIngest, schemaName, country), 1.minutes)
    } catch {
      case t: Throwable => 0L
    }
  }

  def doPostIngest(repository: IngestRepository, schemaName: String, country: String): Future[Long] = {
    println(s"Beginning ingest of international addresses")

    repository.postIngestProcessing(schemaName, country).unsafeToFuture()
  }
}
