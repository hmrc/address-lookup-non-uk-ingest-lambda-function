package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// Designed to be run with a single country/schema combo, async
class InternationalAddressesCreateIndexesLambdaFunction
    extends RequestHandler[jMap[String, Object], Unit] {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): Unit = {
      println(s">>> input: ${input.asScala.mkString("\n")}")
      val (schemaName, country) = {
        val sinput = input.asScala
        (sinput("schemaName").asInstanceOf[String],
          sinput("country").asInstanceOf[String])
      }

    try {
      Await.ready(doCreateMaterializedView(Repository().forIngest, schemaName, country), 30.seconds)
    } catch {
      case error: Throwable => println(s">>> error: ${error}")
    }
  }

  def doCreateMaterializedView(repository: IngestRepository, schemaName: String, country: String): Future[Long] = {
    println(s"Beginning ingest of international addresses")

    repository.createMaterializedViewIndexes(schemaName, country).unsafeToFuture()
  }
}
