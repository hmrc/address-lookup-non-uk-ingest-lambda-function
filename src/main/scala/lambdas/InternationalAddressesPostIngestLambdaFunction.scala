package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap, List => jList}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class InternationalAddressesPostIngestLambdaFunction
  extends RequestHandler[jMap[String, Object], jMap[String, String]] {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): jMap[String, String] = {
    val countries = input.get("countries").asInstanceOf[jList[jMap[String, String]]].asScala.map(_.asScala.toMap).toList
    val schemaName = input.get("schemaName").asInstanceOf[String]
    try {
      Await.ready(doPostIngest(Repository().forIngest, schemaName, countries), 1.minutes)
    } catch {
      case t: Throwable => println(s">>> t: ${t}")
    }
    Map("schemaName" -> input.get("schemaName").asInstanceOf[String]).asJava
  }

  def doPostIngest(repository: IngestRepository, schemaName: String, countries: List[Map[String, String]]): Future[Long] = {
    println(s"Beginning post-ingest processing of international addresses")

    repository.postIngestProcessing(schemaName, countries).unsafeToFuture()
  }
}
