package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class InternationalAddressesIngestLambdaFunction
    extends RequestHandler[jMap[String, Any], Long] {

  override def handleRequest(data: jMap[String, Any],
                             contextNotUsed: Context): Long = {
    val inputs: Map[String, Any] = data.asScala.toMap

    Await.result(doIngest(Repository().forIngest, inputs), 15.minutes)
  }

  private[lambdas] def doIngest(repository: IngestRepository,
                                inputs: Map[String, Any]): Future[Long] = {
    println(s"Beginning ingest of international addresses")

    repository.ingest().unsafeToFuture()
  }
}
