package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}
import cats.effect.unsafe.implicits.global
import java.util.{Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class InternationalAddressesCheckStatusLambdaFunction
  extends RequestHandler[String, jMap[String, String]] {

  override def handleRequest(schemaName: String,
                             contextNotUsed: Context): jMap[String, String] = {
    Await.result(doCheckStatus(Repository().forIngest, schemaName), 5.minutes).asJava
  }

  def doCheckStatus(repository: IngestRepository, schemaName: String): Future[Map[String, String]] = {
    println(s"Beginning check status of international addresses")

    repository.checkStatus(schemaName).map(statii => statii.flatMap {
      case (_, status, errorMessage) => Map("status" -> status, "errorMessage" -> errorMessage.orNull)
    }.groupBy(_._1)
      .mapValues(_.toSet)
      .mapValues(_.map(_._2))
      .mapValues(_.filterNot(_ == null))
      .mapValues(_.mkString)
      .mapValues(v => if (v.isEmpty) null else v)
    ).map { r =>
      Map("status" -> r("status"), "errorMessage" -> r("errorMessage"))
    }.unsafeToFuture()
  }
}
