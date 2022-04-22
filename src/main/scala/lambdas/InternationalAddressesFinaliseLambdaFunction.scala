package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class InternationalAddressesFinaliseLambdaFunction
  extends RequestHandler[jMap[String, Object], Boolean] {

  override def handleRequest(input: jMap[String, Object],
                             contextNotUsed: Context): Boolean = {
    val schemaName = input.get("schemaName").asInstanceOf[String]
    Await.result(doFinalise(Repository().forIngest, schemaName), 5.minutes)
  }

  def doFinalise(repository: IngestRepository, schemaName: String): Future[Boolean] = {
    println(s"Beginning finalisation of international addresses")

    repository.finaliseSchema(schemaName).unsafeToFuture()
  }
}
