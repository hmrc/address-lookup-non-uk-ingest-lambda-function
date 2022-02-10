package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.{IngestRepository, Repository}
import util.timed

import java.util.{Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class InternationalAddressesLambdaFunction
    extends RequestHandler[jMap[String, Any], Int] {
  private val logger =
    LoggerFactory.getLogger(classOf[InternationalAddressesLambdaFunction])

  override def handleRequest(data: jMap[String, Any],
                             contextNotUsed: Context): Int = {
    val inputs: Map[String, Any] = data.asScala.asInstanceOf[Map[String, Any]]

    Await
      .result(doIngest(Repository().forIngest, inputs), 50.seconds)
  }

  private[lambdas] def doIngest(repository: IngestRepository,
                                inputs: Map[String, Any]): Future[Int] = {
    logger.info(s"Beginning ingest of international addresses")

    repository.ingest().unsafeToFuture()
  }
}

object InternationalAddressesLambdaFunction extends App {

  val internationalAddressesLambdaFunction =
    new InternationalAddressesLambdaFunction()

  timed {
    Await.result(
      internationalAddressesLambdaFunction
        .doIngest(Repository.forTesting().forIngest, Map()),
      15.minutes
    )
  }
}
