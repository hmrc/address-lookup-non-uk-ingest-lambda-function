package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import org.slf4j.LoggerFactory
import repositories.{IngestRepository, Repository}
import util.timed

import java.util.{Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class InternationalAddressesIngestLambdaFunction
    extends RequestHandler[jMap[String, Any], Int] {
  private val logger =
    LoggerFactory.getLogger(classOf[InternationalAddressesIngestLambdaFunction])

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

object InternationalAddressesIngestLambdaFunction extends App {
  val listObjectsRequest = new ListObjectsV2Request()
    .withBucketName("cip-international-addresses-integration")
    .withPrefix("collection-global")
    .withMaxKeys(Integer.MAX_VALUE)
  val listObjectsV2Result =
    AmazonS3ClientBuilder.defaultClient().listObjectsV2(listObjectsRequest)
  listObjectsV2Result.getObjectSummaries.asScala
    .map(s => s.getKey)
    .foreach(k => println(s">>> k: ${k}"))

  val internationalAddressesLambdaFunction =
    new InternationalAddressesIngestLambdaFunction()

  timed {
    Await.result(
      internationalAddressesLambdaFunction
        .doIngest(Repository.forTesting().forIngest, Map()),
      15.minutes
    )
  }
}
