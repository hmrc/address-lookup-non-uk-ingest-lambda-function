package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository
import util.S3FileDownloader
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.{Map => jMap}
import scala.collection.JavaConverters._

class InternationalAddressesFileDownloadLambdaFunction
  extends RequestHandler[jMap[String, String], jMap[String, Object]] {

  override def handleRequest(data: jMap[String, String], contextNotUsed: Context): jMap[String, Object] = {
    val forceDownload = data.asScala.getOrElse("force", "false").toBoolean
    val bucketName = data.asScala.getOrElse("bucketName", Repository.Credentials().nonukBucketName)

    val result = doDownload(
      bucketName,
      Repository.Credentials.apply().nonUkBaseDir,
      forceDownload
    )

    Map("filesToIngest" -> result.map(_.asJava).asJava).asInstanceOf[Map[String, Object]].asJava
  }

  private[lambdas] def doDownload(bucketName: String,
                                  outputBasePath: String,
                                  force: Boolean = false): List[Map[String, String]] = {
    println(s"Beginning download of international addresses")

    new S3FileDownloader(bucketName, outputBasePath)
      .downloadFiles(force = force)
  }
}
