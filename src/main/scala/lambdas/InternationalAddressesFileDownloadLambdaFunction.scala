package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository
import util.S3FileDownloader
import java.util.{Map => jMap, List => jList}
import scala.collection.JavaConverters._

class InternationalAddressesFileDownloadLambdaFunction
    extends RequestHandler[jMap[String, String], jMap[String, jList[jMap[String, String]]]] {

  override def handleRequest(data: jMap[String, String],
                             contextNotUsed: Context): jMap[String, jList[jMap[String, String]]] = {
    // OVerride bucket using the provided value
    val bucketName = data.asScala.getOrElse("bucketName", Repository.Credentials().nonukBucketName)
    val forceDownload = data.asScala.getOrElse("force", "false").toBoolean

    val result = doDownload(
      bucketName,
      Repository.Credentials.apply().csvBaseDir,
      forceDownload
    )

    Map("filesToIngest" -> result.map(_.asJava).asJava).asJava
  }

  private[lambdas] def doDownload(bucketName: String,
                                  outputBasePath: String,
                                  force: Boolean = false): List[Map[String, String]] = {
    println(s"Beginning download of international addresses")

    new S3FileDownloader(bucketName, outputBasePath)
      .downloadFiles(force = force)

  }
}
