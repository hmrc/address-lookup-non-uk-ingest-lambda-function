package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository
import util.S3FileDownloader
import java.util.{Map => jMap}
import scala.collection.JavaConverters._

class InternationalAddressesFileDownloadLambdaFunction
    extends RequestHandler[jMap[String, String], Int] {

  override def handleRequest(data: jMap[String, String],
                             contextNotUsed: Context): Int = {
    val bucketName = data.asScala.getOrElse(
      "bucketName",
      throw new IllegalArgumentException("Please specify bucketName")
    )
    val forceDownload = data.asScala.getOrElse("force", "false").toBoolean

    doDownload(
      bucketName,
      Repository.Credentials.apply().csvBaseDir,
      forceDownload
    )
  }

  private[lambdas] def doDownload(bucketName: String,
                                  outputBasePath: String,
                                  force: Boolean = false): Int = {
    println(s"Beginning download of international addresses")

    new S3FileDownloader(bucketName, outputBasePath)
      .downloadFiles(force = force)

  }
}

object InternationalAddressesFileDownloadLambdaFunction extends App {
  val downloader = new InternationalAddressesFileDownloadLambdaFunction()
  downloader.doDownload(
    "cip-international-addresses-integration",
    "/Users/saqib/Temp/",
    true
  )
}
