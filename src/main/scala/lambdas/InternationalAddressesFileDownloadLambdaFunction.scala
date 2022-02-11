package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.Repository
import util.S3FileDownloader

class InternationalAddressesFileDownloadLambdaFunction
    extends RequestHandler[String, Int] {
  private val logger =
    LoggerFactory.getLogger(classOf[InternationalAddressesIngestLambdaFunction])

  override def handleRequest(bucketName: String,
                             contextNotUsed: Context): Int = {

    doDownload(bucketName, Repository.Credentials.apply().csvBaseDir)
  }

  private[lambdas] def doDownload(bucketName: String,
                                  outputBasePath: String): Int = {
    logger.info(s"Beginning ingest of international addresses")

    new S3FileDownloader(bucketName, outputBasePath).downloadFiles()

  }
}

object InternationalAddressesFileDownloadLambdaFunction extends App {
  val downloader = new InternationalAddressesFileDownloadLambdaFunction()
  downloader.doDownload(
    "cip-international-addresses-integration",
    "/Users/saqib/Temp/"
  )
}
