package util

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{
  ListObjectsV2Request,
  S3ObjectInputStream
}
import util.S3FileDownloader.countriesOfInterest

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.Paths
import scala.collection.JavaConverters._

class S3FileDownloader(fromBucket: String,
                       outputBasePath: String,
                       prefix: String = "collection-global") {
  def areFilesAlreadyDownloaded: Boolean =
    Paths.get(outputBasePath, prefix).toFile.exists()

  def downloadFiles(): Int =
    if (!areFilesAlreadyDownloaded) {
      val client =
        AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build()

      val fileKeysToDownload = getFileKeysToDownload
      fileKeysToDownload.par.foreach { key =>
        val fileIn: S3ObjectInputStream =
          client.getObject(fromBucket, key).getObjectContent
        val outputFile = Paths.get(outputBasePath, key).toFile
        if (!outputFile.getParentFile.exists())
          outputFile.getParentFile.mkdirs()
        val fileOut: BufferedOutputStream =
          new BufferedOutputStream(new FileOutputStream(outputFile))

        val buffer = new Array[Byte](4096)
        var bytesRead = -1
        var cont = true

        while (cont) {
          bytesRead = fileIn.read(buffer)
          if (bytesRead == -1) cont = false
          else {
            fileOut.write(buffer, 0, bytesRead)
          }
        }
        fileIn.close()
        fileOut.close()
      }
      fileKeysToDownload.length
    } else 0

  def getFileKeysToDownload: List[String] = {
    val client = AmazonS3ClientBuilder.defaultClient()

    val listObjectRequest = new ListObjectsV2Request()
      .withMaxKeys(1000)
      .withBucketName(fromBucket)
      .withPrefix(prefix)

    client
      .listObjectsV2(listObjectRequest)
      .getObjectSummaries
      .asScala
      .filter(s => countriesOfInterest.exists(c => s.getKey.contains(s"/$c/")))
      .map(_.getKey)
      .toList
  }
}

object S3FileDownloader {
  val countriesOfInterest = Set("bm", "be", "nl")
}
