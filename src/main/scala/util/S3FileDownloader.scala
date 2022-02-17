package util

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{
  ListObjectsV2Request,
  S3ObjectInputStream
}
import os.Path
import util.S3FileDownloader.countriesOfInterest

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

class S3FileDownloader(fromBucket: String,
                       outputBasePath: String,
                       prefix: String = "collection-global") {
  println(
    s">>> S3FileDownloader(fromBucket: $fromBucket, outputBasePath: $outputBasePath, prefix: $prefix)"
  )

  def filesShouldBeDownloaded(force: Boolean): Boolean = {
    val doDownload = force || !Paths.get(outputBasePath, prefix).toFile.exists()
    println(s">>> filesShouldBeDownloaded(force: $force): $doDownload")
    doDownload
  }

  def downloadFiles(force: Boolean = false): Int = {
    if (filesShouldBeDownloaded(force)) {
      if (force)
        removeOldFiles()

      val client =
        AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build()

      val fileKeysToDownload = getFileKeysToDownload
      println(s">>> fileKeysToDownload: ${fileKeysToDownload}")
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
  }

  def removeOldFiles(): Unit = {
    println(s">>> Removing old files...")

    val pathsToDelete = {
      os.walk(path = os.Path(outputBasePath)).groupBy(_.toIO.isFile)
    }

    pathsToDelete
      .getOrElse(true, Seq[Path]())
      .foreach { f =>
        println(s">>> Deleting file : ${f}")
        Files.deleteIfExists(f.toIO.toPath)
      }

    pathsToDelete
      .getOrElse(false, Seq[Path]())
      .sortBy(_.segmentCount)
      .reverse
      .foreach { d =>
        println(s">>> Deleting dir: ${d}")
        Files.delete(d.toIO.toPath)
      }
  }

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
  val countriesOfInterest = Set("bm")
}
