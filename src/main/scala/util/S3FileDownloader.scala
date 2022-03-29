package util

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ListObjectsV2Request, S3ObjectInputStream}
import os.Path
import util.S3FileDownloader.countriesOfInterest

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import scala.annotation.tailrec
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

  private def getFileKeysToDownload: List[String] = {
    def getListObjectRequest(client: AmazonS3, token: Option[String] = None) = {
      val baseRequest = new ListObjectsV2Request()
        .withBucketName(fromBucket)
        .withPrefix(prefix)

      val request = token.fold(baseRequest)(baseRequest.withContinuationToken)
      val result = client.listObjectsV2(request)
      (result, Option(result.getNextContinuationToken))
    }

    @tailrec
    def doGetFilesKeysToDownload(initial: Boolean, client: AmazonS3, token: Option[String], acc: List[String]): List[String] = (initial, token) match {
      case (true, _) =>
        val (result, continuationToken) = getListObjectRequest(client)
        doGetFilesKeysToDownload(initial = false, client, continuationToken,
          acc ++ result.getObjectSummaries.asScala.map(_.getKey))

      case (_, to@Some(t)) =>
        val (result, continuationToken) = getListObjectRequest(client, to)
        doGetFilesKeysToDownload(initial = false, client, continuationToken,
          acc ++ result.getObjectSummaries.asScala.map(_.getKey))

      case (_, None) =>
        acc
    }

    val client = AmazonS3ClientBuilder.defaultClient()

    doGetFilesKeysToDownload(initial = true, client, None, List())
      .filter { s => countriesOfInterest.exists(c => s.contains(s"/$c/")) }
  }
}

object S3FileDownloader {
  val countriesOfInterest = Set("bm", "vg")
}
