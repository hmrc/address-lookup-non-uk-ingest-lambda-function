package util

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import os.Path
import util.S3FileDownloader.{countriesOfInterest, country}

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.nio.file.{Files, Paths}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex

class S3FileDownloader(fromBucket: String,
                       outputBasePath: String,
                       prefix: String = "collection-global") {
  println(
    s">>> S3FileDownloader(fromBucket: $fromBucket, outputBasePath: $outputBasePath, prefix: $prefix)"
  )

  // Ensure that the outputBasePath exists
  val _outputBasePath = Paths.get(outputBasePath).toFile
  if (!_outputBasePath.exists()) _outputBasePath.mkdirs()

  def filesShouldBeDownloaded(force: Boolean): Boolean = {
    val doDownload = force || !Paths.get(outputBasePath, prefix).toFile.exists()
    println(s">>> filesShouldBeDownloaded(force: $force): $doDownload")
    doDownload
  }

  def downloadFiles(force: Boolean = false): List[Map[String, String]] = {
    if (filesShouldBeDownloaded(force)) {
      if (force)
        removeOldFiles()

      val client =
        AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build()

      val countryToFileKeysToDownload: List[(String, String)] = getFileKeysToDownload
      println(s">>> countryToFileKeysToDownload: ${countryToFileKeysToDownload}")
      val done = countryToFileKeysToDownload.par.map { case (cc, filesToDownload) => cc -> downloadFile(client, filesToDownload) }
      done.toList.flatMap { case (cc, fls) => fls.map(f => Map("country" -> cc, "file" -> f)) }
    } else List()
  }

  val maxFileLines = 200000

  def downloadFile(client: AmazonS3, key: String): List[String] = {
    val s3File = client.getObject(fromBucket, key)
    val fileContentStream = s3File.getObjectContent

    val fileIn: BufferedSource = Source.fromInputStream(fileContentStream)

    val outputFileBase = {
      val of = Paths.get(outputBasePath, key).toFile
      if (!of.getParentFile.exists()) of.getParentFile.mkdirs()
      of
    }

    val fileInGrouped = fileIn.getLines.grouped(maxFileLines).zipWithIndex

    val res = fileInGrouped.map {
      case (lines, idx) =>
        val indexedOutputFileBase = outputFileBase.getAbsolutePath.replaceAll(".geojson$", s".$idx.geojson")
        val fileOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(indexedOutputFileBase)))
        doCopy(lines, fileOut)
        fileOut.close()
        indexedOutputFileBase
    }.toList

    fileIn.close()

    res
  }

  private def doCopy(lines: Seq[String], out: BufferedWriter): Unit = {
    lines.foreach { l =>
      out.write(cleanup(l))
      out.newLine()
    }
  }

  // Some lines contain escaped strings which don't play well with the pg copy command
  private def cleanup(str: String): String = {
    str.replaceAll("""\\"""", "")
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

  private def getFileKeysToDownload: List[(String, String)] = {
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
      .filter { s => countriesOfInterest.exists(c => s.startsWith(s"${prefix}/$c/")) }
      .map(s => country(s, prefix) -> s)
  }
}

object S3FileDownloader {
  val countriesOfInterest: Set[String] = Set("nl")

  def countryPattern(prefix: String): Regex = {
    val countryPatternString = s"${prefix}/(\\p{Lower}{2})/.+"
    countryPatternString.r
  }

  def country(p: String, prefix: String): String = {
    val theCountryPattern = countryPattern(prefix)
    println(s">>> p: ${p}")
    p match {
      case theCountryPattern(c) => c
      case _                    => ???
    }
  }
}
