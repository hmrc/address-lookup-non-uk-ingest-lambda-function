package repositories

import cats.effect.{IO, Resource}
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import doobie._
import doobie.implicits._
import doobie.postgres.{PFCM, PHC}
import os.Path
import repositories.Repository.Credentials

import java.io.{File, FileInputStream, InputStream}
import scala.io.Source

class IngestRepository(transactor: => Transactor[IO],
                       private val credentials: Credentials) {

  private val rootDir: String = {
    if (new File(credentials.csvBaseDir).isAbsolute) credentials.csvBaseDir
    else os.pwd.toString + "/" + credentials.csvBaseDir
  }

  def ingest(): IO[Long] = {
    for {
      _ <- printLine(s""">>> ingest() beginning""")
      schemaName <- findCurrentSchema()
      _ <- printLine(s">>> schemaName: ${schemaName}")
      basePath <- IO(s"$rootDir/collection-global")
      _ <- printLine(s">>> basePath: ${basePath}")
      updateCount <- ingestFiles(schemaName, basePath)
      _ <- printLine(s">>> updateCount: ${updateCount}")
    } yield updateCount
  }

  private def findCurrentSchema(): IO[String] =
    for {
      _ <- printLine(s">>> findCurrentSchema() beginning")
      res <- sql"SELECT schema_name FROM public.address_lookup_status WHERE status = 'finalised' ORDER BY timestamp DESC LIMIT 1"
        .query[String]
        .unique
        .transact(transactor)
    } yield res

  def initialiseCountryTableInSchema(schemaName: String,
                                     countryCode: String): IO[Int] =
    for {
      rawDdl <- resourceAsString("/create_raw_table_ddl.sql")
      ddl <- IO {
        rawDdl
          .replace("__schema__", schemaName)
          .replace("__table__", countryCode)
      }
      res <- Fragment.const(ddl).update.run.transact(transactor)
    } yield res

  def ingestFilesFromS3(s3Bucket: String,
                        schemaName: String,
                        countryDataDir: String): IO[Int] = {
    val listObjectsRequest = new ListObjectsV2Request()
      .withBucketName(s3Bucket)
      .withPrefix("collection-global")
      .withMaxKeys(Integer.MAX_VALUE)
    val listObjectsV2Result =
      AmazonS3ClientBuilder.defaultClient().listObjectsV2(listObjectsRequest)
    listObjectsV2Result.getObjectSummaries
    ???
  }

  def ingestFiles(schemaName: String, countryDataDir: String): IO[Long] =
    for {
      _ <- printLine(s""">>> ingestFiles($schemaName, $countryDataDir)""")
      filesOfInterest <- getCountryToFilesOfInterestMap(countryDataDir)
      _ <- printLine(s">>> filesOfInterest: ${filesOfInterest}")
      res <- processCountryFiles(schemaName, filesOfInterest)
    } yield res

  private def processCountryFiles(
    schemaName: String,
    coToFilesMap: Map[String, Seq[Path]]
  ): IO[Long] = {
    coToFilesMap
      .map {
        case (countryCode, files) =>
          println(s">>> countryCode: ${countryCode}, files: ${files.length}")
          initialiseCountryTableInSchema(schemaName, countryCode)
            .flatMap { _ =>
              files
                .map { f =>
                  ingestFile(schemaName, countryCode, s"$f")
                }
                .fold(IO(0L)) {
                  case (aio, bio) =>
                    for { a <- aio; b <- bio } yield a + b
                }
            }
            .flatMap { _ =>
              createMaterializedView(schemaName, countryCode)
            }
      }
      .toList
      .sequence
      .map((x: List[Long]) => x.sum)
  }

  private def getCountryToFilesOfInterestMap(
    countryDataDir: String
  ): IO[Map[String, Seq[Path]]] = IO {
    os.walk(path = os.Path(countryDataDir))
      .filter { p =>
        p.toIO.getName.endsWith("geojson")
      }
      .groupBy { p =>
        val countriesDir = p.toString().replace(countryDataDir, "")
        val seg = os.Path(countriesDir).getSegment(0)
        seg
      }
  }

  def ingestFile(schemaName: String,
                 table: String,
                 filePath: String): IO[Long] =
    for {
      _ <- printLine(
        s">>> Ingest international file $filePath into table $table"
      )
      resrc <- IO(
        Resource.make[IO, InputStream](IO(new FileInputStream(filePath)))(
          s => IO(s.close())
        )
      )
      copySql <- resourceAsString("/raw_copy.sql").map(
        s =>
          s.replaceAll("__schema__", schemaName).replaceAll("__table__", table)
      )
      res <- resrc.use[Long] { in =>
        PHC.pgGetCopyAPI(PFCM.copyIn(copySql, in)).transact(transactor)
      }
    } yield res

  def createMaterializedView(schemaName: String, table: String): IO[Long] =
    for {
      _ <- printLine(s">>> Creating materialized view $table")
      ddlSql <- resourceAsString("/create_table_ddl.sql").map(
        s =>
          s.replaceAll("__schema__", schemaName).replaceAll("__table__", table)
      )
      res <- Fragment.const(ddlSql).update.run.transact(transactor)
    } yield res

  private def resourceAsString(name: String): IO[String] = {
    Resource
      .make[IO, Source](
        IO(Source.fromURL(getClass.getResource(name), "utf-8"))
      )(s => IO(s.close()))
      .use(s => IO(s.mkString))
  }

  private def printLine(s: String): IO[Unit] = IO(println(s))
}
