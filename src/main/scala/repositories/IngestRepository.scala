package repositories

import cats.effect.{IO, Resource}
import cats.implicits._
import util.S3FileDownloader
import doobie._
import doobie.implicits._
import doobie.postgres.{PFCM, PHC}
import doobie.util.fragment.Fragment.{const => csql}
import repositories.Repository.Credentials

import java.io.{File, FileInputStream, InputStream}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.io.Source

class IngestRepository(transactor: => Transactor[IO],
                       private val credentials: Credentials) {

  private val rootDir: String = {
    if (new File(credentials.csvBaseDir).isAbsolute) credentials.csvBaseDir
    else os.pwd.toString + "/" + credentials.csvBaseDir
  }

  type CountryCode = String

  def ingest(schemaName: String, country: String, fileToIngest: String): IO[Long] = {
    for {
      updateCount <- ingestFile(schemaName, country, fileToIngest)
      _           <- printLine(s">>> updateCount: ${updateCount}")
    } yield updateCount
  }

  private val timestampFormat = DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss")

  private def createSchemaName(): IO[String] = IO {
    val timestamp = LocalDateTime.now(ZoneId.of("UTC"))
    s"nonuk_${timestampFormat.format(timestamp)}"
  }

  def createSchema(): IO[String] = for {
    _           <- printLine(s">>> createSchema() beginning")
    schemaName  <- createSchemaName()
    rawDdl      <- resourceAsString("/create_schema_ddl.sql")
    ddl         <- IO (rawDdl.replace("__schema__", schemaName))
    _           <- csql(ddl).update.run.transact(transactor)
    rawProcDdl  <- resourceAsString("/create_view_function_ddl.sql")
    procDdl     <- IO {
                      S3FileDownloader.countriesOfInterest.map{c =>
                        rawProcDdl.replace("__schema__", schemaName)
                          .replace("__table__", c)
                      }.toList
                   }
    _           <- procDdl.map(pddl => csql(pddl).update.run.transact(transactor)).sequence
    _           <- initialiseCountryTablesInSchema(schemaName)
  } yield schemaName

  def initialiseCountryTablesInSchema(schemaName: String): IO[Int] = for {
    x <- S3FileDownloader.countriesOfInterest
      .map(initialiseCountryTableInSchema(schemaName, _)).toList
      .sequence
  } yield x.sum

  def initialiseCountryTableInSchema(schemaName: String, countryCode: String): IO[Int] =
    for {
      rawDdl  <- resourceAsString("/create_raw_table_ddl.sql")
      ddl     <- IO {
                rawDdl
                  .replace("__schema__", schemaName)
                  .replace("__table__", countryCode)
              }
      res     <- Fragment.const(ddl).update.run.transact(transactor)
    } yield res

  def postIngestProcessing(schemaName: String, country: String): IO[Long] = for {
    _         <- printLine(s">>> Starting postIngestProcessing($schemaName)")
    mv        <- createMaterializedView(schemaName, country)
//    idx       <- createIndexesOnMaterializedView(schemaName, country)
//    v         <- createPublicView(schemaName, country)
  } yield mv //+idx+v

  def ingestFile(schemaName: String, table: String, filePath: String): IO[Long] =
    for {
      _         <- printLine(s">>> Ingest international file $filePath into table $table")
      inputData <- IO(Resource.make[IO, InputStream](IO(new FileInputStream(filePath)))(s => IO(s.close())))
      copySql   <- resourceAsString("/raw_copy.sql").map(
                    s => s.replaceAll("__schema__", schemaName)
                          .replaceAll("__table__", table))
      res       <- inputData.use[Long] {
                  in => PHC.pgGetCopyAPI(PFCM.copyIn(copySql, in)).transact(transactor)
                }
    } yield res

  def createMaterializedView(schemaName: String, table: String): IO[Long] =
    for {
      _       <- printLine(s">>> Creating materialized view $table")
      ddlSql  <- resourceAsString("/invoke_create_view_function_ddl.sql").map(
                s => s.replaceAll("__schema__", schemaName)
                      .replaceAll("__table__", table)
              )
      _       <- printLine(s"Executing '$ddlSql'")
      res     <- Fragment.const(ddlSql).update.run.transact(transactor)
    } yield res

  def createIndexesOnMaterializedView(schemaName: String, table: String): IO[Long] =
    for {
      _       <- printLine(s">>> Creating indexes on materialized view $table")
      ddlSql  <- resourceAsString("/create_view_indexes_ddl.sql").map(
                s => s.replaceAll("__schema__", schemaName)
                      .replaceAll("__table__", table)
              )
      res     <- Fragment.const(ddlSql).update.run.transact(transactor)
    } yield res

  def createMaterializedViewIndexes(schemaName: String, table: String): IO[Long] =
    for {
      _       <- printLine(s">>> Creating materialized view indexes on $table")
      ddlSql  <- resourceAsString("/create_view_indexes_ddl.sql").map(
                s => s.replaceAll("__schema__", schemaName)
                      .replaceAll("__table__", table)
              )
      res     <- Fragment.const(ddlSql).update.run.transact(transactor)
    } yield res

  def createPublicView(schemaName: String, table: String): IO[Long] =
    for {
      _       <- printLine(s">>> Creating public view $table")
      ddlSql  <- resourceAsString("/create_public_view_ddl.sql").map(
                s => s.replaceAll("__schema__", schemaName)
                      .replaceAll("__table__", table)
              )
      res     <- Fragment.const(ddlSql).update.run.transact(transactor)
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
