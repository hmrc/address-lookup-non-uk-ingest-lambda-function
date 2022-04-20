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

  private def createStatusRow(schemaName: String, country: String): String =
    s"""INSERT INTO public.nonuk_address_lookup_status(host_schema, status, timestamp)
                  | VALUES(format('%I-%I', '${schemaName}', '${country}'), 'initialized', now());""".stripMargin

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
                      }
                   }
    _           <- procDdl.map(pddl => csql(pddl).update.run.transact(transactor)).sequence
    statusDdl     <- IO {
                      S3FileDownloader.countriesOfInterest.map{c =>
                        createStatusRow(schemaName, c)
                      }
                   }
    _           <- statusDdl.map(sddl => csql(sddl).update.run.transact(transactor)).sequence
    _           <- initialiseCountryTablesInSchema(schemaName)
  } yield schemaName

  def initialiseCountryTablesInSchema(schemaName: String): IO[Int] = for {
    x <- S3FileDownloader.countriesOfInterest
      .map(initialiseCountryTableInSchema(schemaName, _))
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

  def postIngestProcessing(countries: List[Map[String, String]]): IO[Long] = for {
    _         <- printLine(s">>> Starting postIngestProcessing($countries)")
    mv        <- createMaterializedView(countries)
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

  def createMaterializedView(countries: List[Map[String, String]]): IO[Long] =
    for {
      _           <- printLine(s">>> Creating materialized views")
      statements  <- IO{
                    countries.map{ m =>
                      s"""SELECT public.create_nonuk_materialized_view_for_${m("country")}();"""
                    }.mkString("\n")
                  }
      ddlSql      <- resourceAsString("/invoke_create_view_function_ddl.sql").map(
                    s => s.replaceAll("__replacement_statements__", statements)
                  )
      _           <- printLine(s"createMaterializedView sql: '$ddlSql'")
      res         <- csql(ddlSql).update.run.transact(transactor)
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
