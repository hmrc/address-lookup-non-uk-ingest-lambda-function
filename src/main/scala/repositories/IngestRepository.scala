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
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.io.Source

class IngestRepository(transactor: => Transactor[IO],
                       private val credentials: Credentials) {

  def ingest(schemaName: String, country: String, fileToIngest: String): IO[Long] =
    for {
      updateCount <- ingestFile(schemaName, country, fileToIngest)
    } yield updateCount

  private def createStatusRow(schemaName: String, country: String): String =
    s"""INSERT INTO public.nonuk_address_lookup_status(host_schema, status, timestamp)
       | VALUES(format('%I-%I', '${schemaName}', '${country}'), 'initialized', now());""".stripMargin

  private val timestampFormat = DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss")

  private def createSchemaName(): IO[String] = IO {
    val timestamp = LocalDateTime.now(ZoneId.of("UTC"))
    s"nonuk_${timestampFormat.format(timestamp)}"
  }

  def createSchema(): IO[String] = for {
    statusDdl   <- resourceAsString("/create_status_table_ddl.sql")
    _           <- csql(statusDdl).update.run.transact(transactor)
    sTD         <- schemasToDrop()
    _           <- dropSchemas(sTD)
    _           <- cleanOldFunctions()
    schemaName  <- createSchemaName()
    _           <- csql(s"CREATE SCHEMA $schemaName;").update.run.transact(transactor)
    procDdl     <- resourceAsString("/create_view_function_ddl.sql")
    _           <- csql(procDdl).update.run.transact(transactor)
    statusDdl   <- IO {
                        S3FileDownloader.countriesOfInterest.map { c =>
                          createStatusRow(schemaName, c)
                        }
                      }
    _           <- statusDdl.map(sddl => csql(sddl).update.run.transact(transactor)).sequence
    _           <- initialiseCountryTablesInSchema(schemaName)
  } yield schemaName

  private def schemasToDrop(): IO[List[String]] =
    for {
      rs      <- resourceAsString("/schemas_to_drop.sql")
      result  <- csql(rs).query[String].to[List].transact(transactor)
    } yield result

  private def dropSchemas(schemas: List[String]): IO[Int] = {
    val sqlStringRes = resourceAsString("/drop_schema.sql")

    schemas.map { schema =>
      for {
        sqlStr  <- sqlStringRes
        sql     <- csql(sqlStr.replaceAll("__schema__", schema)).update.run.transact(transactor)
      } yield sql
    }.fold(IO(0)) {
      case (ioa, iob) => for {
        a <- ioa
        b <- iob
      } yield a + b
    }
  }

  private def cleanOldFunctions(): IO[List[Int]] = for {
    sqlStr            <- resourceAsString("/functions_to_drop.sql")
    functionsToDrop   <- csql(sqlStr).query[String].to[List].transact(transactor)
    droppedFunctions  <- functionsToDrop.map(f => csql(s"DROP FUNCTION IF EXISTS $f").update.run.transact(transactor)).sequence
  } yield droppedFunctions

  def initialiseCountryTablesInSchema(schemaName: String): IO[Int] = for {
    x <- S3FileDownloader.countriesOfInterest
          .map(initialiseCountryTablesInSchema(schemaName, _))
          .sequence
  } yield x.sum

  def initialiseCountryTablesInSchema(schemaName: String, countryCode: String): IO[Int] =
    for {
      rawDdl          <- resourceAsString("/create_raw_table_ddl.sql")
      ddl             <- IO {
                          rawDdl
                            .replace("__schema__", schemaName)
                            .replace("__table__", countryCode)
                        }
      expandedRawDdl  <- resourceAsString("/create_expanded_raw_table_ddl.sql")
      expandedDdl     <- IO {
                  expandedRawDdl
                    .replace("__schema__", schemaName)
                    .replace("__table__", countryCode)
                }
      res             <- Fragment.const(ddl).update.run.transact(transactor)
      expandedRes     <- Fragment.const(expandedDdl).update.run.transact(transactor)
    } yield expandedRes

  def postIngestProcessing(schema: String, countries: List[Map[String, String]]): IO[Long] = for {
    mv  <- createMaterializedView(schema, countries)
  } yield mv

  def ingestFile(schemaName: String, table: String, filePath: String): IO[Long] = for {
      inputData <- IO(Resource.make[IO, InputStream](IO(new FileInputStream(filePath)))(s => IO(s.close())))
      copySql   <- resourceAsString("/raw_copy.sql").map(
                    s => s.replaceAll("__schema__", schemaName)
                      .replaceAll("__table__", table))
      res       <- inputData.use[Long] {
                      in => PHC.pgGetCopyAPI(PFCM.copyIn(copySql, in)).transact(transactor)
                    }
    } yield res

  def expandJsonToFields(schemaName: String, table: String): IO[Long] = for {
      populateSql   <- resourceAsString("/populate_expanded_raw_table_ddl.sql")
                        .map(s => s.replaceAll("__schema__", schemaName).replaceAll("__table__", table))
      res           <- csql(populateSql).update.run.transact(transactor)
    } yield res

  def createMaterializedView(schema: String, countries: List[Map[String, String]]): IO[Long] = for {
      statements  <- IO {
                      countries.map { m =>
                        s"""SELECT public.create_non_uk_address_lookup_materialized_view('${schema}', '${m("country")}');"""
                      }.mkString("\n")
                    }
      ddlSql      <- resourceAsString("/invoke_create_view_function_ddl.sql").map(
                        s => s.replaceAll("__replacement_statements__", statements)
                      )
      res         <- csql(ddlSql).update.run.transact(transactor)
    } yield res

  def checkStatus(schemaName: String): IO[List[(String, String, Option[String])]] = for {
    statusSql <- IO(
                  s"""SELECT host_schema, status, error_message
                     | FROM public.nonuk_address_lookup_status
                     | WHERE host_schema LIKE '${schemaName}-%'""".stripMargin)
    res       <- csql(statusSql).query[(String, String, Option[String])].to[List].transact(transactor)
  } yield res

  def finaliseSchema(schemaName: String): IO[Boolean] = for {
    res <- csql(
            s"""UPDATE public.nonuk_address_lookup_status
               | SET status = 'finalised'
               | WHERE host_schema like '${schemaName}-%';
               | """.stripMargin).update.run.transact(transactor)
  } yield res > 0

  private def resourceAsString(name: String): IO[String] = {
    Resource
      .make[IO, Source](
        IO(Source.fromURL(getClass.getResource(name), "utf-8"))
      )(s => IO(s.close()))
      .use(s => IO(s.mkString))
  }
}
