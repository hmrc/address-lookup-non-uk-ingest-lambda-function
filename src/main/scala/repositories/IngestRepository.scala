package repositories

import cats.effect.{IO, Resource}
import doobie.implicits._
import doobie.util.fragment.Fragment.{const => csql}
import doobie._
import model.{GeoJson, SqlProperties}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import repositories.Repository.Credentials

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.concurrent.Future
import scala.io.{BufferedSource, Source}

class IngestRepository(transactor: => Transactor[IO],
                       private val credentials: Credentials) {
  private val logger = LoggerFactory.getLogger(classOf[IngestRepository])

  private val rootDir: String = {
    if (new File(credentials.csvBaseDir).isAbsolute) credentials.csvBaseDir
    else os.pwd.toString + "/" + credentials.csvBaseDir
  }

  def ingest(): IO[Int] = {
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
      res <- sql"SELECT schema_name FROM public.address_lookup_status WHERE status = 'finalised'"
        .query[String]
        .unique
        .transact(transactor)
    } yield res

  def initialiseCountryTableInSchema(schemaName: String,
                                     countryCode: String): IO[Int] =
    for {
      rawDdl <- resourceAsString("/create_table_ddl.sql")
      ddl <- IO {
        rawDdl
          .replace("__schema__", schemaName)
          .replace("__table__", countryCode)
      }
      res <- Fragment.const(ddl).update.run.transact(transactor)
    } yield res

  def ingestFiles(schemaName: String, countryDataDir: String): IO[Int] = {
    IO {
      println(s""">>> ingestFiles($schemaName, $countryDataDir)""")

      val filesOfInterest = os
        .walk(path = os.Path(countryDataDir))
        .filter { p =>
          p.toIO.getName.endsWith("geojson")
        }
        .filter(_.toString().contains("/bm/"))
        .groupBy { p =>
          val countriesDir = p.toString().replace(countryDataDir, "")
          val seg = os.Path(countriesDir).getSegment(0)
          seg
        }

      println(s">>> filesOfInterest: ${filesOfInterest}")

      filesOfInterest
    }.flatMap { coToFilesMap =>
      coToFilesMap
        .map {
          case (countryCode, files) =>
            println(s">>> countryCode: ${countryCode}, files: ${files.length}")
            initialiseCountryTableInSchema(schemaName, countryCode)
              .flatMap { _ =>
                files
                  .map { f =>
                    ingestFile(s"$schemaName.$countryCode", s"$f")
                  }
                  .toSeq
                  .fold(IO(0)) {
                    case (aio, bio) =>
                      for { a <- aio; b <- bio } yield a + b
                  }
              }
        }
        .toSeq
        .fold(IO(0)) {
          case (aio, bio) => for { a <- aio; b <- bio } yield a + b
        }
    }
  }

  def ingestFile(table: String, filePath: String): IO[Int] = {
    import model.GeoJson._

    println(s">>> Ingest international file $filePath into table $table")

    Resource
      .make[IO, BufferedSource](IO(Source.fromFile(filePath, "utf-8")))(
        s => IO(s.close())
      )
      .use[Int] { in =>
        in.getLines()
          .map(l => Json.parse(l))
          .flatMap(gj => Json.fromJson[GeoJson](gj).asOpt)
          .map { gj =>
            val p = SqlProperties(gj.properties)
            Fragment
              .const(s"""INSERT INTO $table (id, hash, number, street, unit, city, district, region, postcode) 
                     |VALUES (${p.id},
                     |${p.hash},
                     |${p.number}, 
                     |${p.street}, 
                     |${p.unit}, 
                     |${p.city}, 
                     |${p.district}, 
                     |${p.region}, 
                     |${p.postcode})""".stripMargin)
              .update
              .run
              .transact(transactor)
          }
          .fold(IO(0)) {
            case (aio, bio) =>
              for {
                a <- aio
                b <- bio
              } yield a + b
          }
      }
  }

  private def cleanupOldEpochDirectories(proceed: Boolean,
                                         epoch: String): Unit = {
    if (proceed) {
      os.walk(
          path = os.Path(rootDir),
          skip = p => p.baseName == epoch,
          maxDepth = 1
        )
        .filter(_.toIO.isDirectory)
        .foreach(os.remove.all)
    }
  }

  private def resourceAsString(name: String): IO[String] = {
    Resource
      .make[IO, Source](
        IO(Source.fromURL(getClass.getResource(name), "utf-8"))
      )(s => IO(s.close()))
      .use(s => IO(s.mkString))
  }

  private def printLine(s: String): IO[Unit] = IO(println(s))
}
