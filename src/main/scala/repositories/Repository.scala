package repositories

import cats.effect.{ContextShift, IO}
import doobie.Transactor
import me.lamouri.JCredStash

import java.util
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Assume that the database has already been created by the ABP ingest.
  * <pre>
  * 1. Get the current schema-name
  * 2. Import data
  *  a. Create table for country (2-char iso code)
  *  b. Import all data for that country to that table
  *  c. Update status table to indicate this ???
  *  </pre>
  */
object Repository {
  case class Repositories(forIngest: IngestRepository)

  def apply(): Repositories = repositories(Credentials())

  def forTesting(): Repositories =
    repositories(Credentials.forTesting())

  private def repositories(credentials: Credentials): Repositories = {
    val ingestorTransactor: Transactor[IO] = ingestorXa(credentials)

    Repositories(
      forIngest = new IngestRepository(ingestorTransactor, credentials)
    )
  }

  private def ingestorXa(creds: Credentials): Transactor[IO] = {
    implicit val cs: ContextShift[IO] =
      IO.contextShift(implicitly[ExecutionContext])

    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${creds.host}:${creds.port}/${creds.database}",
      creds.ingestor,
      creds.ingestorPassword
    )
  }

  sealed trait Credentials {
    def host: String

    def port: String

    def database: String

    def admin: String

    def adminPassword: String

    def ingestor: String

    def ingestorPassword: String

    def reader: String

    def readerPassword: String

    def csvBaseDir: String

    def nonukBucketName: String
  }

  object Credentials {
    def apply(): Credentials = {
      new RdsCredentials()
    }

    def forTesting(): Credentials = {
      new LocalCredentials()
    }
  }

  final class LocalCredentials() extends Credentials {
    override def host: String = "localhost"

    override def port: String = "5432"

    override def database: String = "addressbasepremium"

    override def admin: String = "postgres"

    override def adminPassword: String = "postgres"

    override def ingestor: String = admin

    override def ingestorPassword: String = adminPassword

    override def reader: String = admin

    override def readerPassword: String = adminPassword

    override def nonukBucketName: String = "cip-international-addresses"

    override def csvBaseDir: String =
      s"${sys.env("WORKSPACE")}/tmp"
  }

  final class RdsCredentials() extends Credentials {
    private val credstashTableName = "credential-store"
    private val context: util.Map[String, String] =
      Map("role" -> "address_lookup_file_download").asJava

    private def retrieveCredentials(credential: String) = {
      val credStash = new JCredStash()
      credStash.getSecret(credstashTableName, credential, context).trim
    }

    override def host: String = retrieveCredentials("address_lookup_rds_host")

    override def port: String = "5432"

    override def database: String =
      retrieveCredentials("address_lookup_rds_database")

    override def admin: String =
      retrieveCredentials("address_lookup_rds_admin_user")

    override def adminPassword: String =
      retrieveCredentials("address_lookup_rds_admin_password")

    override def ingestor: String =
      retrieveCredentials("address_lookup_rds_ingest_user")

    override def ingestorPassword: String =
      retrieveCredentials("address_lookup_rds_ingest_password")

    override def reader: String =
      retrieveCredentials("address_lookup_rds_readonly_user")

    override def readerPassword: String =
      retrieveCredentials("address_lookup_rds_readonly_password")

    override def nonukBucketName: String =
      retrieveCredentials("non_uk_address_lookup_bucket")

    override def csvBaseDir: String = "/mnt/efs/international-addresses"
  }
}
