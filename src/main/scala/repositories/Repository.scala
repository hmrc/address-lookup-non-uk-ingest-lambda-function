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

    def ingestor: String

    def ingestorPassword: String

    def nonUkBaseDir: String

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

    override def ingestor: String = "postgres"

    override def ingestorPassword: String = "postgres"

    override def nonukBucketName: String = "cip-international-addresses"

    override def nonUkBaseDir: String =
      s"${sys.env("WORKSPACE")}/tmp/international_addresses"
  }

  final class RdsCredentials() extends Credentials {
    private val credStashPrefix = sys.env.getOrElse("CREDSTASH_PREFIX", "")

    private val credstashTableName = "credential-store"
    private val context: util.Map[String, String] =
      Map("role" -> "cip_address_search").asJava

    private val lookupContext: util.Map[String, String] =
      Map("role" -> "address_lookup_file_download").asJava

    private def retrieveCredentials(
      credential: String,
      context: util.Map[String, String] = context
    ) = {
      val credStash = new JCredStash()
      credStash.getSecret(credstashTableName, credential, context).trim
    }

    override def host: String =
      retrieveCredentials(s"address_search_rds_rw_proxy")

    override def port: String = "5432"

    override def database: String =
      retrieveCredentials(s"address_search_rds_database")

    override def ingestor: String =
      retrieveCredentials(s"address_search_rds_admin_user")

    override def ingestorPassword: String =
      retrieveCredentials(s"address_search_rds_admin_password")

    override def nonukBucketName: String =
      retrieveCredentials(
        s"${credStashPrefix}non_uk_address_lookup_bucket",
        lookupContext
      )

    override def nonUkBaseDir: String = "/mnt/efs/international-addresses/"
  }
}
