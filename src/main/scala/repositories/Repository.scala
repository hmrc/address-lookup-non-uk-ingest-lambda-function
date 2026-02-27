package repositories

import cats.effect.IO
import com.amazonaws.secretsmanager.caching.SecretCache
import doobie.Transactor
import me.lamouri.JCredStash
import services.SecretsManagerService
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient

import java.util
import java.util.Properties
import scala.collection.*
import scala.collection.JavaConverters.mapAsJavaMapConverter

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
    val properties = new Properties()
    properties.setProperty("user", creds.ingestor)
    properties.setProperty("password", creds.ingestorPassword)

    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${creds.host}:${creds.port}/${creds.database}",
      properties,
      None
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
    private val awsClientBuilder: SecretsManagerClient = SecretsManagerClient
      .builder()
      .region(Region.EU_WEST_2)
      .build()
    private val secretsManagerService = new SecretsManagerService(new SecretCache(awsClientBuilder))

    private def retrieveCredentials(
                                     credential: String
                                   ) = {
      secretsManagerService.getSecret("rds/cip-address-search-api-rds-cluster/root", credential)
    }

    override def host: String = "address_search_rds_host"

    override def port: String = "5432"

    override def database: String = "addressbasepremium"

    override def ingestor: String = retrieveCredentials("username")

    override def ingestorPassword: String = retrieveCredentials("password")

    override def nonukBucketName: String =
      secretsManagerService.getSecret("attrep-secret/address_lookup_file_download/non_uk_address_lookup_bucket", "secret")

    override def nonUkBaseDir: String = "/mnt/efs/international-addresses/"
  }
}
