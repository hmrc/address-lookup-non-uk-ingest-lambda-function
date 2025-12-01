package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import lambdas.InternationalAddressesEnsureSchemaLambdaFunction.updateJavaInput
import repositories.{IngestRepository, Repository}
import util.S3FileDownloader
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import java.util.{List => jList, Map => jMap}
import scala.collection.JavaConverters._

class InternationalAddressesEnsureSchemaLambdaFunction
  extends RequestHandler[jMap[String, Object], jMap[String, Object]] {

  override def handleRequest(input: jMap[String, Object], contextNotUsed: Context): jMap[String, Object] = {
    val schemaName = Await.result(doEnsureSchema(Repository().forIngest), 5.minute)
    updateJavaInput(input, schemaName)
  }

  def doEnsureSchema(repository: IngestRepository): Future[String] = {
    repository.createSchema().unsafeToFuture()
  }
}

object InternationalAddressesEnsureSchemaLambdaFunction {
  def updateJavaInput(input: jMap[String, Object], schemaName: String): jMap[String, Object] = {
    input.put("schemaName", schemaName)
    val filesToIngest = input.get("filesToIngest").asInstanceOf[jList[jMap[String, String]]]
    filesToIngest.forEach(m => m.put("schemaName", schemaName))
    input.put("countries", S3FileDownloader.countriesOfInterest.map { c =>
      Map("country" -> c, "schemaName" -> schemaName).asJava
    }.asJava)
    input
  }
}
