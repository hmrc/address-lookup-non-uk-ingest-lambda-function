package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import lambdas.InternationalAddressesEnsureSchemaLambdaFunction.updateJavaInput
import repositories.{IngestRepository, Repository}
import util.S3FileDownloader

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import java.util.{List => jList, Map => jMap}
import scala.collection.JavaConverters._

class InternationalAddressesEnsureSchemaLambdaFunction
  extends RequestHandler[jMap[String, Object], jMap[String, Object]] {

  override def handleRequest(input: jMap[String, Object], contextNotUsed: Context): jMap[String, Object] = {
    val schemaName = Await.result(doEnsureSchema(Repository().forIngest), 1.minute)
    updateJavaInput(input, schemaName)
  }

  def doEnsureSchema(repository: IngestRepository): Future[String] = {
    repository.createSchema().unsafeToFuture()
  }
}

object InternationalAddressesEnsureSchemaLambdaFunction {
  def updateJavaInput(input: jMap[String, Object], schemaName: String): jMap[String, Object] = {
    val sInput = input.asScala
    val filesToIngest = sInput("filesToIngest")
    val sFileList = filesToIngest.asInstanceOf[jList[jMap[String, String]]].asScala
    sFileList.map { m => m.asInstanceOf[jMap[String, String]].put("schemaName", schemaName) }
    sInput.put("filesToIngest", sFileList.asJava)
    sInput.put("schemaName", schemaName)
    sInput.put("countries", S3FileDownloader.countriesOfInterest.map { c =>
      Map("country" -> c, "schemaName" -> schemaName).asJava
    }.asJava)
    sInput.asJava
  }
}
