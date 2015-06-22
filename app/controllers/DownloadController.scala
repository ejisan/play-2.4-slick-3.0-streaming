package controllers

import com.google.common.io.BaseEncoding
import db._
import play.contrib.{ BlobRow, BlobRowSubscriber }
import java.sql.Blob
import javax.inject.Inject
import org.reactivestreams.Subscriber
import play.api._
import play.api.mvc._
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.Files.TemporaryFile
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.mvc.MultipartFormData.FilePart
import scala.concurrent.{ ExecutionContext, Future }
import slick.backend.{ DatabaseConfig, DatabasePublisher }
import slick.driver.H2Driver

class DownloadController @Inject() (
  dbConfigProvider: DatabaseConfigProvider)(implicit
  executionContext: ExecutionContext) extends Controller {

  val dbConfig = dbConfigProvider.get[H2Driver]

  case class Upload(
    name: String,
    length: Long,
    data: Enumerator[Array[Byte]]
  )

  /** Retrieve an upload from the database */
  private def getUpload(id: Int): Future[Option[Upload]] = {
    import H2Driver.api._

    // Run a streaming query
    // http://slick.typesafe.com/doc/3.0.0/dbio.html#streaming
    val queryPublisher: DatabasePublisher[(Blob, String)] = dbConfig.db.stream {
      Tables.uploads.filter(_.id === id).map(r => (r.data, r.name)).result
    }

    // Read from the streaming query using a custom Subscriber
    // https://github.com/reactive-streams/reactive-streams-jvm/#2-subscriber-code
    val blobRowSubscriber = new BlobRowSubscriber[String] // May need extra threads in the thread pool
    queryPublisher.subscribe(blobRowSubscriber)

    // Get the result from Subscriber - ether None, Some or an error
    val blobRowFuture: Future[Option[BlobRow[String]]] = blobRowSubscriber.blobRow

    // Map the result into a nicer type
    blobRowFuture.map { blobRowOption: Option[BlobRow[String]] =>
      blobRowOption.map {
        case BlobRow(length, data, extra) =>
          Upload(name = extra, length = length, data = data)
      }
    }
  }

  /**
   * Action to handle download.
   *
   * Test by running `curl`:
   *
   * curl -v http://localhost:9000/download/1
   */
  def download(id: Int) = Action.async { request =>
    getUpload(id).map {
      case None =>
        // If the row wasn't in the database then return a 404.
        NotFound
      case Some(Upload(name, length, data)) =>
        // If the row is in the database then return it as a streaming
        // result.
        Result(
          header = ResponseHeader(OK, Map(
            CONTENT_DISPOSITION -> s"attachment; name=$name",
            CONTENT_LENGTH -> length.toString
            // May want to add other headers, e.g. Content-Type.
          )),
          body = data
        )
    }
  }

}
