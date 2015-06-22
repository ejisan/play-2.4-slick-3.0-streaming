// Need to put this in the 'play' package to access a couple of
// play-private classes. Since these classes are obviously useful
// we will give them higher visibility.
package play.contrib

import java.sql.Blob
import org.reactivestreams._
import play.api.libs.concurrent.StateMachine // This class is private[play]
import play.api.libs.iteratee.Enumerator
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

/** Different states used by the BlobRowSubscriber class */
private[contrib] object BlobRowSubscriber {
  sealed trait State
  case object NotSubscribed extends State
  case class SubscribedAndRequested(subs: Subscription) extends State
  case class ReadingFromBlob(subs: Subscription) extends State
  case object ReadFinished extends State
  case object Completed extends State
}

import BlobRowSubscriber._

/**
 * Holds the results of reading a row from the database. A Blob
 * is converted into an Enumerator and a length.
 *
 * This object is written to be generic. The Blob part of the row
 * is stored in the `length` and `data` fields. All the rest of
 * the row can be stored in the `extra` field. If more than one
 * column needs to be stored, then the `extra` field can hold
 * a tuple.
 */
case class BlobRow[A](length: Long, data: Enumerator[Array[Byte]], extra: A)

/**
 * A Subscriber that reads a single row containing a Blob from Slick's
 * streaming queries.
 *
 * https://github.com/reactive-streams/reactive-streams-jvm/#2-subscriber-code
 * http://slick.typesafe.com/doc/3.0.0/dbio.html#streaming
 */
class BlobRowSubscriber[A](implicit ec: ExecutionContext)
    extends StateMachine[State](initialState = NotSubscribed) with Subscriber[(Blob, A)] {

  private val blobRowPromise = Promise[Option[BlobRow[A]]]()
  def blobRow: Future[Option[BlobRow[A]]] = blobRowPromise.future

  // Streams methods

  override def onSubscribe(subs: Subscription): Unit = exclusive {
    // Our subscription has been accepted, so request 1 row from
    // the database.
    case NotSubscribed =>
      subs.request(1)
      state = SubscribedAndRequested(subs)
    case _ =>
      throw new IllegalStateException(s"Can't subscribe: $state")
  }

  override def onComplete(): Unit = exclusive {
    // There were no rows. This method may be called before our
    // subscription was accepted (state=NotSubscribed) or after
    // we requested a row from the database (state=SubscribedAndRequested)
    case NotSubscribed | SubscribedAndRequested(_) =>
      blobRowPromise.success(None)
      state = Completed
    case _ =>
      throw new IllegalStateException(s"Can't onComplete: $state")
  }

  override def onError(cause: Throwable): Unit = exclusive {
    // There was an error. This method may be called before our
    // subscription was accepted (state=NotSubscribed) or after
    // we requested a row from the database (state=SubscribedAndRequested)
    case NotSubscribed | SubscribedAndRequested(_) =>
      blobRowPromise.failure(cause)
    case _ =>
      throw new IllegalStateException(s"Can't handle onError: $state")
  }

  override def onNext(element: (Blob, A)): Unit = exclusive {
    // The row has been retrieved. This can only be called after we
    // requested a row from the database.
    case SubscribedAndRequested(subs) =>
      try {
        val (blob, extra): (Blob, A) = element
        val blobRow = BlobRow(
          length = blob.length,
          data = Enumerator.fromStream(blob.getBinaryStream()).onDoneEnumerating {
            // Set a callback so that we cancel the subscription after the
            // Enumerator has finished. We can cancel the subscription because
            // we will have finished reading all the data from the row.
            onReadFinished()
          },
          extra = extra
        )
        // Pass the Enumerator and the extra data into the promise.
        blobRowPromise.success(Some(blobRow))
        state = ReadingFromBlob(subs)
      } catch {
        case NonFatal(e) => 
          // An error occurred reading calling getBinaryStream(). We'd
          // better cancel the subscription and return an error.
          blobRowPromise.failure(e)
          subs.cancel()
          state = ReadFinished
      }
    case _ =>
      throw new IllegalStateException(s"Can't handle onNext: $state")
  }

  private def onReadFinished(): Unit = exclusive {
    // We finished reading the blob data from the row. Cancel the
    // subscription.
    case ReadingFromBlob(subs) =>
      subs.cancel()
      state = ReadFinished
    case _ =>
      throw new IllegalStateException(s"Can't handle onNext: $state")
  }

}