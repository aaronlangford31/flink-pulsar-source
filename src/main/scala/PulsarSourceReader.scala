import java.util
import java.util.concurrent.CompletableFuture

import org.apache.flink.streaming.api.functions.source.types.ReaderStatus
import org.apache.flink.streaming.api.functions.source.{SourceOutput, SourceReader, SplitContext}
import org.apache.pulsar.client.api.{Message, PulsarClient, Reader}
import org.apache.pulsar.client.impl.ClientBuilderImpl
import org.apache.pulsar.client.impl.schema.JSONSchema

import scala.collection.JavaConverters._

class PulsarSourceReader[T](pulsarServiceUrl: String, var splits: List[TopicPartition]) extends SourceReader[TopicPartition, Message[T]] {
  val client: PulsarClient = new ClientBuilderImpl()
    .serviceUrl(pulsarServiceUrl)
    .build()

  var readers: List[Reader[T]] = splits
    .map(tp => {
      client.newReader(JSONSchema.of[T](classOf[T]))
        .topic(tp.toString)
        .startMessageId(tp.cursor)
        .create()
    })

  var buffer: Option[Message[T]] = Next

  override def start(splitContext: SplitContext): Unit = {
    splitContext.requestNewSplit()
  }

  // this guy is called to go get some more events when NOT_AVAILABLE is the ReaderStatus,
  // when the future is completed, then emitNext will be called
  override def available(): CompletableFuture[_] =

  override def emitNext(sourceOutput: SourceOutput[Message[T]]): ReaderStatus = {
    if (buffer.isEmpty) {
      ReaderStatus.NOTHING_AVAILABLE
    } else {
      val next :: theRest = buffer
      next.getMessageId
      sourceOutput.emitRecord(next)
    }
  }

  override def addSplits(list: util.List[TopicPartition]): Unit = {
    splits = splits ++ list.asScala
  }

  override def snapshotState(): util.List[TopicPartition] = splits.asJava
}
