import java.time.Duration
import java.util.concurrent.{CompletableFuture, TimeUnit}

import org.apache.flink.streaming.api.functions.source.{AsyncSplitReader, SplitReader, SplitReaderFactory}
import org.apache.pulsar.client.api.{Message, PulsarClient, Reader}
import org.apache.pulsar.client.impl.ClientBuilderImpl
import org.apache.pulsar.client.impl.schema.JSONSchema



class PulsarTopicPartitionReader[T](pulsarServiceUrl: String, topicPartition: TopicPartition) extends SplitReader[Message[T]] {
  val client: PulsarClient = new ClientBuilderImpl()
    .serviceUrl(pulsarServiceUrl)
    .build()

  val reader: Reader[T] = client.newReader(JSONSchema.of[T](classOf[T]))
        .topic(topicPartition.toString)
        .startMessageId(topicPartition.cursor)
        .create()

  override def fetchNextRecord(duration: Duration): Message[T] = {
    val message = reader.readNext(Int(duration.toMillis), TimeUnit.MILLISECONDS)
    topicPartition.cursor = message.getMessageId
    message
  }

  override def wakeup(): Unit = { /* http://www.quickmeme.com/meme/3oybjz */ }

  override def close(): Unit = {
    reader.close()
    client.close()
  }
}


class AsyncPulsarTopicPartitionReader[T](pulsarServiceUrl: String, topicPartition: TopicPartition) extends AsyncSplitReader[Message[T]] {
  val client: PulsarClient = new ClientBuilderImpl()
    .serviceUrl(pulsarServiceUrl)
    .build()

  val reader: Reader[T] = client.newReader(JSONSchema.of[T](classOf[T]))
    .topic(topicPartition.toString)
    .startMessageId(topicPartition.cursor)
    .create()

  override def fetchNextRecord(): CompletableFuture[Message[T]] = {
    reader.readNextAsync().thenApply(message => {
      topicPartition.cursor = message.getMessageId
      message
    })
  }

  override def close(): Unit = {
    reader.close
    client.close
  }
}

class PulsarTopicPartitionReaderFactory[T](pulsarServiceUrl: String) extends SplitReaderFactory[TopicPartition, Message[T]] {
  override def create(splitT: TopicPartition): SplitReader[Message[T]] = new PulsarTopicPartitionReader[T](pulsarServiceUrl, splitT)
}