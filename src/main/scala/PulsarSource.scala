import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.types.Boundedness
import org.apache.flink.streaming.api.functions.source.{SingleSplitSequentialSourceReader, Source, SourceReader}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.impl.conf.ClientConfigurationData

class PulsarSource[T](topic: Topic,
                      pulsarUrl: String,
                      pulsarConfigs: ClientConfigurationData
                     ) extends Source[Message[T], TopicPartition, PulsarSplitEnumeratorState] {
  val pulsarAdmin: PulsarAdmin = new PulsarAdminBuilderImpl()
    .serviceHttpUrl(pulsarUrl)
    .build()

  override def sourceName(): String = topic.toString

  override def supportsBoundedness(boundedness: Boundedness): Boolean = boundedness == Boundedness.CONTINUOUS_UNBOUNDED

  override def createReader(sourceContext: SourceContext[Message[T]]): SourceReader[TopicPartition, Message[T]] = {
    new SingleSplitSequentialSourceReader[TopicPartition, Message[T]](new PulsarTopicPartitionReaderFactory[T](pulsarUrl))
  }

  override def createEnumerator(boundedness: Boundedness): PulsarSplitEnumerator = {
    new PulsarSplitEnumerator(topic, pulsarAdmin)
  }

  override def restoreEnumerator(boundedness: Boundedness, enumChkT: PulsarSplitEnumeratorState): PulsarSplitEnumerator = {
    new PulsarSplitEnumerator(topic, pulsarAdmin, enumChkT.topicPartitions, enumChkT.unassignedTopicPartitions)
  }

  override def getSplitSerializer: SimpleVersionedSerializer[TopicPartition] = ???

  override def getEnumeratorCheckpointSerializer: SimpleVersionedSerializer[PulsarSplitEnumeratorState] = ???
}
