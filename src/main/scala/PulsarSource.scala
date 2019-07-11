import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.types.Boundedness
import org.apache.flink.streaming.api.functions.source.{Source, SourceReader}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl
import org.apache.pulsar.client.impl.conf.ClientConfigurationData

class PulsarSource[T](topic: Topic,
                      pulsarUrl: String,
                      pulsarConfigs: ClientConfigurationData
                     ) extends Source[T, TopicPartition, PulsarSplitEnumeratorState] {
  val pulsarAdmin: PulsarAdmin = new PulsarAdminBuilderImpl()
    .serviceHttpUrl(pulsarUrl)
    .build()

  override def sourceName(): String = topic.toString

  override def supportsBoundedness(boundedness: Boundedness): Boolean = boundedness == Boundedness.CONTINUOUS_UNBOUNDED

  def createReader(sourceContext: SourceContext[T]): SourceReader[TopicPartition, T] = ???

  override def createEnumerator(boundedness: Boundedness): PulsarSplitEnumerator = ???

  override def restoreEnumerator(boundedness: Boundedness, enumChkT: PulsarSplitEnumeratorState): PulsarSplitEnumerator = ???

  override def getSplitSerializer: SimpleVersionedSerializer[PulsarSplitEnumerator] = ???

  override def getEnumeratorCheckpointSerializer: SimpleVersionedSerializer[PulsarSplitEnumeratorState] = ???
}
