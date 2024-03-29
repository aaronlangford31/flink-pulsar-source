import java.util
import java.util.Optional

import org.apache.flink.streaming.api.functions.source.PeriodicSplitEnumerator
import org.apache.flink.streaming.api.functions.source.types.ReaderLocation
import org.apache.pulsar.client.admin.PulsarAdmin

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

case class PulsarSplitEnumeratorState(topicPartitions: Set[TopicPartition], unassignedTopicPartitions: Set[TopicPartition])

class PulsarSplitEnumerator(topic: Topic,
                            pulsarAdmin: PulsarAdmin,
                            var topicPartitions: Set[TopicPartition] = Set.empty,
                            var unassignedTopicPartitions: Set[TopicPartition] = Set.empty
                           ) extends PeriodicSplitEnumerator[TopicPartition, PulsarSplitEnumeratorState] {

  override def nextSplit(readerLocation: ReaderLocation): Optional[TopicPartition] = {
    val next = unassignedTopicPartitions.headOption
    unassignedTopicPartitions = unassignedTopicPartitions.tail

    next.asJava
  }

  override def addSplitsBack(list: util.List[TopicPartition]): Unit = {
    unassignedTopicPartitions = unassignedTopicPartitions ++ list.asScala
  }

  override def snapshotState(): PulsarSplitEnumeratorState = PulsarSplitEnumeratorState(topicPartitions, unassignedTopicPartitions)

  override def close(): Unit = {}

  override def discoverMoreSplits(): Boolean = {
    val partitionRgx = """persistent:\/\/(.+)\/(.+)/(.+)-partition-(\d+)""".r

    val t = pulsarAdmin
      .topics()
      .getList(topic.namespace)
        .asScala
        .map({
          case partitionRgx(tenant, namespace, topicName, partition) => Some(TopicPartition(Topic(tenant, namespace, topicName), partition.toLong))
          case _ => None
        })
      .filter({
        // discover topic partitions which have not already been found
        case Some(topicPartition) => topicPartition.topic == topic
        case None => false
      })
      .map(_.get)
      .toSet[TopicPartition]

    topicPartitions = topicPartitions ++ t
    unassignedTopicPartitions = unassignedTopicPartitions ++ t

    t.nonEmpty
  }
}