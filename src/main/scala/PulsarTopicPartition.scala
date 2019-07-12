import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, Decoder, DefaultNamingStrategy, Encoder, NamingStrategy, SchemaFor}
import org.apache.avro.Schema
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.source.types.SourceSplit
import org.apache.pulsar.client.api.MessageId

case class Topic(tenant: String, namespace: String, name: String) {
  override def toString: String = s"persistent://$tenant/$namespace/$name"
}

case class TopicPartition(topic: Topic, partitionId: Long, var cursor: MessageId = MessageId.earliest) extends SourceSplit {
  override def equals(o: Any): Boolean = toString == o.toString

  override def hashCode(): Int = toString.hashCode

  override def toString: String = s"$topic-partition-$partitionId"

  override def splitId: String = toString

  // pulsar topics shouldn't ever end in theory!
  override def isFinished: Boolean = false
}

class TopicPartitionSerializer extends SimpleVersionedSerializer[TopicPartition] {
  implicit object PulsarMessageIdSchemaFor extends SchemaFor[MessageId] {
    override def schema: Schema = Schema.create(Schema.Type.BYTES)
  }

  implicit object PulsarMessageIdEncoder extends Encoder[MessageId] {
    override def encode(t: MessageId, schema: Schema): AnyRef = t.toByteArray
  }

  implicit object PulsarMessageIdDecoder extends Decoder[MessageId] {
    override def decode(value: Any, schema: Schema): MessageId = MessageId.fromByteArray(value.asInstanceOf[Array[Byte]])
  }

  override def getVersion: Int = 0

  override def serialize(obj: TopicPartition): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val writeStream = AvroOutputStream
      .data[TopicPartition]
      .to(outputStream)
      .build(AvroSchema[TopicPartition])

    writeStream.write(obj)
    writeStream.flush()
    writeStream.close()

    outputStream.toByteArray
  }

  override def deserialize(version: Int, serialized: Array[Byte]): TopicPartition = {
    val inputStream = new ByteArrayInputStream(serialized)
    val readStream = AvroInputStream
      .data[TopicPartition]
      .from(inputStream)
      .build(AvroSchema[TopicPartition])

    readStream.iterator.toList.head
  }
}

