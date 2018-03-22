package ballerina.net.kafka;

@Description { value:"Struct which represents Topic partition"}
@Field { value:"topic: Topic which partition is related" }
@Field { value:"partition: Index for the partition" }
public struct TopicPartition {
  string topic;
  int partition;
}

@Description { value:"Struct which represents Consumer Record which returned from pol cycle"}
@Field { value:"key: Record key byte array" }
@Field { value:"value: Record value byte array" }
@Field { value:"offset: Offset of the Record positioned in partition" }
@Field { value:"partition: Topic partition record to be sent" }
@Field { value:"timestamp: Timestamp to be considered over broker side" }
@Field { value:"topic: Topic record to be sent" }
public struct ConsumerRecord {
   blob key;
   blob value;
   int offset;
   int partition;
   int timestamp;
   string topic;
}

@Description { value:"Struct which represents Topic partition position in which consumed record is stored"}
@Field { value:"partition: TopicPartition which record is related" }
@Field { value:"offset: offset in which record is stored in partition" }
public struct Offset {
  TopicPartition partition;
  int offset;
}

@Description { value:"Struct which represents Kafka producer record"}
@Field { value:"key: Record key byte array" }
@Field { value:"value: Record value byte array" }
@Field { value:"topic: Topic record to be sent" }
@Field { value:"partition: Topic partition record to be sent" }
@Field { value:"timestamp: Timestamp to be considered over broker side" }
public struct ProducerRecord {
   blob key;
   blob value;
   string topic;
   int partition = -1;
   int timestamp = -1;
}