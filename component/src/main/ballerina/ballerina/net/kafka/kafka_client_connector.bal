package ballerina.net.kafka;

public struct ClientConnector {
    map producerHolder = {};
    string connectorId;
    ClientEndpointConfiguration config;
}

@Description { value:"Simple Send action which produce records to Kafka server"}
@Param { value:"value: value of Kafka ProducerRecord to be sent." }
@Param { value:"topic: topic of Kafka ProducerRecord to be sent." }
public native function<ClientConnector ep> send (blob value, string topic);

@Description { value:"Advanced Send action which produce records to Kafka server"}
@Param { value:"record: ProducerRecord to be sent." }
public native function<ClientConnector ep> sendAdvanced (ProducerRecord record);

@Description { value:"Flush action which flush batch of records"}
public native function<ClientConnector ep> flush ();

@Description { value:"Close action which closes Kafka producer"}
public native function<ClientConnector ep> close ();

@Description { value:"GetTopicPartitions action which returns given topic partition information"}
@Param { value:"topic: Topic which partition information is given" }
@Return { value:"TopicPartition[]: Partition for given topic" }
public native function<ClientConnector ep> getTopicPartitions (string topic) (TopicPartition[]);

@Description { value:"CommitConsumer action which commits consumer consumed offsets to offset topic"}
@Param { value:"consumer: Consumer which needs offsets to be committed" }
public native function<ClientConnector ep> commitConsumer (Consumer consumer);

@Description { value:"CommitConsumerOffsets action which commits consumer offsets in given transaction"}
@Param { value:"offsets: Consumer offsets to commit for given transaction" }
@Param { value:"groupID: Consumer group id" }
public native function<ClientConnector ep> commitConsumerOffsets (Offset[] offsets, string groupID);