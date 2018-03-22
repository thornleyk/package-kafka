package ballerina.net.kafka;

public struct ServiceEndpoint {
    Context context;
    ServiceEndpointConfiguration config;
}

@Description { value:"Struct which represents Kafka Consumer configuration" }
@Field { value:"bootstrapServers: List of remote server endpoints." }
@Field { value:"groupId: Unique string that identifies the consumer." }
@Field { value:"offsetReset: Offset reset strategy if no initial offset." }
@Field { value:"partitionAssignmentStrategy: Strategy class for handle partition assignment among consumers." }
@Field { value:"metricsRecordingLevel: Metrics recording level." }
@Field { value:"metricReporterClasses: Metrics reporter classes." }
@Field { value:"clientID: Id to be used for server side logging." }
@Field { value:"interceptorClasses: Interceptor classes to be used before sending records." }
@Field { value:"isolationLevel: How the transactional messages are read." }
@Field { value:"sessionTimeout: Timeout used to detect consumer failures when heartbeat threshold is reached." }
@Field { value:"heartBeatInterval: Expected time between heartbeats." }
@Field { value:"metadataMaxAge: Max time to force a refresh of metadata." }
@Field { value:"autoCommitInterval: Offset committing interval." }
@Field { value:"maxPartitionFetchBytes: The max amount of data per-partition the server return." }
@Field { value:"sendBuffer: Size of the TCP send buffer (SO_SNDBUF)." }
@Field { value:"receiveBuffer: Size of the TCP receive buffer (SO_RCVBUF)." }
@Field { value:"fetchMinBytes: Minimum amount of data the server should return for a fetch request." }
@Field { value:"fetchMaxBytes: Maximum amount of data the server should return for a fetch request." }
@Field { value:"fetchMaxWait: Maximum amount of time the server will block before answering the fetch request." }
@Field { value:"reconnectBackoffMax: Maximum amount of time in milliseconds to wait when reconnecting." }
@Field { value:"retryBackoff: Time to wait before attempting to retry a failed request." }
@Field { value:"metricsSampleWindow: Window of time a metrics sample is computed over." }
@Field { value:"metricsNumSamples: Number of samples maintained to compute metrics." }
@Field { value:"requestTimeout: Wait time for response of a request." }
@Field { value:"connectionsMaxIdle: Close idle connections after the number of milliseconds." }
@Field { value:"maxPollRecords: Maximum number of records returned in a single call to poll." }
@Field { value:"maxPollInterval: Maximum delay between invocations of poll." }
@Field { value:"reconnectBackoff: Time to wait before attempting to reconnect." }
@Field { value:"autoCommit: Enables auto commit offsets." }
@Field { value:"checkCRCS: Check the CRC32 of the records consumed." }
@Field { value:"excludeInternalTopics: Whether records from internal topics should be exposed to the consumer." }
public struct ServiceEndpointConfiguration {
    string bootstrapServers;                    // BOOTSTRAP_SERVERS_CONFIG 0
    string groupId;                             // GROUP_ID_CONFIG 1
    string offsetReset;                         // AUTO_OFFSET_RESET_CONFIG 2
    string partitionAssignmentStrategy;         // PARTITION_ASSIGNMENT_STRATEGY_CONFIG 3
    string metricsRecordingLevel;               // METRICS_RECORDING_LEVEL_CONFIG 4
    string metricsReporterClasses;              // METRIC_REPORTER_CLASSES_CONFIG 5
    string clientId;                            // CLIENT_ID_CONFIG 6
    string interceptorClasses;                  // INTERCEPTOR_CLASSES_CONFIG 7
    string isolationLevel;                      // ISOLATION_LEVEL_CONFIG 8

    int sessionTimeout = -1;                    // SESSION_TIMEOUT_MS_CONFIG  0
    int heartBeatInterval = -1;                 // HEARTBEAT_INTERVAL_MS_CONFIG 1
    int metadataMaxAge = -1;                    // METADATA_MAX_AGE_CONFIG  2
    int autoCommitInterval = -1;                // AUTO_COMMIT_INTERVAL_MS_CONFIG 3
    int maxPartitionFetchBytes = -1;            // MAX_PARTITION_FETCH_BYTES_CONFIG 4
    int sendBuffer = -1;                        // SEND_BUFFER_CONFIG 5
    int receiveBuffer = -1;                     // RECEIVE_BUFFER_CONFIG 6
    int fetchMinBytes = -1;                     // FETCH_MIN_BYTES_CONFIG 7
    int fetchMaxBytes = -1;                     // FETCH_MAX_BYTES_CONFIG 8
    int fetchMaxWait = -1;                      // FETCH_MAX_WAIT_MS_CONFIG 9
    int reconnectBackoffMax = -1;               // RECONNECT_BACKOFF_MAX_MS_CONFIG 10
    int retryBackoff = -1;                      // RETRY_BACKOFF_MS_CONFIG 11
    int metricsSampleWindow = -1;               // METRICS_SAMPLE_WINDOW_MS_CONFIG 12
    int metricsNumSamples = -1;                 // METRICS_NUM_SAMPLES_CONFIG 13
    int requestTimeout = -1;                    // REQUEST_TIMEOUT_MS_CONFIG 14
    int connectionMaxIdle = -1;                 // CONNECTIONS_MAX_IDLE_MS_CONFIG 15
    int maxPollRecords = -1;                    // MAX_POLL_RECORDS_CONFIG 16
    int maxPollInterval = -1;                   // MAX_POLL_INTERVAL_MS_CONFIG 17
    int reconnectBackoff = -1;                  // RECONNECT_BACKOFF_MAX_MS_CONFIG 18

    boolean autoCommit = true;                  // ENABLE_AUTO_COMMIT_CONFIG 0
    boolean checkCRCS = true;                   // CHECK_CRCS_CONFIG 1
    boolean excludeInternalTopics = true;       // EXCLUDE_INTERNAL_TOPICS_CONFIG 2
}



@Description { value:"Connects to consumer to external Kafka broker"}
@Return { value:"error: Error will be returned if connection to broker is failed" }
public native function <ServiceEndpoint consumer> connect() (error);

@Description { value:"Subscribes to consumer to external Kafka broker topic pattern"}
@Param { value:"regex: Topic pattern to be subscribed" }
@Return { value:"error: Error will be returned if subscription to broker is failed" }
public native function <ServiceEndpoint consumer> subscribeToPattern(string regex) (error);

@Description { value:"Subscribes to consumer to external Kafka broker topic array"}
@Param { value:"regex: Topic array to be subscribed" }
@Return { value:"error: Error will be returned if subscription to broker is failed" }
public native function <ServiceEndpoint consumer> subscribe(string[] topics) (error);

@Description { value:"Subscribes consumer to external Kafka broker topic with rebalance listening is enabled"}
@Param { value:"regex: Topic array to be subscribed" }
@Param { value:"onPartitionsRevoked: Function will be executed if partitions are revoked from this consumer" }
@Param { value:"onPartitionsAssigned: Function will be executed if partitions are assigned this consumer" }
@Return { value:"error: Error will be returned if subscription to broker is failed" }
public native function <ServiceEndpoint consumer> subscribeWithPartitionRebalance(string[] topics,
  function(Consumer consumer, TopicPartition[] partitions) onPartitionsRevoked,
  function(Consumer consumer, TopicPartition[] partitions) onPartitionsAssigned) (error);

@Description { value:"Assign consumer to external Kafka broker set of topic partitions"}
@Param { value:"partitions: Topic partitions to be assigned" }
@Return { value:"error: Error will be returned if assignment to broker is failed" }
public native function <ServiceEndpoint consumer> assign(TopicPartition[] partitions) (error);

@Description { value:"Returns current offset position in which consumer is at"}
@Param { value:"partition: Topic partitions in which the position is required" }
@Return { value:"int: Position in which the consumer is at in given Topic partition" }
@Return { value:"error: Error will be returned if position retrieval from broker is failed" }
public native function <ServiceEndpoint consumer> getPositionOffset(TopicPartition partition) (int, error);

@Description { value:"Returns current assignment of partitions for a consumer"}
@Return { value:"TopicPartition[]: Assigned partitions array for consumer" }
@Return { value:"error: Error will be returned if assignment retrieval from broker is failed" }
public native function <ServiceEndpoint consumer> getAssignment() (TopicPartition[], error);

@Description { value:"Returns current subscription of topics for a consumer"}
@Return { value:"string[]: Subscribed topic array for consumer" }
@Return { value:"error: Error will be returned if subscription retrieval from broker is failed" }
public native function <ServiceEndpoint consumer> getSubscription() (string[], error);

@Description { value:"Returns current subscription of topics for a consumer"}
@Param { value:"partition: Partition in which offset is returned for consumer" }
@Return { value:"Offset: Committed offset for consumer for given partition" }
@Return { value:"error: Error will be returned if committed offset retrieval from broker is failed" }
public native function <ServiceEndpoint consumer> getCommittedOffset(TopicPartition partition) (Offset, error);

@Description { value:"Poll the consumer for external broker for records"}
@Param { value:"timeoutValue: Polling time in milliseconds" }
@Return { value:"ConsumerRecord[]: Consumer record array" }
@Return { value:"error: Error will be returned if record retrieval from broker is failed" }
public native function <ServiceEndpoint consumer> poll(int timeoutValue) (ConsumerRecord[], error);

@Description { value:"Commits current consumed offsets for consumer"}
public native function <ServiceEndpoint consumer> commit();

@Description { value:"Commits given offsets for consumer"}
@Param { value:"offsets: Offsets to be commited" }
public native function <ServiceEndpoint consumer> commitOffset(Offset[] offsets);

@Description { value:"Seek consumer for given offset in a topic partition" }
@Param { value:"offset: Given offset to seek" }
@Return { value:"error: Error will be returned if seeking of position is failed" }
public native function <ServiceEndpoint consumer> seek(Offset offset) (error);

@Description { value:"Seek consumer for beginning offsets for set of topic partitions"}
@Param { value:"partitions: Set of partitions to seek" }
@Return { value:"error: Error will be returned if seeking of partitions is failed" }
public native function <ServiceEndpoint consumer> seekToBeginning(TopicPartition[] partitions) (error);

@Description { value:"Seek consumer for end offsets for set of topic partitions"}
@Param { value:"partitions: Set of partitions to seek" }
@Return { value:"error: Error will be returned if seeking of partitions is failed" }
public native function <ServiceEndpoint consumer> seekToEnd(TopicPartition[] partitions) (error);

@Description { value:"Retrieve the set of partitions which topic belongs"}
@Param { value:"topic: Given topic for partition information is needed" }
@Return { value:"TopicPartition[]: Partition array for given topic" }
@Return { value:"error: Error will be returned if retrieval of partition information is failed" }
public native function <ServiceEndpoint consumer> getTopicPartitions (string topic) (TopicPartition[], error);

@Description { value:"Un-subscribe consumer from all external broaker topic subscription"}
@Return { value:"error: Error will be returned if unsubscription from topics is failed" }
public native function <ServiceEndpoint consumer> unsubscribe() (error);

@Description { value:"Closes consumer connection to external Kafka broker"}
@Return { value:"error: Error will be returned if connection close to broker is failed" }
public native function <ServiceEndpoint consumer> close() (error);

@Description { value:"Pause consumer retrieving messages from set of partitions"}
@Param { value:"partitions: Set of partitions to pause messages retrieval" }
@Return { value:"error: Error will be returned if pausing message retrieval is failed" }
public native function <ServiceEndpoint consumer> pause(TopicPartition[] partitions) (error);

@Description { value:"Resume consumer retrieving messages from set of partitions which were paused earlier"}
@Param { value:"partitions: Set of partitions to resume messages retrieval" }
@Return { value:"error: Error will be returned if resuming message retrieval is failed" }
public native function <ServiceEndpoint consumer> resume(TopicPartition[] partitions) (error);

@Description { value:"Returns partitions in which the consumer is paused retrieving messages"}
@Return { value:"TopicPartition[]: Set of partitions paused from message retrieval" }
@Return { value:"error: Error will be returned if paused partitions retrieval is failed" }
public native function <ServiceEndpoint consumer> getPausedPartitions() (TopicPartition[], error);

@Description { value:"Returns start offsets for given set of partitions"}
@Param { value:"partitions: Set of partitions to return start offsets" }
@Return { value:"Offset[]: Start offsets for partitions" }
@Return { value:"error: Error will be returned if offset retrieval is failed" }
public native function <ServiceEndpoint consumer> getBeginningOffsets(TopicPartition[] partitions) (Offset[], error);

@Description { value:"Returns last offsets for given set of partitions"}
@Param { value:"partitions: Set of partitions to return last offsets" }
@Return { value:"Offset[]: Last offsets for partitions" }
@Return { value:"error: Error will be returned if offset retrieval is failed" }
public native function <ServiceEndpoint consumer> getEndOffsets(TopicPartition[] partitions) (Offset[], error);

