package ballerina.net.kafka;

@Description {value:"Represents a JMS client endpoint"}
@Field {value:"epName: The name of the endpoint"}
@Field {value:"config: The configurations associated with the endpoint"}
public struct ClientEndpoint {
    string epName;
    ClientEndpointConfiguration config;
}

@Description { value:"Struct which represents Kafka Producer configuration" }
@Field { value:"bootstrapServers: List of remote server endpoints." }
@Field { value:"acks: Number of acknowledgments." }
@Field { value:"compressionType: Compression type to be used for." }
@Field { value:"clientID: Id to be used for server side logging." }
@Field { value:"metricsRecordingLevel: Metrics recording level." }
@Field { value:"metricReporterClasses: Metrics reporter classes." }
@Field { value:"partitionerClass: Partitioner class to be used to select partition the message is sent." }
@Field { value:"interceptorClasses: Interceptor classes to be used before sending records." }
@Field { value:"transactionalID: TransactionalId to use for transactional delivery." }
@Field { value:"bufferMemory: Total bytes of memory the producer can use to buffer records ." }
@Field { value:"noRetries: Number of retries to resend a record." }
@Field { value:"batchSize: Number of records to be batched for a single request." }
@Field { value:"linger: Delay to allow other records to be batched." }
@Field { value:"sendBuffer: Size of the TCP send buffer (SO_SNDBUF)." }
@Field { value:"receiveBuffer: Size of the TCP receive buffer (SO_RCVBUF)." }
@Field { value:"maxRequestSize: The maximum size of a request in bytes." }
@Field { value:"reconnectBackoff: Time to wait before attempting to reconnect." }
@Field { value:"reconnectBackoffMax: Maximum amount of time in milliseconds to wait when reconnecting." }
@Field { value:"retryBackoff: Time to wait before attempting to retry a failed request." }
@Field { value:"maxBlock: Max block time which the send is blocked if buffer is full ." }
@Field { value:"requestTimeout: Wait time for response of a request." }
@Field { value:"metadataMaxAge: Max time to force a refresh of metadata." }
@Field { value:"metricsSampleWindow: Window of time a metrics sample is computed over." }
@Field { value:"metricsNumSamples: Number of samples maintained to compute metrics." }
@Field { value:"maxInFlightRequestsPerConnection: Maximum number of unacknowledged requests on a single connection." }
@Field { value:"connectionsMaxIdle: Close idle connections after the number of milliseconds." }
@Field { value:"transactionTimeout: Timeout fro transaction status update from the producer." }
@Field { value:"enableIdempotence: Exactly one copy of each message is written in the stream when enabled." }
public struct ClientEndpointConfiguration {
    string bootstrapServers;                    // BOOTSTRAP_SERVERS_CONFIG 0
    string acks;                                // ACKS_CONFIG 1
    string compressionType;                     // COMPRESSION_TYPE_CONFIG 2
    string clientID;                            // CLIENT_ID_CONFIG 3
    string metricsRecordingLevel;               // METRICS_RECORDING_LEVEL_CONFIG 4
    string metricReporterClasses;               // METRIC_REPORTER_CLASSES_CONFIG 5
    string partitionerClass;                    // PARTITIONER_CLASS_CONFIG 6
    string interceptorClasses;                  // INTERCEPTOR_CLASSES_CONFIG 7
    string transactionalID;                     // TRANSACTIONAL_ID_CONFIG 8

    int bufferMemory = -1;                      // BUFFER_MEMORY_CONFIG 0
    int noRetries = -1;                          // RETRIES_CONFIG 1
    int batchSize = -1;                         // BATCH_SIZE_CONFIG 2
    int linger = -1;                            // LINGER_MS_CONFIG 3
    int sendBuffer = -1;                        // SEND_BUFFER_CONFIG 4
    int receiveBuffer = -1;                     // RECEIVE_BUFFER_CONFIG 5
    int maxRequestSize = -1;                    // MAX_REQUEST_SIZE_CONFIG 6
    int reconnectBackoff = -1;                  // RECONNECT_BACKOFF_MS_CONFIG 7
    int reconnectBackoffMax = -1;               // RECONNECT_BACKOFF_MAX_MS_CONFIG  8
    int retryBackoff = -1;                      // RETRY_BACKOFF_MS_CONFIG 9
    int maxBlock = -1;                          // MAX_BLOCK_MS_CONFIG 10
    int requestTimeout = -1;                    // REQUEST_TIMEOUT_MS_CONFIG  11
    int metadataMaxAge = -1;                    // METADATA_MAX_AGE_CONFIG 12
    int metricsSampleWindow = -1;               // METRICS_SAMPLE_WINDOW_MS_CONFIG 13
    int metricsNumSamples = -1;                 // METRICS_NUM_SAMPLES_CONFIG  14
    int maxInFlightRequestsPerConnection = -1;  // MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION 15
    int connectionsMaxIdle = -1;                // CONNECTIONS_MAX_IDLE_MS_CONFIG 16
    int transactionTimeout = -1;                // TRANSACTION_TIMEOUT_CONFIG 17

    boolean enableIdempotence = false;          // ENABLE_IDEMPOTENCE_CONFIG 0
}

public function <ClientEndpoint ep> init (ClientEndpointConfiguration config) {
    ep.config = config;
    ep.initEndpoint();
}

public native function<ClientEndpoint ep> initEndpoint ();

public native function<ClientEndpoint ep> createTextMessage () (Message);

public function <ClientEndpoint ep> register (typedesc serviceType) {

}

public function <ClientEndpoint ep> start () {

}

@Description { value:"Returns the connector that client code uses"}
@Return { value:"The connector that client code uses" }
public native function <ClientEndpoint ep> getClient () (ClientConnector);

@Description { value:"Stops the registered service"}
@Return { value:"Error occured during registration" }
public function <ClientEndpoint ep> stop () {

}


