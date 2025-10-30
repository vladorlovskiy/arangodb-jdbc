package com.iotahoe.jdbc;

public final class ArangoDbConstants {
    public static final String URL_PREFIX = "jdbc:arangodb://";
    public static final String SYSTEM_DATABASE = "_system";
    public static final String KEY_ATTRIBUTE = "_key";

    public static final String HOST_DEFAULT = "localhost";
    public static final int PORT_DEFAULT = 8529;
    
    // Metadata configuration
    public static final String JDBC_METADATA_SAMPLE_SIZE = "jdbcMetadataSampleSize";
    public static final String JDBC_METADATA_SAMPLE_SIZE_DEFAULT = "1000";

    public static final String JDBC_SCHEMA_NAME = "jdbcSchemaName";
    public static final String JDBC_SCHEMA_NAME_DEFAULT = "public";
    
    // Authentication properties
    public static final String PROPERTY_URL = "url";
    public static final String PROPERTY_HOST = "host";
    public static final String PROPERTY_PORT = "port";
    public static final String PROPERTY_DATABASE_NAME = "databaseName";
    public static final String PROPERTY_USER = "user";
    public static final String PROPERTY_PASSWORD = "password";
    public static final String PROPERTY_JWT = "jwt";
    
    // Connection properties
    public static final String PROPERTY_TIMEOUT = "timeout";
    public static final String PROPERTY_MAX_CONNECTIONS = "maxConnections";
    public static final String PROPERTY_CONNECTION_TTL = "connectionTtl";
    public static final String PROPERTY_KEEP_ALIVE_INTERVAL = "keepAliveInterval";
    
    // SSL/TLS properties
    public static final String PROPERTY_USE_SSL = "useSsl";
    public static final String PROPERTY_VERIFY_HOST = "verifyHost";
    
    // Cluster properties
    public static final String PROPERTY_ACQUIRE_HOST_LIST = "acquireHostList";
    public static final String PROPERTY_ACQUIRE_HOST_LIST_INTERVAL = "acquireHostListInterval";
    
    // Response queue properties
    public static final String PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES = "responseQueueTimeSamples";
    public static final String PROPERTY_CHUNK_SIZE = "chunkSize";
}
