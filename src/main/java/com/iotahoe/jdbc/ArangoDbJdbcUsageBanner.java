package com.iotahoe.jdbc;

public class ArangoDbJdbcUsageBanner {

    public static void main(String[] args) {
        System.out.println("ArangoDB JDBC driver usage:");
        System.out.println(String.format(" - URL format: %s<host>:<port>/<databaseName>", ArangoDbConstants.URL_PREFIX));
        System.out.println(String.format(" - Default host: %s", ArangoDbConstants.HOST_DEFAULT));
        System.out.println(String.format(" - Default port: %s", ArangoDbConstants.PORT_DEFAULT));
        System.out.println(String.format(" - Default databaseName: %s", ArangoDbConstants.SYSTEM_DATABASE));
        System.out.println(String.format(" - Driver class name: %s", ArangoDbDriver.class.getName()));
        System.out.println("Properties configuring connection over ArangoDB client:");
        System.out.println(String.format(" - user"));
        System.out.println(String.format(" - password"));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_TIMEOUT));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_MAX_CONNECTIONS));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_CONNECTION_TTL));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_USE_SSL));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_VERIFY_HOST));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES));
        System.out.println(String.format(" - %s", ArangoDbConstants.PROPERTY_CHUNK_SIZE));
        System.out.println("Properties configuring metadata gathering by JDBC Driver:");
        System.out.println(String.format(" - %s, default value: %s", ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE, ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE_DEFAULT));
        System.out.println(String.format(" - %s, default value: %s", ArangoDbConstants.JDBC_SCHEMA_NAME, ArangoDbConstants.JDBC_SCHEMA_NAME_DEFAULT));

        System.exit(0);
    }
}
