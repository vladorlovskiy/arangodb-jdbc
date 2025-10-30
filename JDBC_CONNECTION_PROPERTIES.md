# ArangoDB JDBC Connection Properties

This document describes the JDBC connection properties available for the ArangoDB JDBC driver. These properties can be passed when creating a connection via `DriverManager.getConnection()`.

## Connection URL Format

```
jdbc:arangodb://host:port/databaseName
```

Example:
```
jdbc:arangodb://localhost:8529/mydb
```

## Authentication Properties

### user
- **Type:** String
- **Default:** `root`
- **Description:** Username for authentication
- **Example:** `user=myuser`

### password
- **Type:** String
- **Default:** empty string
- **Description:** Password for authentication
- **Example:** `password=mypassword`

### jwt
- **Type:** String
- **Description:** JWT token for authentication (alternative to user/password)
- **Example:** `jwt=eyJhbGc...`

## Connection Properties

### timeout
- **Type:** Integer (milliseconds)
- **Default:** `30000` (30 seconds)
- **Description:** Connection and request timeout
- **Example:** `timeout=60000`

### maxConnections
- **Type:** Integer
- **Default:** `20` (HTTP protocols), `1` (VST protocol)
- **Description:** Maximum number of connections per host
- **Example:** `maxConnections=50`

### connectionTtl
- **Type:** Long (milliseconds)
- **Default:** `600000` (10 minutes)
- **Description:** Time-to-live for connections (after which connection will be closed)
- **Example:** `connectionTtl=300000`

### keepAliveInterval
- **Type:** Integer (milliseconds)
- **Default:** `60000` (1 minute)
- **Description:** Keep-alive interval for VST connections
- **Example:** `keepAliveInterval=30000`

## SSL/TLS Properties

### useSsl
- **Type:** Boolean
- **Default:** `false`
- **Description:** Enable SSL/TLS for the connection
- **Example:** `useSsl=true`

### verifyHost
- **Type:** Boolean
- **Default:** `true` (when SSL is enabled)
- **Description:** Enable hostname verification for SSL
- **Example:** `verifyHost=false`

## Cluster Properties

### acquireHostList
- **Type:** Boolean
- **Default:** `false`
- **Description:** Whether to acquire a list of available coordinators in an ArangoDB cluster or single server with active failover
- **Example:** `acquireHostList=true`

### acquireHostListInterval
- **Type:** Integer (milliseconds)
- **Default:** `60000` (1 minute)
- **Description:** Interval for acquiring the host list
- **Example:** `acquireHostListInterval=30000`

## Other Properties

### chunkSize
- **Type:** Integer
- **Default:** `8192`
- **Description:** Chunk size when using Protocol.VST
- **Example:** `chunkSize=16384`

### responseQueueTimeSamples
- **Type:** Integer
- **Default:** `10`
- **Description:** Number of samples kept for queue time metrics
- **Example:** `responseQueueTimeSamples=20`

### jdbcMetadataSampleSize
- **Type:** Integer
- **Default:** `1000`
- **Description:** Number of documents to sample when gathering metadata
- **Example:** `jdbcMetadataSampleSize=5000`

### jdbcSchemaName
- **Type:** String
- **Default:** `public`
- **Description:** Schema name (JDBC concept, not ArangoDB)
- **Example:** `jdbcSchemaName=myschema`

## Usage Examples

### Basic Connection
```java
String url = "jdbc:arangodb://localhost:8529/mydb";
Properties props = new Properties();
props.setProperty("user", "myuser");
props.setProperty("password", "mypassword");
Connection conn = DriverManager.getConnection(url, props);
```

### Connection with SSL
```java
String url = "jdbc:arangodb://localhost:8529/mydb";
Properties props = new Properties();
props.setProperty("user", "myuser");
props.setProperty("password", "mypassword");
props.setProperty("useSsl", "true");
props.setProperty("verifyHost", "true");
Connection conn = DriverManager.getConnection(url, props);
```

### Connection with Custom Timeout
```java
String url = "jdbc:arangodb://localhost:8529/mydb";
Properties props = new Properties();
props.setProperty("user", "myuser");
props.setProperty("password", "mypassword");
props.setProperty("timeout", "60000");
props.setProperty("maxConnections", "50");
Connection conn = DriverManager.getConnection(url, props);
```

### Connection with JWT Authentication
```java
String url = "jdbc:arangodb://localhost:8529/mydb";
Properties props = new Properties();
props.setProperty("jwt", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...");
Connection conn = DriverManager.getConnection(url, props);
```

### Connection to ArangoDB Cluster
```java
String url = "jdbc:arangodb://coordinator-host:8529/mydb";
Properties props = new Properties();
props.setProperty("user", "myuser");
props.setProperty("password", "mypassword");
props.setProperty("acquireHostList", "true");
Connection conn = DriverManager.getConnection(url, props);
```

## Properties Reference in Code

All property names are defined as constants in the `ArangoDbConstants` class:

- `ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE` → `"jdbcMetadataSampleSize"`
- `ArangoDbConstants.JDBC_SCHEMA_NAME` → `"jdbcSchemaName"`
- `ArangoDbConstants.PROPERTY_USER` → `"user"`
- `ArangoDbConstants.PROPERTY_PASSWORD` → `"password"`
- `ArangoDbConstants.PROPERTY_JWT` → `"jwt"`
- `ArangoDbConstants.PROPERTY_TIMEOUT` → `"timeout"`
- `ArangoDbConstants.PROPERTY_MAX_CONNECTIONS` → `"maxConnections"`
- `ArangoDbConstants.PROPERTY_CONNECTION_TTL` → `"connectionTtl"`
- `ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL` → `"keepAliveInterval"`
- `ArangoDbConstants.PROPERTY_USE_SSL` → `"useSsl"`
- `ArangoDbConstants.PROPERTY_VERIFY_HOST` → `"verifyHost"`
- `ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST` → `"acquireHostList"`
- `ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL` → `"acquireHostListInterval"`
- `ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES` → `"responseQueueTimeSamples"`
- `ArangoDbConstants.PROPERTY_CHUNK_SIZE` → `"chunkSize"`

These properties are automatically read and applied to the underlying ArangoDB client configuration in `ArangoConnection.initializeConnection()`.

