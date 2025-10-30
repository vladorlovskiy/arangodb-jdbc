package com.iotahoe.jdbc;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.Protocol;
import com.arangodb.internal.net.ProtocolProvider;

import java.sql.*;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ArangoDB JDBC Connection implementation.
 * This class implements the java.sql.Connection interface for ArangoDB.
 */
public class ArangoDbConnection implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbConnection.class);
    private final String url;
    private final Properties info;
    private ArangoDB arangoDB;
    private ArangoDatabase database;
    private boolean closed = false;
    private boolean autoCommit = true;
    private String catalog;
    private String schema;
    private int transactionIsolation = TRANSACTION_READ_COMMITTED;
    private boolean readOnly = false;
    private int jdbcMetadataSampleSize;
    
    public ArangoDbConnection(String url, Properties info) throws SQLException {
        this.url = url;
        this.info = info;
        initializeConnection();
    }
    
    private void initializeConnection() throws SQLException {
        LOGGER.debug("initializeConnection(url={}, info={})", 
            new Object[]{url, ArangoDbJdbcUtils.redactProperties(info)}
        );
        try {
            String host;
            int port;
            String dbName;

            if (url != null && !url.trim().isEmpty()) {
                if (!url.startsWith(ArangoDbConstants.URL_PREFIX)) {
                    throw new SQLException(String.format("Invalid URL format. Expected: %s//host:port/databaseName", ArangoDbConstants.URL_PREFIX));
                }
                String connectionString = url.substring(ArangoDbConstants.URL_PREFIX.length());
                String[] parts = connectionString.split("/");

                if (parts.length < 1 || parts[0].trim().isEmpty()) {
                    throw new SQLException(String.format("Invalid URL format. Expected: %s//host:port/databaseName", ArangoDbConstants.URL_PREFIX));
                }

                String hostPort = parts[0];
                dbName = parts.length > 1 ? parts[1] : ArangoDbConstants.SYSTEM_DATABASE;

                String[] hostPortParts = hostPort.split(":");
                host = hostPortParts[0];
                port = hostPortParts.length > 1 ? Integer.parseInt(hostPortParts[1]) : ArangoDbConstants.PORT_DEFAULT;
            } else {
                host = info.getProperty(ArangoDbConstants.PROPERTY_HOST, ArangoDbConstants.HOST_DEFAULT);
                port = Integer.parseInt(info.getProperty(ArangoDbConstants.PROPERTY_PORT, String.valueOf(ArangoDbConstants.PORT_DEFAULT)));
                dbName = info.getProperty(ArangoDbConstants.PROPERTY_DATABASE_NAME, ArangoDbConstants.SYSTEM_DATABASE);
            }

            // Create ArangoDB.Builder with all possible properties
            ArangoDB.Builder builder = new ArangoDB.Builder()
                .host(host, port);
            
            // Authentication properties
            String user = info.getProperty(ArangoDbConstants.PROPERTY_USER, "");
            String password = info.getProperty(ArangoDbConstants.PROPERTY_PASSWORD, "");
            
            if (info.containsKey(ArangoDbConstants.PROPERTY_JWT)) {
                builder.jwt(info.getProperty(ArangoDbConstants.PROPERTY_JWT));
            } else {
                builder.user(user).password(password);
            }
            
            // Connection timeout and timeout properties
            if (info.containsKey(ArangoDbConstants.PROPERTY_TIMEOUT)) {
                try {
                    builder.timeout(Integer.parseInt(info.getProperty(ArangoDbConstants.PROPERTY_TIMEOUT)));
                } catch (NumberFormatException e) {
                    // Invalid timeout value, use default
                    LOGGER.error("Invalid value for {}: {}", ArangoDbConstants.PROPERTY_TIMEOUT, info.getProperty(ArangoDbConstants.PROPERTY_TIMEOUT));
                }
            }
            
            // Max connections property
            if (info.containsKey(ArangoDbConstants.PROPERTY_MAX_CONNECTIONS)) {
                try {
                    builder.maxConnections(Integer.parseInt(info.getProperty(ArangoDbConstants.PROPERTY_MAX_CONNECTIONS)));
                } catch (NumberFormatException e) {
                    LOGGER.error("Invalid value for {}: {}", ArangoDbConstants.PROPERTY_MAX_CONNECTIONS, info.getProperty(ArangoDbConstants.PROPERTY_MAX_CONNECTIONS));
                }
            }
            
            // Connection TTL property
            if (info.containsKey(ArangoDbConstants.PROPERTY_CONNECTION_TTL)) {
                try {
                    builder.connectionTtl(Long.parseLong(info.getProperty(ArangoDbConstants.PROPERTY_CONNECTION_TTL)));
                } catch (NumberFormatException e) {
                    LOGGER.error("Invalid value for {}: {}", ArangoDbConstants.PROPERTY_CONNECTION_TTL, info.getProperty(ArangoDbConstants.PROPERTY_CONNECTION_TTL));
                }
            }
            
            // Keep-alive interval property
            if (info.containsKey(ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL)) {
                try {
                    builder.keepAliveInterval(Integer.parseInt(info.getProperty(ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL)));
                } catch (NumberFormatException e) {
                    LOGGER.error("Invalid value for {}: {}", ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL, info.getProperty(ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL));
                }
            }
            
            // SSL/TLS properties
            if (info.containsKey(ArangoDbConstants.PROPERTY_USE_SSL)) {
                builder.useSsl(Boolean.parseBoolean(info.getProperty(ArangoDbConstants.PROPERTY_USE_SSL)));
            }
            
            if (info.containsKey(ArangoDbConstants.PROPERTY_VERIFY_HOST)) {
                builder.verifyHost(Boolean.parseBoolean(info.getProperty(ArangoDbConstants.PROPERTY_VERIFY_HOST)));
            }
            
            // Cluster properties
            if (info.containsKey(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST)) {
                builder.acquireHostList(Boolean.parseBoolean(info.getProperty(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST)));
            }
            
            if (info.containsKey(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL)) {
                try {
                    builder.acquireHostListInterval(Integer.parseInt(info.getProperty(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL)));
                } catch (NumberFormatException e) {
                    LOGGER.error("Invalid value for {}: {}", ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL, info.getProperty(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL));
                }
            }
            
            // Response queue time samples property
            if (info.containsKey(ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES)) {
                try {
                    builder.responseQueueTimeSamples(Integer.parseInt(info.getProperty(ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES)));
                } catch (NumberFormatException e) {
                    LOGGER.error("Invalid value for {}: {}", ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES, info.getProperty(ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES));
                }
            }
            
            // Chunk size property
            if (info.containsKey(ArangoDbConstants.PROPERTY_CHUNK_SIZE)) {
                try {
                    builder.chunkSize(Integer.parseInt(info.getProperty(ArangoDbConstants.PROPERTY_CHUNK_SIZE)));
                } catch (NumberFormatException e) {
                    LOGGER.error("Invalid value for {}: {}", ArangoDbConstants.PROPERTY_CHUNK_SIZE, info.getProperty(ArangoDbConstants.PROPERTY_CHUNK_SIZE));
                }
            }

            // Build the ArangoDB instance
            arangoDB = builder.build();
            
            // Get database
            database = arangoDB.db(dbName);
            this.catalog = dbName;
            this.schema = info.getProperty("schema", ArangoDbConstants.JDBC_SCHEMA_NAME_DEFAULT);
            
            // Parse metadataSampleSize from properties with default value
            String sampleSizeStr = info.getProperty(ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE, ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE_DEFAULT);
            try {
                this.jdbcMetadataSampleSize = Integer.parseInt(sampleSizeStr);
            } catch (NumberFormatException e) {
                LOGGER.error("Invalid value for {}: {}, using default value: {}", ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE, sampleSizeStr, ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE_DEFAULT);
                this.jdbcMetadataSampleSize = Integer.parseInt(ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE_DEFAULT);
            }
            
        } catch (Exception e) {
            throw new SQLException("Failed to initialize ArangoDB connection", e);
        }
    }
    
    public ArangoDatabase getDatabase() {
        return database;
    }
    
    public String getUrl() {
        return url;
    }
    
    public ArangoDB getArangoDB() {
        return arangoDB;
    }
    
    public int getJdbcMetadataSampleSize() {
        return jdbcMetadataSampleSize;
    }

    @Override
    public Statement createStatement() throws SQLException {
        LOGGER.debug("createStatement()");
        checkClosed();
        return new ArangoDbStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        LOGGER.debug("prepareStatement(sql={})", sql);
        checkClosed();
        return new ArangoDbPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        LOGGER.debug("nativeSQL(sql={})", sql);
        return sql; // ArangoDB uses AQL, so we return as-is
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        LOGGER.debug("setAutoCommit(autoCommit={})", autoCommit);
        checkClosed();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        LOGGER.debug("commit()");
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot commit when autoCommit is true");
        }
        // ArangoDB transactions are handled differently
    }

    @Override
    public void rollback() throws SQLException {
        LOGGER.debug("rollback()");
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot rollback when autoCommit is true");
        }
        // ArangoDB transactions are handled differently
    }

    @Override
    public void close() throws SQLException {
        LOGGER.debug("close()");
        if (!closed) {
            if (arangoDB != null) {
                arangoDB.shutdown();
            }
            closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new ArangoDbDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        LOGGER.debug("setReadOnly(readOnly={})", readOnly);
        checkClosed();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        LOGGER.debug("setCatalog(catalog={})", catalog);
        checkClosed();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        LOGGER.debug("setTransactionIsolation(level={})", level);
        checkClosed();
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        LOGGER.debug("clearWarnings()");
        checkClosed();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        LOGGER.debug("createStatement(resultSetType={}, resultSetConcurrency={})", new Object[]{resultSetType, resultSetConcurrency});
        checkClosed();
        return new ArangoDbStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        LOGGER.debug("prepareStatement(sql={}, resultSetType={}, resultSetConcurrency={})", new Object[]{sql, resultSetType, resultSetConcurrency});
        checkClosed();
        return new ArangoDbPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setTypeMap not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setHoldability not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        LOGGER.debug("createStatement(resultSetType={}, resultSetConcurrency={}, resultSetHoldability={})", new Object[]{resultSetType, resultSetConcurrency, resultSetHoldability});
        checkClosed();
        return new ArangoDbStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        LOGGER.debug("prepareStatement(sql={}, resultSetType={}, resultSetConcurrency={}, resultSetHoldability={})", new Object[]{sql, resultSetType, resultSetConcurrency, resultSetHoldability});
        checkClosed();
        return new ArangoDbPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        LOGGER.debug("prepareStatement(sql={}, autoGeneratedKeys={})", new Object[]{sql, autoGeneratedKeys});
        checkClosed();
        return new ArangoDbPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        LOGGER.debug("prepareStatement(sql={}, columnIndexes={})", new Object[]{sql, columnIndexes});
        checkClosed();
        return new ArangoDbPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        LOGGER.debug("prepareStatement(sql={}, columnNames={})", new Object[]{sql, columnNames});
        checkClosed();
        return new ArangoDbPreparedStatement(this, sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        LOGGER.debug("isValid(timeout={})", timeout);
        if (closed) {
            return false;
        }
        try {
            // Test connection by getting database info
            database.getInfo();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException("setClientInfo not supported", Collections.emptyMap());
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException("setClientInfo not supported", Collections.emptyMap());
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Struct not supported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        LOGGER.debug("setSchema(schema={})", schema);
        checkClosed();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        LOGGER.debug("abort(executor={})", executor);
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        LOGGER.debug("setNetworkTimeout(executor={}, milliseconds={})", new Object[]{executor, milliseconds});
        checkClosed();
        // Not supported
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
    
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
}

