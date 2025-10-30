package com.iotahoe.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

public class ArangoDbDataSource implements DataSource {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ArangoDbDataSource.class);
    private final Properties properties = new Properties();
    private PrintWriter logWriter;
    private int loginTimeout = 0;

    public ArangoDbDataSource() {
        LOGGER.debug("ArangoDbDataSource()");
        properties.setProperty(ArangoDbConstants.PROPERTY_HOST, ArangoDbConstants.HOST_DEFAULT);
        properties.setProperty(ArangoDbConstants.PROPERTY_PORT, String.valueOf(ArangoDbConstants.PORT_DEFAULT));
        properties.setProperty(ArangoDbConstants.PROPERTY_DATABASE_NAME, ArangoDbConstants.SYSTEM_DATABASE);
    }

    @Override
    public Connection getConnection() throws SQLException {
        LOGGER.debug("getConnection()");
        return getConnection(
                properties.getProperty(ArangoDbConstants.PROPERTY_USER),
                properties.getProperty(ArangoDbConstants.PROPERTY_PASSWORD)
        );
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        LOGGER.debug("getConnection(username={}, password=REDACTED)", username);
        Properties connectionProps = new Properties(properties);
        if (username != null) {
            connectionProps.setProperty(ArangoDbConstants.PROPERTY_USER, username);
        }
        if (password != null) {
            connectionProps.setProperty(ArangoDbConstants.PROPERTY_PASSWORD, password);
        }
        String url = properties.getProperty(ArangoDbConstants.PROPERTY_URL);
        return new ArangoDbConnection(url, connectionProps);
    }

    // Getters and Setters
    public String getUrl() {
        return properties.getProperty(ArangoDbConstants.PROPERTY_URL);
    }

    public void setUrl(String url) {
        LOGGER.debug("setUrl(url={})", url);
        properties.setProperty(ArangoDbConstants.PROPERTY_URL, url);
    }

    public String getHost() {
        return properties.getProperty(ArangoDbConstants.PROPERTY_HOST);
    }

    public void setHost(String serverName) {
        LOGGER.debug("setHost(serverName={})", serverName);
        properties.setProperty(ArangoDbConstants.PROPERTY_HOST, serverName);
    }

    public int getPort() {
        return Integer.parseInt(properties.getProperty(ArangoDbConstants.PROPERTY_PORT));
    }

    public void setPort(int port) {
        LOGGER.debug("setPort(port={})", port);
        properties.setProperty(ArangoDbConstants.PROPERTY_PORT, String.valueOf(port));
    }

    public String getDatabaseName() {
        return properties.getProperty(ArangoDbConstants.PROPERTY_DATABASE_NAME);
    }

    public void setDatabaseName(String databaseName) {
        LOGGER.debug("setDatabaseName(databaseName={})", databaseName);
        properties.setProperty(ArangoDbConstants.PROPERTY_DATABASE_NAME, databaseName);
    }

    public String getUser() {
        return properties.getProperty(ArangoDbConstants.PROPERTY_USER);
    }

    public void setUser(String user) {
        LOGGER.debug("setUser(user={})", user);
        properties.setProperty(ArangoDbConstants.PROPERTY_USER, user);
    }

    public String getPassword() {
        return properties.getProperty(ArangoDbConstants.PROPERTY_PASSWORD);
    }

    public void setPassword(String password) {
        LOGGER.debug("setPassword(password=REDACTED)");
        properties.setProperty(ArangoDbConstants.PROPERTY_PASSWORD, password);
    }

    public String getJwt() {
        return properties.getProperty(ArangoDbConstants.PROPERTY_JWT);
    }

    public void setJwt(String jwt) {
        LOGGER.debug("setJwt(jwt=REDACTED)");
        properties.setProperty(ArangoDbConstants.PROPERTY_JWT, jwt);
    }

    public Integer getTimeout() {
        String timeout = properties.getProperty(ArangoDbConstants.PROPERTY_TIMEOUT);
        return timeout != null ? Integer.valueOf(timeout) : null;
    }

    public void setTimeout(Integer timeout) {
        LOGGER.debug("setTimeout(timeout={})", timeout);
        if (timeout != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_TIMEOUT, timeout.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_TIMEOUT);
        }
    }

    public Integer getMaxConnections() {
        String maxConnections = properties.getProperty(ArangoDbConstants.PROPERTY_MAX_CONNECTIONS);
        return maxConnections != null ? Integer.valueOf(maxConnections) : null;
    }

    public void setMaxConnections(Integer maxConnections) {
        LOGGER.debug("setMaxConnections(maxConnections={})", maxConnections);
        if (maxConnections != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_MAX_CONNECTIONS, maxConnections.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_MAX_CONNECTIONS);
        }
    }

    public Long getConnectionTtl() {
        String connectionTtl = properties.getProperty(ArangoDbConstants.PROPERTY_CONNECTION_TTL);
        return connectionTtl != null ? Long.valueOf(connectionTtl) : null;
    }

    public void setConnectionTtl(Long connectionTtl) {
        LOGGER.debug("setConnectionTtl(connectionTtl={})", connectionTtl);
        if (connectionTtl != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_CONNECTION_TTL, connectionTtl.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_CONNECTION_TTL);
        }
    }

    public Integer getKeepAliveInterval() {
        String keepAliveInterval = properties.getProperty(ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL);
        return keepAliveInterval != null ? Integer.valueOf(keepAliveInterval) : null;
    }

    public void setKeepAliveInterval(Integer keepAliveInterval) {
        LOGGER.debug("setKeepAliveInterval(keepAliveInterval={})", keepAliveInterval);
        if (keepAliveInterval != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL, keepAliveInterval.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_KEEP_ALIVE_INTERVAL);
        }
    }

    public Boolean getUseSsl() {
        String useSsl = properties.getProperty(ArangoDbConstants.PROPERTY_USE_SSL);
        return useSsl != null ? Boolean.valueOf(useSsl) : null;
    }

    public void setUseSsl(Boolean useSsl) {
        LOGGER.debug("setUseSsl(useSsl={})", useSsl);
        if (useSsl != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_USE_SSL, useSsl.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_USE_SSL);
        }
    }

    public Boolean getVerifyHost() {
        String verifyHost = properties.getProperty(ArangoDbConstants.PROPERTY_VERIFY_HOST);
        return verifyHost != null ? Boolean.valueOf(verifyHost) : null;
    }

    public void setVerifyHost(Boolean verifyHost) {
        LOGGER.debug("setVerifyHost(verifyHost={})", verifyHost);
        if (verifyHost != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_VERIFY_HOST, verifyHost.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_VERIFY_HOST);
        }
    }

    public Boolean getAcquireHostList() {
        String acquireHostList = properties.getProperty(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST);
        return acquireHostList != null ? Boolean.valueOf(acquireHostList) : null;
    }

    public void setAcquireHostList(Boolean acquireHostList) {
        LOGGER.debug("setAcquireHostList(acquireHostList={})", acquireHostList);
        if (acquireHostList != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST, acquireHostList.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST);
        }
    }

    public Integer getAcquireHostListInterval() {
        String acquireHostListInterval = properties.getProperty(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL);
        return acquireHostListInterval != null ? Integer.valueOf(acquireHostListInterval) : null;
    }

    public void setAcquireHostListInterval(Integer acquireHostListInterval) {
        LOGGER.debug("setAcquireHostListInterval(acquireHostListInterval={})", acquireHostListInterval);
        if (acquireHostListInterval != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL, acquireHostListInterval.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_ACQUIRE_HOST_LIST_INTERVAL);
        }
    }

    public Integer getResponseQueueTimeSamples() {
        String responseQueueTimeSamples = properties.getProperty(ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES);
        return responseQueueTimeSamples != null ? Integer.valueOf(responseQueueTimeSamples) : null;
    }

    public void setResponseQueueTimeSamples(Integer responseQueueTimeSamples) {
        LOGGER.debug("setResponseQueueTimeSamples(responseQueueTimeSamples={})", responseQueueTimeSamples);
        if (responseQueueTimeSamples != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES, responseQueueTimeSamples.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_RESPONSE_QUEUE_TIME_SAMPLES);
        }
    }

    public Integer getChunkSize() {
        String chunkSize = properties.getProperty(ArangoDbConstants.PROPERTY_CHUNK_SIZE);
        return chunkSize != null ? Integer.valueOf(chunkSize) : null;
    }

    public void setChunkSize(Integer chunkSize) {
        LOGGER.debug("setChunkSize(chunkSize={})", chunkSize);
        if (chunkSize != null) {
            properties.setProperty(ArangoDbConstants.PROPERTY_CHUNK_SIZE, chunkSize.toString());
        } else {
            properties.remove(ArangoDbConstants.PROPERTY_CHUNK_SIZE);
        }
    }

    public String getSchema() {
        return properties.getProperty("schema");
    }

    public void setSchema(String schema) {
        LOGGER.debug("setSchema(schema={})", schema);
        properties.setProperty("schema", schema);
    }

    public Integer getJdbcMetadataSampleSize() {
        String jdbcMetadataSampleSize = properties.getProperty(ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE);
        return jdbcMetadataSampleSize != null ? Integer.valueOf(jdbcMetadataSampleSize) : null;
    }

    public void setJdbcMetadataSampleSize(Integer jdbcMetadataSampleSize) {
        LOGGER.debug("setJdbcMetadataSampleSize(jdbcMetadataSampleSize={})", jdbcMetadataSampleSize);
        if (jdbcMetadataSampleSize != null) {
            properties.setProperty(ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE, jdbcMetadataSampleSize.toString());
        } else {
            properties.remove(ArangoDbConstants.JDBC_METADATA_SAMPLE_SIZE);
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        LOGGER.debug("setLogWriter(out={})", out);
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        LOGGER.debug("setLoginTimeout(seconds={})", seconds);
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger not supported");
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
}
