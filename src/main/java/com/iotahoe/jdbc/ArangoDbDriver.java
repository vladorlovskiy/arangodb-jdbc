package com.iotahoe.jdbc;

import java.sql.*;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ArangoDB JDBC Driver implementation.
 * This class implements the java.sql.Driver interface for ArangoDB.
 */
public class ArangoDbDriver implements Driver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbDriver.class);
    private static final int DRIVER_MAJOR_VERSION = 1;
    private static final int DRIVER_MINOR_VERSION = 0;
    
    private static final String URL_PREFIX = "jdbc:arangodb:";
    
    static {
        try {
            DriverManager.registerDriver(new ArangoDbDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register ArangoDB JDBC driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        LOGGER.debug("connect(url={}, info={})", new Object[]{url, ArangoDbJdbcUtils.redactProperties(info)});
        if (!acceptsURL(url)) {
            return null;
        }
        try {
            return new ArangoDbConnection(url, info);
        } catch (Exception e) {
            throw new SQLException("Failed to connect to ArangoDB", e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // ArangoDB is not fully JDBC compliant
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger not supported");
    }
}
