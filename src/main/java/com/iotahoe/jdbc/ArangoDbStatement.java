package com.iotahoe.jdbc;

import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.ArangoCursor;

import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ArangoDB JDBC Statement implementation.
 * This class implements the java.sql.Statement interface for ArangoDB.
 */
public class ArangoDbStatement implements Statement {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbStatement.class);
    protected final ArangoDbConnection connection;
    protected ResultSet currentResultSet;
    private boolean closed = false;
    private int maxRows = 0;
    private int queryTimeout = 0;
    private int fetchSize = 0;
    private int maxFieldSize = 0;
    private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    private int resultSetHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    
    public ArangoDbStatement(ArangoDbConnection connection) {
        LOGGER.debug("ArangoDbStatement(connection={})", new Object[]{connection});
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        LOGGER.debug("executeQuery(sql={})", sql);
        checkClosed();
        try {
            ArangoDatabase database = connection.getDatabase();
            ArangoCursor<BaseDocument> cursor = database.query(sql, BaseDocument.class, null, null );
            currentResultSet = new ArangoDbCursorResultSet<BaseDocument>(this, cursor);
            return currentResultSet;
        } catch (Exception e) {
            throw new SQLException("Failed to execute query: " + sql, e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        LOGGER.debug("executeUpdate(sql={})", sql);
        checkClosed();
        try {
            ArangoDatabase database = connection.getDatabase();
            try(ArangoCursor<Object> cursor = database.query(sql,Object.class,  null, null)){
                if (cursor.hasNext()){
                    return cursor.getCount();
                }
                return 0;
            }
        } catch (Exception e) {
            throw new SQLException("Failed to execute update: " + sql, e);
        }
    }

    @Override
    public void close() throws SQLException {
        LOGGER.debug("close()");
        if (currentResultSet != null) {
            currentResultSet.close();
        }
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        LOGGER.debug("setMaxFieldSize(max={})", max);
        checkClosed();
        this.maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        LOGGER.debug("setMaxRows(max={})", max);
        checkClosed();
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        LOGGER.debug("cancel()");
        checkClosed();
        // Not supported in ArangoDB
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setCursorName not supported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        LOGGER.debug("execute(sql={})", sql);
        checkClosed();
        try {
            ArangoDatabase database = connection.getDatabase();
            ArangoCursor<BaseDocument> cursor = database.query(sql, BaseDocument.class, null, null);
            currentResultSet = new ArangoDbCursorResultSet<BaseDocument>(this, cursor);
            return true; // Always returns a result set
        } catch (Exception e) {
            throw new SQLException("Failed to execute: " + sql, e);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return -1; // No update count available
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        return false; // ArangoDB doesn't support multiple result sets
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Batch operations not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Batch operations not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Batch operations not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("getGeneratedKeys not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        LOGGER.debug("executeUpdate(sql={}, autoGeneratedKeys={})", new Object[]{sql, autoGeneratedKeys});
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        LOGGER.debug("executeUpdate(sql={}, columnIndexes={})", new Object[]{sql, columnIndexes});
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        LOGGER.debug("executeUpdate(sql={}, columnNames={})", new Object[]{sql, columnNames});
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        LOGGER.debug("execute(sql={}, autoGeneratedKeys={})", new Object[]{sql, autoGeneratedKeys});
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        LOGGER.debug("execute(sql={}, columnIndexes={})", new Object[]{sql, columnIndexes});
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        LOGGER.debug("execute(sql={}, columnNames={})", new Object[]{sql, columnNames});
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return false;
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
    
    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
}
