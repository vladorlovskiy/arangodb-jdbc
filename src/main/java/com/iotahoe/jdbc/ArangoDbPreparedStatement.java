package com.iotahoe.jdbc;

import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.ArangoCursor;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ArangoDB JDBC PreparedStatement implementation.
 * This class implements the java.sql.PreparedStatement interface for ArangoDB.
 * ArangoDB only supports named parameters (@paramName), so positional setter methods
 * are mapped to named parameters based on their order in the query.
 */
public class ArangoDbPreparedStatement extends ArangoDbStatement implements PreparedStatement {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbPreparedStatement.class);
    private final String queryText;
    private final Map<String, Object> namedParameters;
    private final List<String> parameterOrder; // Order of parameters as they appear in the query
    private final Map<Integer, String> indexToParameterName; // Maps parameter index to parameter name
    
    // Pattern to match named parameters like @paramName
    private static final Pattern NAMED_PARAMETER_PATTERN = Pattern.compile("@([a-zA-Z_][a-zA-Z0-9_]*)");
    
    public ArangoDbPreparedStatement(ArangoDbConnection connection, String queryText) {
        super(connection);
        LOGGER.debug("ArangoDbPreparedStatement(connection={}, queryText={})", new Object[]{connection, queryText});
        this.queryText = queryText;
        this.namedParameters = new HashMap<>();
        this.parameterOrder = extractParameterOrder(queryText);
        this.indexToParameterName = createIndexMapping();
    }
    
    /**
     * Extracts parameter names in the order they appear in the query.
     */
    private List<String> extractParameterOrder(String sql) {
        List<String> order = new ArrayList<>();
        Matcher matcher = NAMED_PARAMETER_PATTERN.matcher(sql);
        while (matcher.find()) {
            String paramName = matcher.group(1);
            if (!order.contains(paramName)) { // Avoid duplicates
                order.add(paramName);
            }
        }
        return order;
    }
    
    /**
     * Creates a mapping from parameter index (1-based) to parameter name.
     */
    private Map<Integer, String> createIndexMapping() {
        Map<Integer, String> mapping = new HashMap<>();
        for (int i = 0; i < parameterOrder.size(); i++) {
            mapping.put(i + 1, parameterOrder.get(i)); // 1-based indexing
        }
        return mapping;
    }

    /**
     * Validates that all required parameters are set.
     * @throws SQLException if any parameter is missing
     */
    private void validateParameters() throws SQLException {
        // Check that all required parameters are set
        for (String paramName : parameterOrder) {
            if (!namedParameters.containsKey(paramName)) {
                throw new SQLException("Parameter '" + paramName + "' is not set");
            }
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        LOGGER.debug("executeQuery()");
        checkClosed();
        try {
            validateParameters();
            ArangoDatabase database = connection.getDatabase();
            ArangoCursor<BaseDocument> cursor = database.query(queryText, BaseDocument.class, namedParameters, null);
            currentResultSet = new ArangoDbCursorResultSet<BaseDocument>(this, cursor);
            return currentResultSet;
        } catch (Exception e) {
            throw new SQLException("Failed to execute prepared query", e);
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        LOGGER.debug("executeUpdate()");
        checkClosed();
        try {
            validateParameters();
            ArangoDatabase database = connection.getDatabase();
            try(ArangoCursor<BaseDocument> cursor = database.query(queryText, BaseDocument.class, namedParameters, null)){
                if (cursor.hasNext()){
                    return cursor.getCount();
                }
                return 0;
            }
        } catch (Exception e) {
            throw new SQLException("Failed to execute prepared update", e);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        LOGGER.debug("execute()");
        checkClosed();
        try {
            validateParameters();
            ArangoDatabase database = connection.getDatabase();
            try(ArangoCursor<BaseDocument> cursor = database.query(queryText, BaseDocument.class, namedParameters, null)){}
            return true;
        } catch (Exception e) {
            throw new SQLException("Failed to execute prepared statement", e);
        }
    }

    /**
     * Sets a named parameter value.
     * @param parameterName the name of the parameter (without @)
     * @param value the parameter value
     * @throws SQLException if the parameter name is not found in the query
     */
    public void setParameter(String parameterName, Object value) throws SQLException {
        LOGGER.debug("setParameter(parameterName={}, value={})", new Object[]{parameterName, value});
        checkClosed();
        if (!parameterOrder.contains(parameterName)) {
            throw new SQLException("Parameter '" + parameterName + "' not found in query");
        }
        namedParameters.put(parameterName, value);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        LOGGER.debug("setNull(parameterIndex={}, sqlType={})", new Object[]{parameterIndex, sqlType});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        LOGGER.debug("setBoolean(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        LOGGER.debug("setByte(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        LOGGER.debug("setShort(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        LOGGER.debug("setInt(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        LOGGER.debug("setLong(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        LOGGER.debug("setFloat(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        LOGGER.debug("setDouble(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        LOGGER.debug("setBigDecimal(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        LOGGER.debug("setString(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        LOGGER.debug("setBytes(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        LOGGER.debug("setDate(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        LOGGER.debug("setTime(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        LOGGER.debug("setTimestamp(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setUnicodeStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    /**
     * Gets the parameter name for a given parameter index.
     * @param parameterIndex the 1-based parameter index
     * @return the parameter name
     * @throws SQLException if the parameter index is out of range
     */
    private String getParameterName(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > parameterOrder.size()) {
            throw new SQLException("Parameter index out of range: " + parameterIndex + 
                                 ". Query has " + parameterOrder.size() + " parameters.");
        }
        return indexToParameterName.get(parameterIndex);
    }

    @Override
    public void clearParameters() throws SQLException {
        LOGGER.debug("clearParameters()");
        checkClosed();
        namedParameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        LOGGER.debug("setObject(parameterIndex={}, x={}, targetSqlType={})", new Object[]{parameterIndex, x, targetSqlType});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        LOGGER.debug("setObject(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Batch operations not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setRef not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setArray not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        if (currentResultSet != null) {
            return currentResultSet.getMetaData();
        }
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        LOGGER.debug("setURL(parameterIndex={}, x={})", new Object[]{parameterIndex, x});
        checkClosed();
        String paramName = getParameterName(parameterIndex);
        namedParameters.put(paramName, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return new ArangoDbParameterMetaData(parameterOrder.size());
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setRowId not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setSQLXML not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }
    
}
