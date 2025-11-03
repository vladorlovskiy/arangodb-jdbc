package com.iotahoe.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple ResultSet implementation that works with a list of Maps.
 * This is a more straightforward implementation for basic JDBC operations.
 */
class ArangoDbListResultSet<T extends Object> implements ResultSet {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbListResultSet.class);
    private final Statement statement;
    private final List<Map<String, T>> data;
    private final String[] columnNames;
    private final int[] columnTypes;
    
    private int currentRow = -1;
    private boolean closed = false;
    private boolean wasNull = false;
    
    public ArangoDbListResultSet(Statement statement, List<Map<String, T>> data) {
        LOGGER.debug("ArangoDbListResultSet(statement={}, data.size={})", new Object[]{statement, data != null ? data.size() : 0});
        this.statement = statement;
        this.data = data != null ? data : new ArrayList<>();
        
        // Extract column names and types from first row
        if (!this.data.isEmpty()) {
            Map<String, T> firstRow = this.data.get(0);
            this.columnNames = firstRow.keySet().toArray(new String[0]);
            this.columnTypes = new int[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                T value = firstRow.get(columnNames[i]);
                this.columnTypes[i] = ArangoDbJdbcUtils.getSqlType(value);
            }
        } else {
            this.columnNames = new String[0];
            this.columnTypes = new int[0];
        }
    }
    
    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (currentRow + 1 < data.size()) {
            currentRow++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asString(value);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asBoolean(value);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asByte(value);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asShort(value);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asInt(value);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asLong(value);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asFloat(value);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asDouble(value);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asBigDecimal(value);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asBytes(value);
    }

    @Override
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asDate(value);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asTime(value);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return ArangoDbJdbcUtils.asTimestamp(value);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public java.sql.Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new ArangoDbListResultSetMetaData(columnNames, columnTypes);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        LOGGER.debug("getObject(columnIndex={})", columnIndex);
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        return value;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currentRow < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return currentRow >= data.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currentRow == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currentRow == data.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        LOGGER.debug("beforeFirst()");
        checkClosed();
        currentRow = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        LOGGER.debug("afterLast()");
        checkClosed();
        currentRow = data.size();
    }

    @Override
    public boolean first() throws SQLException {
        LOGGER.debug("first()");
        checkClosed();
        if (data.isEmpty()) return false;
        currentRow = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        LOGGER.debug("last()");
        checkClosed();
        if (data.isEmpty()) return false;
        currentRow = data.size() - 1;
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRow + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        LOGGER.debug("absolute(row={})", row);
        checkClosed();
        if (row < 0) {
            currentRow = data.size() + row;
        } else {
            currentRow = row - 1;
        }
        return currentRow >= 0 && currentRow < data.size();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        LOGGER.debug("relative(rows={})", rows);
        checkClosed();
        currentRow += rows;
        return currentRow >= 0 && currentRow < data.size();
    }

    @Override
    public boolean previous() throws SQLException {
        LOGGER.debug("previous()");
        checkClosed();
        if (currentRow > 0) {
            currentRow--;
            return true;
        }
        return false;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public <R> R getObject(int columnIndex, Class<R> type) throws SQLException {
        LOGGER.debug("getObject(columnIndex={}, type={})", new Object[]{columnIndex, type});
        Object value = getValue(columnIndex);
        wasNull = (value == null);
        if (value == null) return null;
        if (type.isAssignableFrom(value.getClass())) {
            return type.cast(value);
        }
        return null;
    }

    @Override
    public <R> R getObject(String columnLabel, Class<R> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public <R> R unwrap(Class<R> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
    
    private Object getValue(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow < 0 || currentRow >= data.size()) {
            throw new SQLException("No current row");
        }
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        Map<String, T> row = data.get(currentRow);
        return row.get(columnNames[columnIndex - 1]);
    }
    
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }

    // Implement all other required methods with basic implementations
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream not supported");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
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
    public String getCursorName() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(columnIndex, 0);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        // Not supported
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    // All other methods throw SQLFeatureNotSupportedException
    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowUpdated not supported");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowInserted not supported");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowDeleted not supported");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, java.sql.Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    // All other methods throw SQLFeatureNotSupportedException
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public java.sql.Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL not supported");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    // All remaining methods throw SQLFeatureNotSupportedException
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(findColumn(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(findColumn(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(findColumn(columnLabel), x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(findColumn(columnLabel), x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateNString(findColumn(columnLabel), nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        updateNClob(findColumn(columnLabel), nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }
}
