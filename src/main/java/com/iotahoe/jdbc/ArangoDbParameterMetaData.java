package com.iotahoe.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * ArangoDB JDBC ParameterMetaData implementation.
 * This class implements the java.sql.ParameterMetaData interface for ArangoDB.
 */
public class ArangoDbParameterMetaData implements ParameterMetaData {
    
    private final int parameterCount;
    
    public ArangoDbParameterMetaData(int parameterCount) {
        this.parameterCount = parameterCount;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        checkParameterIndex(param);
        return parameterNullable;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        checkParameterIndex(param);
        return false; // Default to not signed
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        checkParameterIndex(param);
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        checkParameterIndex(param);
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        checkParameterIndex(param);
        return Types.VARCHAR; // Default to VARCHAR
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        checkParameterIndex(param);
        return "VARCHAR";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        checkParameterIndex(param);
        return "java.lang.String";
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        checkParameterIndex(param);
        return parameterModeIn;
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
    
    private void checkParameterIndex(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw new SQLException("Parameter index out of range: " + param);
        }
    }
}
