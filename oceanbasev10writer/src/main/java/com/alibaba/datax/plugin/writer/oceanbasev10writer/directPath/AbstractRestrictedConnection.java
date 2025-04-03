package com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public abstract class AbstractRestrictedConnection implements java.sql.Connection {

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("prepareCall(String) is unsupported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("nativeSQL(String) is unsupported");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new UnsupportedOperationException("setAutoCommit(boolean) is unsupported");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw new UnsupportedOperationException("getAutoCommit is unsupported");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException("abort(Executor) is unsupported");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException("setNetworkTimeout(Executor, int) is unsupported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException("getNetworkTimeout is unsupported");
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("getMetaData is unsupported");
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException("setReadOnly(boolean) is unsupported");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException("isReadOnly is unsupported");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("setCatalog(String) is unsupported");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("getCatalog is unsupported");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException("setTransactionIsolation(int) is unsupported");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException("getTransactionIsolation is unsupported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("getWarnings is unsupported");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("clearWarnings is unsupported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("createStatement(int, int) is unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("prepareStatement(String, int, int) is unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("prepareCall(String, int, int) is unsupported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("getTypeMap is unsupported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("setTypeMap(Map<String, Class<?>>) is unsupported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException("setHoldability is unsupported");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("getHoldability is unsupported");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("setSavepoint is unsupported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("setSavepoint(String) is unsupported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("rollback(Savepoint) is unsupported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("releaseSavepoint(Savepoint) is unsupported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("createStatement(int, int, int) is unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("prepareStatement(String, int, int, int) is unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("prepareCall(String, int, int, int) is unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("prepareStatement(String, int) is unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("prepareStatement(String, int[]) is unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("prepareStatement(String, String[]) is unsupported");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException("createClob is unsupported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException("createBlob is unsupported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException("createNClob is unsupported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException("createSQLXML is unsupported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException("isValid(int) is unsupported");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("setClientInfo(String, String) is unsupported");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("setClientInfo(Properties) is unsupported");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException("getClientInfo(String) is unsupported");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException("getClientInfo is unsupported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException("createArrayOf(String, Object[]) is unsupported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("createStruct(String, Object[]) is unsupported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("setSchema(String) is unsupported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("unwrap(Class<T>) is unsupported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("isWrapperFor(Class<?>) is unsupported");
    }
}