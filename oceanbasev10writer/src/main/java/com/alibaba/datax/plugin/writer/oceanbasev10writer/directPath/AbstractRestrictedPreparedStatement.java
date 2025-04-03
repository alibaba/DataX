package com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.List;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.ObVString;
import org.apache.commons.io.IOUtils;

public abstract class AbstractRestrictedPreparedStatement implements java.sql.PreparedStatement {

    private boolean closed;

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.setParameter(parameterIndex, createObObj(null));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new UnsupportedOperationException("setNull(int, int, String) is unsupported");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("setDate(int, Date, Calendar) is unsupported");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("setTime(int, Time, Calendar) is unsupported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("setTimestamp(int, Timestamp, Calendar) is unsupported");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new UnsupportedOperationException("setObject(int, Object, int) is unsupported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("setObject(int, Object, int, int) is unsupported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException("setRef(int, Ref) is unsupported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException("setArray(int, Array) is unsupported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("setSQLXML(int, SQLXML) is unsupported");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        // if (x == null) {
        //     this.setParameter(parameterIndex, createObObj(x));
        // } else {
        //    // TODO If need BackslashEscapes and character encoding ?
        //    this.setParameter(parameterIndex, createObObj(x.toString()));
        // }
        throw new UnsupportedOperationException("setURL(int, URL) is unsupported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("setRowId(int, RowId) is unsupported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.setParameter(parameterIndex, createObObj(value));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("setBlob(int, InputStream, length) is unsupported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        this.setCharacterStream(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("setClob(int, Reader, length) is unsupported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        this.setClob(parameterIndex, (Clob) (x));
    }

    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        this.setClob(parameterIndex, x);
    }

    @Override
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("setNClob(int, Reader, length) is unsupported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("setAsciiStream(int, InputStream, length) is unsupported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("setAsciiStream(int, InputStream, length) is unsupported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("setBinaryStream(int, InputStream, length) is unsupported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("setBinaryStream(int, InputStream, length) is unsupported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException("setCharacterStream(int, InputStream, length) is unsupported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("setCharacterStream(int, InputStream, length) is unsupported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        this.setParameter(parameterIndex, createObObj(x));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("setNCharacterStream(int, InputStream, length) is unsupported");
    }

    /**
     * @return boolean
     */
    protected abstract boolean isOracleMode();

    /**
     * Set parameter to the target position.
     *
     * @param parameterIndex
     * @param obObj
     * @throws SQLException
     */
    protected abstract void setParameter(int parameterIndex, ObObj obObj) throws SQLException;

    /**
     * Close the current prepared statement.
     *
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    /**
     * Return whether the current prepared statement is closed?
     *
     * @return boolean
     * @throws SQLException
     */
    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    /**
     * Create a {@link ObObj } array with input values.
     *
     * @param values Original row value
     * @return ObObj[]
     */
    public ObObj[] createObObjArray(Object[] values) {
        if (values == null) {
            return null;
        }
        ObObj[] array = new ObObj[values.length];
        for (int i = 0; i < values.length; i++) {
            array[i] = createObObj(values[i]);
        }
        return array;
    }

    /**
     * Create a {@link ObObj } array with input values.
     *
     * @param values Original row value
     * @return ObObj[]
     */
    public ObObj[] createObObjArray(List<Object> values) {
        if (values == null) {
            return null;
        }
        ObObj[] array = new ObObj[values.size()];
        for (int i = 0; i < values.size(); i++) {
            array[i] = createObObj(values.get(i));
        }
        return array;
    }

    /**
     * Create a {@link ObObj } instance.
     *
     * @param value Original column value
     * @return ObObj
     */
    public ObObj createObObj(Object value) {
        try {
            // Only used for strongly typed declared variables
            Object convertedValue = value == null ? null : convertValue(value);
            return new ObObj(ObObjType.defaultObjMeta(convertedValue), convertedValue);
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Some values with data type is unsupported by ObObjType#valueOfType.
     * We should convert the input value to supported value data type.
     *
     * @param value
     * @return Object
     * @throws Exception
     */
    public static Object convertValue(Object value) throws Exception {
        if (value instanceof BigDecimal) {
            return value.toString();
        } else if (value instanceof BigInteger) {
            return value.toString();
        } else if (value instanceof Instant) {
            return Timestamp.from(((Instant) value));
        } else if (value instanceof LocalDate) {
            // Warn: java.sql.Date.valueOf() is deprecated. As local zone is used.
            return Date.valueOf(((LocalDate) value));
        } else if (value instanceof LocalTime) {
            // Warn: java.sql.Time.valueOf() is deprecated.
            Time t = Time.valueOf((LocalTime) value);
            return new Timestamp(t.getTime());
        } else if (value instanceof LocalDateTime) {
            return Timestamp.valueOf(((LocalDateTime) value));
        } else if (value instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) value).toInstant());
        } else if (value instanceof Time) {
            return new Timestamp(((Time) value).getTime());
        } else if (value instanceof ZonedDateTime) {
            // Note: Be care of time zone!!!
            return Timestamp.from(((ZonedDateTime) value).toInstant());
        } else if (value instanceof OffsetTime) {
            LocalTime lt = ((OffsetTime) value).toLocalTime();
            // Warn: java.sql.Time.valueOf() is deprecated.
            return new Timestamp(Time.valueOf(lt).getTime());
        } else if (value instanceof InputStream) {
            try (InputStream is = ((InputStream) value)) {
                // Note: Be care of character set!!!
                return new ObVString(IOUtils.toString(is, Charset.defaultCharset()));
            }
        } else if (value instanceof Blob) {
            Blob b = (Blob) value;
            try (InputStream is = b.getBinaryStream()) {
                if (is == null) {
                    return null;
                }
                // Note: Be care of character set!!!
                return new ObVString(IOUtils.toString(is, Charset.defaultCharset()));
            } finally {
                b.free();
            }
        } else if (value instanceof Reader) {
            try (Reader r = ((Reader) value)) {
                return IOUtils.toString(r);
            }
        } else if (value instanceof Clob) {
            Clob c = (Clob) value;
            try (Reader r = c.getCharacterStream()) {
                return r == null ? null : IOUtils.toString(r);
            } finally {
                c.free();
            }
        } else {
            return value;
        }
    }

    // *********************************************************************************** //

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("getMoreResults(int) is unsupported");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("getGeneratedKeys is unsupported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("executeUpdate(String, int) is unsupported");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("executeUpdate(String, int[]) is unsupported");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("executeUpdate(String, String[]) is unsupported");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("execute(String, int) is unsupported");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("execute(String, int[]) is unsupported");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("execute(String, String[]) is unsupported");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("getResultSetHoldability is unsupported");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("setPoolable(boolean) is unsupported");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("isPoolable is unsupported");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("closeOnCompletion is unsupported");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("isCloseOnCompletion is unsupported");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new UnsupportedOperationException("executeQuery(String) is unsupported");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException("executeUpdate(String) is unsupported");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("getMaxFieldSize is unsupported");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("setMaxFieldSize(int) is unsupported");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("getMaxRows is unsupported");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException("setMaxRows(int) is unsupported");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("setEscapeProcessing(boolean) is unsupported");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("getQueryTimeout is unsupported");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("setQueryTimeout(int) is unsupported");
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("cancel is unsupported");
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
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("setCursorName(String) is unsupported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new UnsupportedOperationException("execute(String) is unsupported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("getResultSet is unsupported");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException("getUpdateCount is unsupported");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("getMoreResults is unsupported");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("setFetchDirection(int) is unsupported");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("getFetchDirection is unsupported");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("setFetchSize(int) is unsupported");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("getFetchSize is unsupported");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("getResultSetConcurrency is unsupported");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("getResultSetType is unsupported");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("addBatch(String) is unsupported");
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throw new UnsupportedOperationException("executeQuery is unsupported");
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new UnsupportedOperationException("executeUpdate is unsupported");
    }

    @Override
    public boolean execute() throws SQLException {
        throw new UnsupportedOperationException("execute is unsupported");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException("getParameterMetaData is unsupported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("getMetaData is unsupported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("isWrapperFor(Class<T>) is unsupported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("isWrapperFor(Class<?>) is unsupported");
    }
}
