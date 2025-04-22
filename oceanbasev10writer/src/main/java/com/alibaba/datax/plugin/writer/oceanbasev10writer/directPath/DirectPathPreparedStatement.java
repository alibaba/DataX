package com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;

import static com.google.common.base.Preconditions.checkArgument;

public class DirectPathPreparedStatement extends AbstractRestrictedPreparedStatement {

    private ObDirectLoadBucket bucket;
    private final DirectPathConnection conn;
    private final Map<Integer, ObObj> parameters;
    private final Integer bufferSize;
    private static final int DEFAULT_BUFFERSIZE = 1048576;
    public static final int[] EMPTY_ARRAY = new int[0];

    /**
     * Construct a new {@link DirectPathConnection } instance.
     *
     * @param conn
     */
    public DirectPathPreparedStatement(DirectPathConnection conn) {
        this.conn = conn;
        this.parameters = new HashMap<>();
        this.bufferSize = DEFAULT_BUFFERSIZE;
        this.bucket = new ObDirectLoadBucket();
    }

    public DirectPathPreparedStatement(DirectPathConnection conn, Integer bufferSize) {
        this.conn = conn;
        this.parameters = new HashMap<>();
        this.bufferSize = bufferSize;
        this.bucket = new ObDirectLoadBucket(bufferSize);
    }

    /**
     * Return current direct path connection.
     *
     * @return DirectPathConnection
     * @throws SQLException
     */
    @Override
    public DirectPathConnection getConnection() throws SQLException {
        return this.conn;
    }

    /**
     * Copy a new row data avoid overwrite.
     *
     * @throws SQLException
     */
    @Override
    public void addBatch() throws SQLException {
        checkRange();
        ObObj[] objObjArray = new ObObj[parameters.size()];
        for (Map.Entry<Integer, ObObj> entry : parameters.entrySet()) {
            objObjArray[entry.getKey() - 1] = entry.getValue();
        }
        this.addBatch(objObjArray);
    }

    /**
     * Add a new row into buffer with input original value list.
     *
     * @param values One original row data.
     */
    public void addBatch(List<Object> values) {
        this.addBatch(createObObjArray(values));
    }

    /**
     * Add a new row into buffer with input original value array.
     *
     * @param values One original row data.
     */
    public void addBatch(Object[] values) {
        this.addBatch(createObObjArray(values));
    }

    /**
     * Add a new row into buffer with input ObObj array.
     *
     * @param arr One row data described as ObObj.
     */
    private void addBatch(ObObj[] arr) {
        checkArgument(arr != null && arr.length > 0, "Input values is null");
        try {
            this.bucket.addRow(arr);
        } catch (ObDirectLoadException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Buffered the row data in memory. (defined in the bucket)
     * You must invoke {@code ObDirectLoadBucket.clearBatch } after executeBatch.
     *
     * @return int[]
     * @throws SQLException
     */
    @Override
    public int[] executeBatch() throws SQLException {
        return this.bucket.isEmpty() ? EMPTY_ARRAY : this.conn.insert(bucket);
    }

    /**
     * Clear batch is always recreate a new {@link ObDirectLoadBucket}
     */
    @Override
    public void clearBatch() {
        this.parameters.clear();
        this.bucket = new ObDirectLoadBucket(bufferSize);
    }

    /**
     * Clear the holder parameters.
     *
     * @throws SQLException
     */
    @Override
    public void clearParameters() throws SQLException {
        this.parameters.clear();
    }

    /**
     * @return boolean
     */
    @Override
    public boolean isOracleMode() {
        return false;
    }

    /**
     * Set parameter to the target position.
     *
     * @param parameterIndex Start From 1
     * @param obObj          Convert original value to {@link ObObj }
     * @throws SQLException
     */
    @Override
    protected void setParameter(int parameterIndex, ObObj obObj) throws SQLException {
        checkArgument(parameterIndex > 0, "Parameter index should start from 1");
        this.parameters.put(parameterIndex, obObj);
    }

    /**
     * Avoid range exception:
     * <p>
     * Map.put(1, "abc");
     * Map.put(5, "def");  // Error: parameter index is 5, but 2 values exists.
     */
    private void checkRange() {
        OptionalInt optionalInt = parameters.keySet().stream().mapToInt(e -> e).max();
        int parameterIndex = optionalInt.orElseThrow(() -> new IllegalArgumentException("No parameter index found"));
        checkArgument(parameterIndex == parameters.size(), "Parameter index(%s) is unmatched with value list(%s)", parameterIndex, parameters.size());
    }
}