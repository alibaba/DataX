package com.alibaba.datax.plugin.reader.obhbasereader.util;

import static com.alibaba.datax.plugin.reader.obhbasereader.enums.ModeType.MultiVersionFixedColumn;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_DATABASE;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_FULL_USER_NAME;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_ODP_ADDR;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_ODP_MODE;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_ODP_PORT;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_PARAM_URL;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_PASSWORD;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_SYS_PASSWORD;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_SYS_USER_NAME;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obhbasereader.Constant;
import com.alibaba.datax.plugin.reader.obhbasereader.HTableManager;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.obhbasereader.Key;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.ColumnType;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.ModeType;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import com.alipay.oceanbase.hbase.OHTable;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ObHbaseReaderUtil {
    private static Logger LOG = LoggerFactory.getLogger(ObHbaseReaderUtil.class);

    public static void doPretreatment(Configuration originalConfig) {
        String mode = ObHbaseReaderUtil.dealMode(originalConfig);
        originalConfig.set(Key.MODE, mode);

        String encoding = originalConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        if (!Charset.isSupported(encoding)) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, String.format("The encoding you configured is not supported by hbasereader:[%s]", encoding));
        }
        originalConfig.set(Key.ENCODING, encoding);

        // 此处增强一个检查：isBinaryRowkey 配置不能出现在与 hbaseConfig 等配置平级地位
        Boolean isBinaryRowkey = originalConfig.getBool(Key.IS_BINARY_ROWKEY);
        if (isBinaryRowkey != null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, String.format("%s cannot be configured here. It should be configured in range.", Key.IS_BINARY_ROWKEY));
        }
    }

    /**
     * 对模式以及与模式进行配对的配置进行检查
     */
    private static String dealMode(Configuration originalConfig) {
        String mode = originalConfig.getString(Key.MODE);
        ModeType modeType = ModeType.getByTypeName(mode);
        List<Map> column = originalConfig.getList(Key.COLUMN, Map.class);
        if (column == null || column.isEmpty()) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE,
                    "You have configured the normal mode to read the data in HBase, so you must configure the column in the form of:column:[{\"name\": \"cf0:column0\",\"type\": \"string\"},"
                            + "{\"name\": \"cf1:column1\",\"type\": \"long\"}]");
        }

        // 通过 parse 进行 column 格式的进一步检查
        ObHbaseReaderUtil.parseColumn(column);
        if (MultiVersionFixedColumn.equals(modeType)) {
            Integer maxVersion = originalConfig.getInt(Key.MAX_VERSION);
            Validate.notNull(maxVersion, String.format("You have configured thw mode %s to read the data in HBase, so you must configure: maxVersion", mode));

            boolean isMaxVersionValid = maxVersion == -1 || maxVersion > 1;
            Validate.isTrue(isMaxVersionValid, String.format(
                    "You have configured the mode %s to read the data in HBase, but the configured maxVersion value is wrong. maxVersion specifies that: - 1 is to read all versions, and cannot be "
                            + "configured as 0 or 1 (because 0 or 1, we think the user wants to read the data in normal mode instead of reading in mode %s, the difference is big). If it is greater "
                            + "than"
                            + " 1, it means to read the latest corresponding number of versions.",
                    mode, mode));
        }
        return mode;
    }

    /**
     * 注意：convertUserStartRowkey 和 convertInnerStartRowkey，前者会受到 isBinaryRowkey 的影响，只用于第一次对用户配置的 String 类型的 rowkey 转为二进制时使用。而后者约定：切分时得到的二进制的 rowkey 回填到配置中时采用
     */
    public static byte[] convertInnerStartRowkey(Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(startRowkey);
    }

    public static byte[] convertInnerEndRowkey(Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(endRowkey);
    }

    private static void setObHBaseConfig(com.alibaba.datax.common.util.Configuration confFile, org.apache.hadoop.conf.Configuration oHbaseConf) {

        boolean odpMode = confFile.getBool(Key.USE_ODP_MODE);
        String username = confFile.getString(Key.USERNAME);
        String password = confFile.getString(Key.PASSWORD);
        String dbName = confFile.getString(Key.DB_NAME);

//        oHbaseConf.set(RS_LIST_ACQUIRE_CONNECT_TIMEOUT.getKey(), "500");
//        oHbaseConf.set(RS_LIST_ACQUIRE_READ_TIMEOUT.getKey(), "5000");
        oHbaseConf.set(HBASE_OCEANBASE_FULL_USER_NAME, username);
        oHbaseConf.set(HBASE_OCEANBASE_PASSWORD, password);
//        oHbaseConf.set(HBASE_, META_SCANNER_CACHING);
        if (odpMode) {
            oHbaseConf.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);
            oHbaseConf.set(HBASE_OCEANBASE_DATABASE, dbName);
            oHbaseConf.set(HBASE_OCEANBASE_ODP_ADDR, confFile.getString(Key.ODP_HOST));
            oHbaseConf.setInt(HBASE_OCEANBASE_ODP_PORT, confFile.getInt(Key.ODP_PORT));
        } else {
            String clusterName = null;
            final Pattern pattern = Pattern.compile("([\\w]+)@([\\w]+)#([\\w]+)");
            Matcher matcher = pattern.matcher(username);
            if (matcher.find()) {
                clusterName = matcher.group(3);
            } else {
                throw new RuntimeException("user name is not in the correct format: user@tenant#cluster");
            }
            String configUrl = confFile.getString(Key.CONFIG_URL);
            if (!configUrl.contains("ObRegion")) {
                if (configUrl.contains("?")) {
                    configUrl += "&ObRegion=" + clusterName;
                } else {
                    configUrl += "?ObRegion=" + clusterName;
                }
            }

            if (!configUrl.contains("database")) {
                configUrl += "&database=" + dbName;
            }
            oHbaseConf.set(HBASE_OCEANBASE_PARAM_URL, configUrl);
            oHbaseConf.set(HBASE_OCEANBASE_SYS_USER_NAME, confFile.getString(Key.OB_SYS_USERNAME));
            oHbaseConf.set(HBASE_OCEANBASE_SYS_PASSWORD, confFile.getString(Key.OB_SYS_PASSWORD));
        }

        String hbaseConf = confFile.getString(Key.HBASE_CONFIG);
        Map<String, String> map = JSON.parseObject(hbaseConf, new TypeReference<Map<String, String>>() {
        });
        if (MapUtils.isNotEmpty(map)) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                oHbaseConf.set(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 每次都获取一个新的HTable  注意：HTable 本身是线程不安全的
     */
    public static OHTable initOHtable(com.alibaba.datax.common.util.Configuration configuration) {
        String tableName = configuration.getString(Key.TABLE);
        try {
            org.apache.hadoop.conf.Configuration oHbaseConf = new org.apache.hadoop.conf.Configuration();
            setObHBaseConfig(configuration, oHbaseConf);
            return HTableManager.createHTable(oHbaseConf, tableName);
        } catch (Exception e) {
            LOG.error("init ohTable error, reason: {}", e.getMessage(), e);
            throw DataXException.asDataXException(HbaseReaderErrorCode.INIT_TABLE_ERROR, e);
        }
    }

    public static boolean isRowkeyColumn(String columnName) {
        return Constant.ROWKEY_FLAG.equalsIgnoreCase(columnName);
    }

    public static String parseColumnFamily(Collection<HbaseColumnCell> hbaseColumnCells) {
        for (HbaseColumnCell columnCell : hbaseColumnCells) {
            if (ObHbaseReaderUtil.isRowkeyColumn(columnCell.getColumnName())) {
                continue;
            }
            if (columnCell.getColumnName() == null || columnCell.getColumnName().split(":").length != 2) {
                LOG.error("column cell format is unknown: {}", columnCell);
                throw new RuntimeException("Column cell format is unknown: " + columnCell);
            }
            return columnCell.getColumnName().split(":")[0];
        }
        throw new RuntimeException("parse column family failed.");
    }

    /**
     * 用于解析列配置
     */
    public static LinkedHashMap<String, HbaseColumnCell> parseColumn(List<Map> column) {
        return parseColumn(column, null, Constant.DEFAULT_ENCODING, Constant.DEFAULT_TIMEZONE);
    }

    public static LinkedHashMap<String, HbaseColumnCell> parseColumn(List<Map> column, Map<String, Column> constantMap, String encoding, String timezone) {
        LinkedHashMap<String, HbaseColumnCell> hbaseColumnCells = new LinkedHashMap<>(column.size());
        boolean cacheConstantValue = constantMap != null;
        HbaseColumnCell oneColumnCell;
        try {
            for (Map<String, String> aColumn : column) {
                ColumnType type = ColumnType.getByTypeName(aColumn.get("type"));
                boolean isRowKey = isRowkeyColumn(aColumn.get("name"));
                String columnName = isRowKey ? Constant.ROWKEY_FLAG : aColumn.get("name");

                String columnValue = aColumn.get("value");
                String dateFormat = aColumn.getOrDefault("format", Constant.DEFAULT_DATE_FORMAT);
                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue),
                        "It is either a combination of type + name + format or a combination of type + value + format. Your configuration is neither of the two. Please check and modify it.");
                if (type == ColumnType.DATE) {
                    if (StringUtils.isBlank(dateFormat)) {
                        LOG.warn("date format for {} is empty, use default date format 'yyyy-MM-dd HH:mm:ss' instead.", columnName);
                    }
                    oneColumnCell = new HbaseColumnCell.Builder(type).columnName(columnName).columnValue(columnValue).dateformat(dateFormat).build();
                } else {
                    oneColumnCell = new HbaseColumnCell.Builder(type).columnName(columnName).columnValue(columnValue).build();
                }
                hbaseColumnCells.put(columnName, oneColumnCell);
                if (cacheConstantValue && oneColumnCell.isConstant()) {
                    constantMap.put(columnName, buildColumn(columnValue, type, encoding, dateFormat, timezone));
                }
            }
            return hbaseColumnCells;
        } catch (Exception e) {
            LOG.error("parse column failed, reason:{}", e.getMessage(), e);
            throw DataXException.asDataXException(HbaseReaderErrorCode.PARSE_COLUMN_ERROR, e.getMessage());
        }
    }

    public static Column buildColumn(String columnValue, ColumnType columnType, String encoding, String dateformat, String timezone) throws Exception {
        return buildColumn(columnValue.getBytes(encoding), columnType, encoding, dateformat, timezone);
    }

    public static Column buildColumn(byte[] columnValue, ColumnType columnType, String encoding, String dateformat, String timezone) throws Exception {
        switch (columnType) {
            case BOOLEAN:
                return new BoolColumn(columnValue == null ? null : Bytes.toBoolean(columnValue));
            case SHORT:
                return new LongColumn(columnValue == null ? null : String.valueOf(Bytes.toShort(columnValue)));
            case INT:
                return new LongColumn(columnValue == null ? null : Bytes.toInt(columnValue));
            case LONG:
                return new LongColumn(columnValue == null ? null : Bytes.toLong(columnValue));
            case BYTES:
                return new BytesColumn(columnValue == null ? null : columnValue);
            case FLOAT:
                return new DoubleColumn(columnValue == null ? null : Bytes.toFloat(columnValue));
            case DOUBLE:
                return new DoubleColumn(columnValue == null ? null : Bytes.toDouble(columnValue));
            case STRING:
                return new StringColumn(columnValue == null ? null : new String(columnValue, encoding));
            case BINARY_STRING:
                return new StringColumn(columnValue == null ? null : Bytes.toStringBinary(columnValue));
            case DATE:
                String dateValue = Bytes.toStringBinary(columnValue);
                String timestamp = null;
                try {
                    long milliSec = Long.parseLong(dateValue);
                    Date date = new java.util.Date(milliSec);
                    SimpleDateFormat sdf = new java.text.SimpleDateFormat(dateformat);
                    sdf.setTimeZone(java.util.TimeZone.getTimeZone(timezone));
                    timestamp = sdf.format(date);
                } catch (Exception e) {
                    // this is already formatted timestamp
                    timestamp = dateValue;
                }
                return columnValue == null ? null : new DateColumn(DateUtils.parseDate(timestamp, dateformat));
            default:
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "obHbasereader 不支持您配置的列类型:" + columnType);
        }
    }
}
