package com.alibaba.datax.plugin.writer.obhbasewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by johnxu.xj on Sept 30 2018
 */
public class ConfigValidator {
    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(ConfigValidator.class);

    public static void validateParameter(com.alibaba.datax.common.util.Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.USERNAME, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, Hbase094xWriterErrorCode.REQUIRED_VALUE);
//        originalConfig.getNecessaryValue(ConfigKey.OBCONFIG_URL, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(ConfigKey.TABLE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(ConfigKey.DBNAME, Hbase094xWriterErrorCode.REQUIRED_VALUE);

        ConfigValidator.validateMode(originalConfig);

        String encoding = originalConfig.getString(ConfigKey.ENCODING, Constant.DEFAULT_ENCODING);
        if (!Charset.isSupported(encoding)) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.9", encoding));
        }
        originalConfig.set(ConfigKey.ENCODING, encoding);
    }

    public static void validateMode(com.alibaba.datax.common.util.Configuration originalConfig) {
        String mode = originalConfig.getNecessaryValue(ConfigKey.MODE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        ModeType modeType = ModeType.getByTypeName(mode);
        if (ModeType.Normal.equals(modeType)) {
            validateRowkeyColumn(originalConfig);
            validateColumn(originalConfig);
            validateVersionColumn(originalConfig);
        }

        if (originalConfig.getBool(ConfigKey.USE_ODP_MODE)) {
            originalConfig.getNecessaryValue(ConfigKey.ODP_HOST, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(ConfigKey.ODP_PORT, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        } else {
            originalConfig.getNecessaryValue(ConfigKey.OBCONFIG_URL, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(ConfigKey.OB_SYS_USER, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        }
    }

    public static void validateColumn(com.alibaba.datax.common.util.Configuration originalConfig) {
        List<Configuration> columns = originalConfig.getListConfiguration(ConfigKey.COLUMN);
        if (columns == null || columns.isEmpty()) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.11"));
        }
        for (Configuration aColumn : columns) {
            Integer index = aColumn.getInt(ConfigKey.INDEX);
            String type = aColumn.getNecessaryValue(ConfigKey.TYPE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            String name = aColumn.getNecessaryValue(ConfigKey.NAME, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if (name.split(":").length != 2) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.12", name));
            }
            if (index == null || index < 0) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.13"));
            }
        }
    }

    public static void validateRowkeyColumn(com.alibaba.datax.common.util.Configuration originalConfig) {
        List<Configuration> rowkeyColumn = originalConfig.getListConfiguration(ConfigKey.ROWKEY_COLUMN);
        if (rowkeyColumn == null || rowkeyColumn.isEmpty()) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.14"));
        }
        int rowkeyColumnSize = rowkeyColumn.size();
        //包含{"index":0,"type":"string"} 或者 {"index":-1,"type":"string","value":"_"}
        for (Configuration aRowkeyColumn : rowkeyColumn) {
            Integer index = aRowkeyColumn.getInt(ConfigKey.INDEX);
            String type = aRowkeyColumn.getNecessaryValue(ConfigKey.TYPE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if (index == null) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.15"));
            }
            //不能只有-1列,即rowkey连接串
            if (rowkeyColumnSize == 1 && index == -1) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.16"));
            }
            if (index == -1) {
                aRowkeyColumn.getNecessaryValue(ConfigKey.VALUE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            }
        }
    }

    public static void validateVersionColumn(com.alibaba.datax.common.util.Configuration originalConfig) {
        Configuration versionColumn = originalConfig.getConfiguration(ConfigKey.VERSION_COLUMN);
        //为null,表示用当前时间;指定列,需要index
        if (versionColumn != null) {
            Integer index = versionColumn.getInt(ConfigKey.INDEX);
            if (index == null) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.17"));
            }
            if (index == -1) {
                //指定时间,需要index=-1,value
                versionColumn.getNecessaryValue(ConfigKey.VALUE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            } else if (index < 0) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbase094xhelper.18"));
            }
        }
    }
}
