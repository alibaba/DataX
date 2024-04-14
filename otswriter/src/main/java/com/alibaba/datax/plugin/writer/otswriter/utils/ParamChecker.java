package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage.*;


public class ParamChecker {

    private static void throwNotExistException(String key) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.MISSING_PARAMTER_ERROR, key));
    }

    private static void throwStringLengthZeroException(String key) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.PARAMTER_STRING_IS_EMPTY_ERROR, key));
    }

    private static void throwEmptyListException(String key) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.PARAMETER_LIST_IS_EMPTY_ERROR, key));
    }

    private static void throwNotListException(String key, Throwable t) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.PARAMETER_IS_NOT_ARRAY_ERROR, key), t);
    }

    public static String checkStringAndGet(Configuration param, String key) {
        String value = param.getString(key);
        value = value != null ? value.trim() : null;
        if (null == value) {
            throwNotExistException(key);
        } else if (value.length() == 0) {
            throwStringLengthZeroException(key);
        }
        return value;
    }

    public static List<Object> checkListAndGet(Configuration param, String key, boolean isCheckEmpty) {
        List<Object> value = null;
        try {
            value = param.getList(key);
        } catch (ClassCastException e) {
            throwNotListException(key, e);
        }
        if (null == value) {
            throwNotExistException(key);
        } else if (isCheckEmpty && value.isEmpty()) {
            throwEmptyListException(key);
        }
        return value;
    }
    
    public static void checkPrimaryKey(TableMeta meta, List<PrimaryKeySchema> pk) {
        Map<String, PrimaryKeyType> pkNameAndTypeMapping = meta.getPrimaryKeyMap();
        // 个数是否相等
        if (pkNameAndTypeMapping.size() != pk.size()) {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.INPUT_PK_COUNT_NOT_EQUAL_META_ERROR, pk.size(), pkNameAndTypeMapping.size()));
        }
        
        // 名字类型是否相等
        for (PrimaryKeySchema col : pk) {
            PrimaryKeyType type = pkNameAndTypeMapping.get(col.getName());
            if (type == null) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_MISSING_ERROR, col.getName()));
            }
            if (type != col.getType()) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.INPUT_PK_TYPE_NOT_MATCH_META_ERROR, col.getName(), type, col.getType()));
            }
        }
    }

    public static void checkVersion(OTSConf conf) {
        /**
         * conf检查遵循以下规则
         *  1. 旧版本插件 不支持 主键自增列
         *  2. 旧版本插件 不支持 多版本模式
         *  3. 多版本模式 不支持 主键自增列
         *  4. 旧版本插件 不支持 时序数据表
         *  5. 时序数据表 不支持 主键自增列
         */
        if (!conf.isNewVersion() && conf.getEnableAutoIncrement()) {
            throw new IllegalArgumentException(PUBLIC_SDK_NO_SUPPORT_AUTO_INCREMENT);
        }
        if (!conf.isNewVersion() && conf.getMode() == OTSMode.MULTI_VERSION) {
            throw new IllegalArgumentException(PUBLIC_SDK_NO_SUPPORT_MULTI_VERSION);
        }
        if (conf.getMode() == OTSMode.MULTI_VERSION && conf.getEnableAutoIncrement()) {
            throw new IllegalArgumentException(NOT_SUPPORT_MULTI_VERSION_AUTO_INCREMENT);
        }
        if (!conf.isNewVersion() && conf.isTimeseriesTable()) {
            throw new IllegalArgumentException(PUBLIC_SDK_NO_SUPPORT_TIMESERIES_TABLE);
        }
        if (conf.isTimeseriesTable() && conf.getEnableAutoIncrement()) {
            throw new IllegalArgumentException(NOT_SUPPORT_TIMESERIES_TABLE_AUTO_INCREMENT);
        }
    }

    public static void checkPrimaryKeyWithAutoIncrement(TableMeta meta, List<PrimaryKeySchema> pk) {
        Map<String, PrimaryKeyType> pkNameAndTypeMapping = meta.getPrimaryKeyMap();
        int autoIncrementKeySize = 0;
        for(PrimaryKeySchema p : meta.getPrimaryKeyList()){
            if(p.hasOption()){
                autoIncrementKeySize++;
            }
        }
        // 个数是否相等
        if (pkNameAndTypeMapping.size() != pk.size() + autoIncrementKeySize) {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.INPUT_PK_COUNT_NOT_EQUAL_META_ERROR, pk.size() + autoIncrementKeySize, pkNameAndTypeMapping.size()));
        }

        // 名字类型是否相等
        for (PrimaryKeySchema col : pk) {
            if(col.hasOption()){
                continue;
            }
            PrimaryKeyType type = pkNameAndTypeMapping.get(col.getName());
            if (type == null) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_MISSING_ERROR, col.getName()));
            }
            if (type != col.getType()) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.INPUT_PK_TYPE_NOT_MATCH_META_ERROR, col.getName(), type, col.getType()));
            }
        }
    }
    
    public static void checkAttribute(List<OTSAttrColumn> attr) {
        // 检查重复列
        Set<String> names = new HashSet<String>();
        for (OTSAttrColumn col : attr) {
            if (names.contains(col.getName())) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.ATTR_REPEAT_COLUMN_ERROR, col.getName()));
            } else {
                names.add(col.getName());
            }
        }
    }

    public static TimeUnit checkTimeUnitAndGet(String str) {
        if (null == str) {
            return null;
        } else if ("NANOSECONDS".equalsIgnoreCase(str)) {
            return TimeUnit.NANOSECONDS;
        } else if ("MICROSECONDS".equalsIgnoreCase(str)) {
            return TimeUnit.MICROSECONDS;
        } else if ("MILLISECONDS".equalsIgnoreCase(str)) {
            return TimeUnit.MILLISECONDS;
        } else if ("SECONDS".equalsIgnoreCase(str)) {
            return TimeUnit.SECONDS;
        } else if ("MINUTES".equalsIgnoreCase(str)) {
            return TimeUnit.MINUTES;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.TIMEUNIT_FORMAT_ERROR, str));
        }
    }

}
