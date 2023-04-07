package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Key {
    // ----------------------------------------
    //  类型定义 主键字段定义
    // ----------------------------------------
    public static final String PRIMARY_KEY_COLUMN_NAME = "pk";

    public static enum ActionType {
        UNKONW,
        INDEX,
        CREATE,
        DELETE,
        UPDATE
    }

    public static ActionType getActionType(Configuration conf) {
        String actionType = conf.getString("actionType", "index");
        if ("index".equals(actionType)) {
            return ActionType.INDEX;
        } else if ("create".equals(actionType)) {
            return ActionType.CREATE;
        } else if ("delete".equals(actionType)) {
            return ActionType.DELETE;
        } else if ("update".equals(actionType)) {
            return ActionType.UPDATE;
        } else {
            return ActionType.UNKONW;
        }
    }


    public static String getEndpoint(Configuration conf) {
        return conf.getNecessaryValue("endpoint", ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE);
    }

    public static String getUsername(Configuration conf) {
        return conf.getString("username", conf.getString("accessId"));
    }

    public static String getPassword(Configuration conf) {
        return conf.getString("password", conf.getString("accessKey"));
    }

    public static int getBatchSize(Configuration conf) {
        return conf.getInt("batchSize", 1024);
    }

    public static int getTrySize(Configuration conf) {
        return conf.getInt("trySize", 30);
    }

    public static long getTryInterval(Configuration conf) {
        return conf.getLong("tryInterval", 60000L);
    }

    public static int getTimeout(Configuration conf) {
        return  conf.getInt("timeout", 600000);
    }

    public static boolean isTruncate(Configuration conf) {
        return conf.getBool("truncate", conf.getBool("cleanup", false));
    }

    public static boolean isDiscovery(Configuration conf) {
        return conf.getBool("discovery", false);
    }

    public static boolean isCompression(Configuration conf) {
        return conf.getBool("compress", conf.getBool("compression", true));
    }

    public static boolean isMultiThread(Configuration conf) {
        return conf.getBool("multiThread", true);
    }

    public static String getIndexName(Configuration conf) {
        return conf.getNecessaryValue("index", ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE);
    }

    public static String getDeleteBy(Configuration conf) {
        return conf.getString("deleteBy");
    }

    
    /**
     * TODO: 在7.0开始，一个索引只能建一个Type为_doc
     * */
    public static String getTypeName(Configuration conf) {
        String indexType = conf.getString("indexType");
        if(StringUtils.isBlank(indexType)){
            indexType = conf.getString("type", getIndexName(conf));
        }
        return indexType;
    }


    public static boolean isIgnoreWriteError(Configuration conf) {
        return conf.getBool("ignoreWriteError", false);
    }

    public static boolean isIgnoreParseError(Configuration conf) {
        return conf.getBool("ignoreParseError", true);
    }


    public static boolean isHighSpeedMode(Configuration conf) {
        if ("highspeed".equals(conf.getString("mode", ""))) {
            return  true;
        }
        return false;
    }

    public static String getAlias(Configuration conf) {
        return conf.getString("alias", "");
    }

    public static boolean isNeedCleanAlias(Configuration conf) {
        String mode = conf.getString("aliasMode", "append");
        if ("exclusive".equals(mode)) {
            return true;
        }
        return false;
    }

    public static Map<String, Object> getSettings(Configuration conf) {
        return conf.getMap("settings", new HashMap<String, Object>());
    }

    public static String getSplitter(Configuration conf) {
        return conf.getString("splitter", "-,-");
    }

    public static boolean getDynamic(Configuration conf) {
        return conf.getBool("dynamic", false);
    }

    public static String getDstDynamic(Configuration conf) {
        return conf.getString("dstDynamic");
    }

    public static String getDiscoveryFilter(Configuration conf){
        return conf.getString("discoveryFilter","_all");
    }

    public static Boolean getVersioning(Configuration conf) {
        return conf.getBool("versioning", false);
    }

    public static Long getUnifiedVersion(Configuration conf) {
        return conf.getLong("version", System.currentTimeMillis());
    }

    public static Map<String, Object> getUrlParams(Configuration conf) {
        return conf.getMap("urlParams", new HashMap<String, Object>());
    }

    public static Integer getESVersion(Configuration conf) {
        return conf.getInt("esVersion");
    }
    
    public static String getMasterTimeout(Configuration conf) {
        return conf.getString("masterTimeout", "5m");
    }
    
    public static boolean isEnableNullUpdate(Configuration conf) {
        return conf.getBool("enableWriteNull", true);
    }
    
    public static String getFieldDelimiter(Configuration conf) {
        return conf.getString("fieldDelimiter", "");
    }
    
    public static PrimaryKeyInfo getPrimaryKeyInfo(Configuration conf) {
        String primaryKeyInfoString = conf.getString("primaryKeyInfo");
        if (StringUtils.isNotBlank(primaryKeyInfoString)) {
            return JSON.parseObject(primaryKeyInfoString, new TypeReference<PrimaryKeyInfo>() {});
        } else {
            return null;
        }
    }
    
    public static List<PartitionColumn> getEsPartitionColumn(Configuration conf) {
        String esPartitionColumnString = conf.getString("esPartitionColumn");
        if (StringUtils.isNotBlank(esPartitionColumnString)) {
            return JSON.parseObject(esPartitionColumnString, new TypeReference<List<PartitionColumn>>() {});
        } else {
            return null;
        }
    }
}
