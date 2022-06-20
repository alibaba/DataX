package com.leehom.arch.datax.plugin.rdb2graph.writer.neo4jwriter;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @类名: Key
 * @说明: 配置项
 *
 * @author   leehom
 * @Date	 2022年4月27日 下午2:49:13
 * 修改记录：
 *
 * @see 	 
 */
public final class Key {
	
    public static String database(Configuration conf) {
        return conf.getString("database", "");
    }

    public static String schemaUri(Configuration conf) {
        return conf.getString("schemaUri", "");
    }
    
    public static String uri(Configuration conf) {
        return conf.getString("uri", "");
    }

    public static String userName(Configuration conf) {
        return conf.getString("username", "");
    }

    public static String password(Configuration conf) {
        return conf.getString("password", "");
    }

    public static int batchSize(Configuration conf) {
        return conf.getInt("batchSize", 1000);
    }

    public static int getTrySize(Configuration conf) {
        return conf.getInt("trySize", 30);
    }

    public static int getTimeout(Configuration conf) {
        return  conf.getInt("timeout", 600000);
    }

    public static boolean isCleanup(Configuration conf) {
        return conf.getBool("cleanup", false);
    }

    public static boolean isDiscovery(Configuration conf) {
        return conf.getBool("discovery", false);
    }

    public static boolean isCompression(Configuration conf) {
        return conf.getBool("compression", true);
    }

    public static boolean isMultiThread(Configuration conf) {
        return conf.getBool("multiThread", true);
    }

    public static String getIndexName(Configuration conf) {
        return conf.getNecessaryValue("index", Neo4jWriterErrorCode.BAD_CONFIG_VALUE);
    }

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
}
