package com.alibaba.datax.plugin.s3common.util;

import cn.hutool.core.util.StrUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/21
 */
public class FileSystemUtil {

    public static final Logger LOG = LoggerFactory.getLogger(FileSystemUtil.class);

    private static final String AUTHENTICATION_TYPE = "Kerberos";
    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    private static final String KEY_DEFAULT_FS = "fs.default.name";
    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    private static final String KEY_DFS_NAMESERVICES = "dfs.nameservices";
    private static final String KEY_HADOOP_USER_NAME = "hadoop.user.name";

    public static FileSystem getFileSystem(Map<String, Object> hadoopConfigMap, String defaultFs) throws Exception {
        Configuration conf = getConfiguration(hadoopConfigMap, defaultFs);
        setHadoopUserName(conf);

        return FileSystem.get(getConfiguration(hadoopConfigMap, defaultFs));
    }

    public static void setHadoopUserName(Configuration conf){
        String hadoopUserName = conf.get(KEY_HADOOP_USER_NAME);
        if(StrUtil.isEmpty(hadoopUserName)){
            return;
        }

        try {
            String previousUserName = UserGroupInformation.getLoginUser().getUserName();
            LOG.info("Hadoop user from '{}' switch to '{}' with SIMPLE auth", previousUserName, hadoopUserName);
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
            UserGroupInformation.setLoginUser(ugi);
        } catch (Exception e) {
            LOG.warn("Set hadoop user name error:", e);
        }
    }

    public static boolean isOpenKerberos(Map<String, Object> hadoopConfig){
        if(!MapUtils.getBoolean(hadoopConfig, KEY_HADOOP_SECURITY_AUTHORIZATION, false)){
            return false;
        }

        return AUTHENTICATION_TYPE.equalsIgnoreCase(MapUtils.getString(hadoopConfig, KEY_HADOOP_SECURITY_AUTHENTICATION));
    }

    public static Configuration getConfiguration(Map<String, Object> confMap, String defaultFs) {
        confMap = fillConfig(confMap, defaultFs);

        Configuration conf = new Configuration();
        confMap.forEach((key, val) -> {
            if(val != null){
                conf.set(key, val.toString());
            }
        });

        return conf;
    }

    public static JobConf getJobConf(Map<String, Object> confMap, String defaultFs){
        confMap = fillConfig(confMap, defaultFs);

        JobConf jobConf = new JobConf();
        confMap.forEach((key, val) -> {
            if(val != null){
                jobConf.set(key, val.toString());
            }
        });

        return jobConf;
    }

    private static Map<String, Object> fillConfig(Map<String, Object> confMap, String defaultFs) {
        if (confMap == null) {
            confMap = new HashMap<>();
        }

        if (isHaMode(confMap)) {
            if(defaultFs != null){
                confMap.put(KEY_HA_DEFAULT_FS, defaultFs);
            }
        } else {
            if(defaultFs != null){
                confMap.put(KEY_DEFAULT_FS, defaultFs);
            }
        }

        confMap.put(KEY_FS_HDFS_IMPL_DISABLE_CACHE, "true");
        return confMap;
    }

    private static boolean isHaMode(Map<String, Object> confMap){
        return StrUtil.isNotEmpty(MapUtils.getString(confMap, KEY_DFS_NAMESERVICES));
    }
}
