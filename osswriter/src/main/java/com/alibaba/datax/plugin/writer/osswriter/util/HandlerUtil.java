package com.alibaba.datax.plugin.writer.osswriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.plugin.writer.osswriter.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:35
 */
public class HandlerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HandlerUtil.class);

    /**
     * 将configuration处理成 ODPS->OSS的 config
     *
     * @param jobConfiguration
     */
    public static void preHandler(Configuration jobConfiguration) {
        LOG.info("================ OssWriter Phase 1 preHandler starting... ================ ");
        Configuration writerOriginPluginConf = jobConfiguration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
        Configuration writerOssPluginConf = writerOriginPluginConf.getConfiguration(Key.OSS_CONFIG);
        Configuration newWriterPluginConf = Configuration.newDefault();
        jobConfiguration.remove(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
        //将postgresqlwriter的pg配置注入到postgresqlConfig中, 供后面的postHandler使用
        writerOriginPluginConf.remove(Key.OSS_CONFIG);
        newWriterPluginConf.set(Key.POSTGRESQL_CONFIG, writerOriginPluginConf);
        newWriterPluginConf.merge(writerOssPluginConf, true);
        //设置writer的名称为osswriter
        jobConfiguration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME, "osswriter");
        jobConfiguration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER, newWriterPluginConf);
        LOG.info("================ OssWriter Phase 1 preHandler end... ================ ");
    }
}
