package com.alibaba.datax.plugin.writer.obswriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.plugin.writer.obswriter.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HandlerUtil.class);

    /**
     * 将configuration处理成 ODPS->OBS的 config
     *
     * @param jobConfiguration
     */
    public static void preHandler(Configuration jobConfiguration) {
        LOG.info("================ ObsWriter Phase 1 preHandler starting... ================ ");
        Configuration writerOriginPluginConf = jobConfiguration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
        Configuration writerObsPluginConf = writerOriginPluginConf.getConfiguration(Key.OBS_CONFIG);
        Configuration newWriterPluginConf = Configuration.newDefault();
        jobConfiguration.remove(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
        //将postgresqlwriter的pg配置注入到postgresqlConfig中, 供后面的postHandler使用
        writerOriginPluginConf.remove(Key.OBS_CONFIG);
        newWriterPluginConf.set(Key.POSTGRESQL_CONFIG, writerOriginPluginConf);
        newWriterPluginConf.merge(writerObsPluginConf, true);
        //设置writer的名称为obswriter
        jobConfiguration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME, "obswriter");
        jobConfiguration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER, newWriterPluginConf);
        LOG.info("================ ObsWriter Phase 1 preHandler end... ================ ");
    }
}
