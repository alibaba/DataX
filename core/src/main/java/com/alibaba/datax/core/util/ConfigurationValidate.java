package com.alibaba.datax.core.util;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang.Validate;

/**
 * Created by jingxing on 14-9-16.
 * <p>
 * 对配置文件做整体检查
 */
public class ConfigurationValidate {

    /**
     * 对配置进行校验
     * TODO 几个校验方法进行实现
     *
     * @param allConfig
     */
    public static void doValidate(Configuration allConfig) {
        Validate.isTrue(allConfig != null, "");
        coreValidate(allConfig);
        pluginValidate(allConfig);
        jobValidate(allConfig);
    }

    private static void coreValidate(Configuration allconfig) {
        return;
    }

    private static void pluginValidate(Configuration allConfig) {
        return;
    }

    private static void jobValidate(Configuration allConfig) {
        return;
    }
}
