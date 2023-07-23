package com.alibaba.datax.core.util.container;


import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.io.File;

/**
 * @author fuyouj
 */
public class PluginLoaderFactory {

    public static ClassLoader create(Configuration configuration) {
        String path = configuration.getString("path");
        if (StringUtils.isNotBlank(path) && path.contains(File.separator)) {
            return new JarLoader(new String[]{path});
        } else {
            return new PluginClassLoader();
        }
    }
}
