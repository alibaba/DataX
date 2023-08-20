package com.alibaba.datax.example;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.example.util.ExampleConfigParser;

/**
 * {@code Date} 2023/8/6 11:22
 *
 * @author fuyouj
 */

public class ExampleContainer {
    /**
     * example对外暴露的启动入口
     * 使用前最好看下 datax-example/doc/README.MD
     * @param jobPath 任务json绝对路径
     */
    public static void start(String jobPath) {

        Configuration configuration = ExampleConfigParser.parse(jobPath);

        Engine engine = new Engine();
        engine.start(configuration);
    }
}
