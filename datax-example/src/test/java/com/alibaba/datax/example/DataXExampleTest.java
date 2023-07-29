package com.alibaba.datax.example;


import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.example.util.ExampleConfigParser;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;


public class DataXExampleTest extends ExampleTestTemplate {

    @Test
    public void testStreamReader2StreamWriter() {

        String path = "/job/stream2stream.json";
        Configuration testConfiguration = ExampleConfigParser.parse(
                PathUtil.getAbsolutePathFromClassPath(path)
        );
        Engine engine = new Engine();
        engine.start(testConfiguration);
    }

}
