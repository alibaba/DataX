package com.alibaba.datax.example.util.com.alibaba.datax.example.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.example.util.ExampleConfigParser;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;


public class ExampleConfigParserTest {


    @Test
    public void testExampleConfigParserShouldLoadDefaultConf() {

        String path = "/job/stream2stream.json";
        Configuration testConfiguration = ExampleConfigParser.parse(
                PathUtil.getAbsolutePathFromClassPath(path)
        );
        Configuration defaultConf = loadDefaultConf();
        Assert.assertEquals(testConfiguration.get("core"), defaultConf.get("core"));
        Assert.assertEquals(testConfiguration.get("common"), defaultConf.get("common"));
    }

    private Configuration loadDefaultConf() {
        return Configuration.from(
                new File(PathUtil.getAbsolutePathFromClassPath("/example/conf/core.json")
                )
        );
    }
}
