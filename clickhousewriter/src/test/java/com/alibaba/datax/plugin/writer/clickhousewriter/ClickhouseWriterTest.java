package com.alibaba.datax.plugin.writer.clickhousewriter;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * @author : donghao
 * @version : 1.0
 * @className : ClickhouseWriterTest
 * @description: TODO
 * @date : 2022-07-27 19:27
 */
public class ClickhouseWriterTest {
    ClickhouseWriter.Job job;

    @Before
    public void before() {
        job = new ClickhouseWriter.Job();
        File file = new File(
                "C:\\Users\\Mobvista\\IdeaProjects\\DataX\\src\\main\\resources\\conf\\test_ck_writer.json");

        Configuration configuration = Configuration.from(file);
        job.setPluginJobConf(configuration);
    }

    @Test
    public void jobInit() {
        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("default", conf.getString("username"));
        Assert.assertEquals("", conf.getString("password"));
        job.destroy();
    }
}
