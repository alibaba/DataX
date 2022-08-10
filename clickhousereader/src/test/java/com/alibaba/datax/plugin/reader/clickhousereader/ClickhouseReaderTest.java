package com.alibaba.datax.plugin.reader.clickhousereader;

import java.io.File;

import org.junit.Assert;
import org.junit.Before;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Test;

/**
 * @author : donghao
 * @version : 1.0
 * @className : com.alibaba.datax.plugin.reader.clickhousereader.ClickhouseReaderTest
 * @description: 测试类
 * @date : 2022-07-27 11:18
 */
public class ClickhouseReaderTest {

    ClickhouseReader.Job job;

    @Before
    public void before() {
        job = new ClickhouseReader.Job();
        File file = new File(
            "C:\\Users\\Mobvista\\IdeaProjects\\DataX\\src\\main\\resources\\conf\\test_ck_reader.json");

        Configuration configuration = Configuration.from(file);
        job.setPluginJobConf(configuration);
    }

    @Test
    public void jobInit() {
        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("solar_reader", conf.getString("username"));
        Assert.assertEquals("Solar123", conf.getString("password"));
        Assert.assertEquals(new Integer(10), conf.getInt("fetchSize"));

        job.destroy();
    }
}
