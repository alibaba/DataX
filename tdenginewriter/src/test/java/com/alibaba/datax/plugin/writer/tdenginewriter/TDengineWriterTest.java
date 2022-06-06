package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TDengineWriterTest {

    TDengineWriter.Job job;

    @Before
    public void before() {
        job = new TDengineWriter.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"column\": [\"ts\", \"f1\", \"f2\", \"t1\"]," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":\"jdbc:TAOS-RS://master:6041/test\"}]," +
                "\"batchSize\": \"1000\"" +
                "}");
        job.setPluginJobConf(configuration);
    }

    @Test
    public void jobInit() {
        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString("username"));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("connection[0].jdbcUrl"));
        Assert.assertEquals(new Integer(1000), conf.getInt("batchSize"));
        Assert.assertEquals("ts", conf.getString("column[0]"));
        Assert.assertEquals("f2", conf.getString("column[2]"));
    }

    @Test
    public void jobSplit() {
        // when
        job.init();
        List<Configuration> configurationList = job.split(10);

        // assert
        Assert.assertEquals(10, configurationList.size());
        for (Configuration conf : configurationList) {
            Assert.assertEquals("root", conf.getString("username"));
            Assert.assertEquals("taosdata", conf.getString("password"));
            Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("jdbcUrl"));
            Assert.assertEquals(new Integer(1000), conf.getInt("batchSize"));
            Assert.assertEquals("ts", conf.getString("column[0]"));
            Assert.assertEquals("f2", conf.getString("column[2]"));

        }
    }

}