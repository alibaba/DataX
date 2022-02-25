package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.tdenginewriter.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TDengineReaderTest {

    @Test
    public void jobInit_case01() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":\"jdbc:TAOS-RS://master:6041/test\"}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "\"beginDateTime\": \"2021-01-01 00:00:00\"," +
                "\"endDateTime\": \"2021-01-01 12:00:00\"" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString(Key.USERNAME));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("weather", conf.getString("connection[0].table[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("connection[0].jdbcUrl"));
        Assert.assertEquals("2021-01-01 00:00:00", conf.getString("beginDateTime"));
        Assert.assertEquals("2021-01-01 12:00:00", conf.getString("endDateTime"));
    }


    @Test
    public void jobInit_case02() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":\"jdbc:TAOS-RS://master:6041/test\"}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString(Key.USERNAME));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("weather", conf.getString("connection[0].table[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("connection[0].jdbcUrl"));
    }

    @Test
    public void jobSplit_case01() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":\"jdbc:TAOS-RS://master:6041/test\"}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "\"beginDateTime\": \"2021-01-01 00:00:00\"," +
                "\"endDateTime\": \"2021-01-01 12:00:00\"" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(1, configurationList.size());
        Configuration conf = configurationList.get(0);
        Assert.assertEquals("root", conf.getString("username"));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("weather", conf.getString("table[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("jdbcUrl"));

    }

    @Test
    public void jobSplit_case02() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":\"jdbc:TAOS-RS://master:6041/test\"}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(1, configurationList.size());
        Configuration conf = configurationList.get(0);
        Assert.assertEquals("root", conf.getString("username"));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("weather", conf.getString("table[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("jdbcUrl"));
    }

}