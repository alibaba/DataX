package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

public class TDengineReaderTest {

    TDengineReader.Job job;

    @Before
    public void before() {
        job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"user\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":\"jdbc:TAOS-RS://master:6041/test\"}]," +
                "\"beginDateTime\": \"2021-01-01 00:00:00\"," +
                "\"endDateTime\": \"2021-01-01 10:00:00\"," +
                "\"splitInterval\": \"1h\"" +
                "}");
        job.setPluginJobConf(configuration);
    }

    @Test
    public void jobInit() {
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();
        Assert.assertEquals("select * from weather", conf.getString("sql"));
    }

    @Test
    public void jobSplit() {
        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

//        IntStream.range(0, configurationList.size()).forEach(i -> System.out.println(i + ": " + configurationList.get(i)));
    }

}