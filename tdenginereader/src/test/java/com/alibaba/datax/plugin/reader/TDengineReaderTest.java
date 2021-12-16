package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "\"beginDateTime\": \"2021-01-01 00:00:00\"," +
                "\"endDateTime\": \"2021-01-01 12:00:00\"," +
                "\"splitInterval\": \"1h\"" +
                "}");
        job.setPluginJobConf(configuration);
    }

    @Test
    public void jobInit() throws ParseException {
        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString("user"));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("weather", conf.getString("connection[0].table[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("connection[0].jdbcUrl"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Long start = sdf.parse("2021-01-01 00:00:00").getTime();
        Assert.assertEquals(start, conf.getLong("beginDateTime"));

        Long end = sdf.parse("2021-01-01 12:00:00").getTime();
        Assert.assertEquals(end, conf.getLong("endDateTime"));

        Assert.assertEquals(new Long(3600 * 1000), conf.getLong("splitInterval"));
    }

    @Test
    public void jobSplit() {
        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(12, configurationList.size());
        for (int i = 0; i < configurationList.size(); i++) {
            Configuration conf = configurationList.get(i);
            Assert.assertEquals("root", conf.getString("user"));
            Assert.assertEquals("taosdata", conf.getString("password"));
            Assert.assertEquals("weather", conf.getString("table[0]"));
            Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("jdbcUrl"));
        }
    }

}