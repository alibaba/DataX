package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.influxdbreader.InfluxDBReader;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class InfluxDBReaderTest {
    @Test
    public void jobInitCase01() {
        InfluxDBReader.Job job = new InfluxDBReader.Job();

        Configuration configuration = Configuration.from("{" +
                "\"connection\": [{\"url\":\"http://172.20.48.111:8086\",\"token\":\"ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==\",\"org\":\"tsdb\",\"bucket\":\"shen\"}]," +
                "\"splitIntervalH\":24," +
                "\"beginDateTime\": \"2022-08-20 00:00:00\"," +
                "\"endDateTime\": \"2022-09-23 00:00:00\"" +
                "}");

        job.setPluginJobConf(configuration);

        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("http://172.20.48.111:8086", conf.getString("connection[0].url"));
        Assert.assertEquals("ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==", conf.getString("connection[0].token"));
        Assert.assertEquals("tsdb", conf.getString("connection[0].org"));
        Assert.assertEquals("shen", conf.getString("connection[0].bucket"));
        Assert.assertEquals("24", conf.getString("splitIntervalH"));
        Assert.assertEquals("2022-08-20 00:00:00", conf.getString("beginDateTime"));
        Assert.assertEquals("2022-09-23 00:00:00", conf.getString("endDateTime"));
    }

    @Test
    public void jobSplitCase01() {
        InfluxDBReader.Job job = new InfluxDBReader.Job();

        Configuration configuration = Configuration.from("{" +
                "\"connection\": [{\"url\":\"http://172.20.48.111:8086\",\"token\":\"ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==\",\"org\":\"tsdb\",\"bucket\":\"shen\"}]," +
                "\"splitIntervalH\":24," +
                "\"beginDateTime\": \"2022-09-20 00:00:00\"," +
                "\"endDateTime\": \"2022-09-23 00:00:00\"" +
                "}");

        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(3, configurationList.size());
        Configuration conf = configurationList.get(0);
        Assert.assertEquals("24", conf.getString("splitIntervalH"));
    }

    @Test
    public void jobSplitCase02() {
        InfluxDBReader.Job job = new InfluxDBReader.Job();

        Configuration configuration = Configuration.from("{" +
                "\"connection\": [{\"url\":\"http://172.20.48.111:8086\",\"token\":\"ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==\",\"org\":\"tsdb\",\"bucket\":\"shen\"}]," +
                "\"splitIntervalH\":48," +
                "\"beginDateTime\": \"2022-09-20 00:00:00\"," +
                "\"endDateTime\": \"2022-09-23 00:00:00\"" +
                "}");

        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(2, configurationList.size());
        Configuration conf = configurationList.get(0);
        Assert.assertEquals("48", conf.getString("splitIntervalH"));
    }

    @Test
    public void taskInitCase01() {
        InfluxDBReader.Task task = new InfluxDBReader.Task();

        Configuration readerSliceConfig = Configuration.from("{" +
                "\"connection\": [{\"url\":\"http://172.20.48.111:8086\",\"token\":\"ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==\",\"org\":\"tsdb\",\"bucket\":\"shen\"}]," +
                "\"splitIntervalH\":24," +
                "\"beginDateTime\": \"2022-09-20 00:00:00\"," +
                "\"endDateTime\": \"2022-09-21 00:00:00\"" +
                "}");

        task.setPluginJobConf(readerSliceConfig);

        task.init();

        // assert
        Configuration conf = task.getPluginJobConf();

        Assert.assertEquals("http://172.20.48.111:8086", conf.getString("connection[0].url"));
        Assert.assertEquals("ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==", conf.getString("connection[0].token"));
        Assert.assertEquals("tsdb", conf.getString("connection[0].org"));
        Assert.assertEquals("shen", conf.getString("connection[0].bucket"));
        Assert.assertEquals("24", conf.getString("splitIntervalH"));
        Assert.assertEquals("2022-09-20 00:00:00", conf.getString("beginDateTime"));
        Assert.assertEquals("2022-09-21 00:00:00", conf.getString("endDateTime"));
    }

    // TODO 没想好怎么写，目前是 Debug 看看
    @Test
    public void taskPrepareCase01() {
        InfluxDBReader.Task task = new InfluxDBReader.Task();

        Configuration readerSliceConfig = Configuration.from("{" +
                "\"connection\": [{\"url\":\"http://172.20.48.111:8086\",\"token\":\"ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==\",\"org\":\"tsdb\",\"bucket\":\"shen\"}]," +
                "\"splitIntervalH\":24," +
                "\"beginDateTime\": \"2022-09-20 00:00:00\"," +
                "\"endDateTime\": \"2022-09-21 00:00:00\"" +
                "}");

        task.setPluginJobConf(readerSliceConfig);

        task.init();
        task.prepare();

        // assert
        Configuration conf = task.getPluginJobConf();

        Assert.assertEquals("http://172.20.48.111:8086", conf.getString("connection[0].url"));
        Assert.assertEquals("ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==", conf.getString("connection[0].token"));
        Assert.assertEquals("tsdb", conf.getString("connection[0].org"));
        Assert.assertEquals("shen", conf.getString("connection[0].bucket"));
        Assert.assertEquals("24", conf.getString("splitIntervalH"));
        Assert.assertEquals("2022-09-20 00:00:00", conf.getString("beginDateTime"));
        Assert.assertEquals("2022-09-21 00:00:00", conf.getString("endDateTime"));
    }

    @Test
    public void taskStartReadCase01() {
        InfluxDBReader.Task task = new InfluxDBReader.Task();

        Configuration readerSliceConfig = Configuration.from("{" +
                "\"connection\": [{\"url\":\"http://172.20.48.111:8086\",\"token\":\"ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==\",\"org\":\"tsdb\",\"bucket\":\"shen\"}]," +
                "\"splitIntervalH\":24," +
                "\"beginDateTime\": \"2022-09-20 00:00:00\"," +
                "\"endDateTime\": \"2022-09-21 00:00:00\"" +
                "}");

        task.setPluginJobConf(readerSliceConfig);

        task.init();
        task.prepare();
//        RecordSender recordSender = new
//        task.startRead();

        // assert
        Configuration conf = task.getPluginJobConf();

        Assert.assertEquals("http://172.20.48.111:8086", conf.getString("connection[0].url"));
        Assert.assertEquals("ty1lVsJUYYfSO-Da0IMXWig2Tpuhzr-uqv7PEFwB71WcPo5NVLFyK3AYAG8QbTskUN62-37rU7-C9Pw9JNAKEA==", conf.getString("connection[0].token"));
        Assert.assertEquals("tsdb", conf.getString("connection[0].org"));
        Assert.assertEquals("shen", conf.getString("connection[0].bucket"));
        Assert.assertEquals("24", conf.getString("splitIntervalH"));
        Assert.assertEquals("2022-09-20 00:00:00", conf.getString("beginDateTime"));
        Assert.assertEquals("2022-09-21 00:00:00", conf.getString("endDateTime"));
    }
}
