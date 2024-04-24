package com.alibaba.datax.example.streamreader;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;

/**
 * 测试数据发送Kafka
 *
 * @author LZC
 * @date 2024-04-23
 **/
public class KafkaWriterTest {

    @Test
    public void testDm2Kafka() {
        String path = "/dm2kafka.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testMysql2Kafka() {
        String path = "/mysql2kafka.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testStream2Kafka() {
        String path = "/stream2kafka.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
