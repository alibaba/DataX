package com.alibaba.datax.example.kafka;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;


public class KafkaTest {
    @Test
    public void testStreamReader2KafkaWriter() {
        String path = "/stream2kafka.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
