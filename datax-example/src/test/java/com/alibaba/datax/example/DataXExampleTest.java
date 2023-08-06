package com.alibaba.datax.example;


import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;


public class DataXExampleTest {

    @Test
    public void testStreamReader2StreamWriter() {
        String path = "/job/stream2stream.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
