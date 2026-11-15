package com.alibaba.datax.example.iotdb;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;

public class TestIoTDB {

    @Test
    public void testIoTDBReader2MySQLWriter() {
        String path = "/iotdb2mysql.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testMySQLReader2IoTDBWriter() {
        String path = "/mysql2iotdb.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testIoTDBReader2txtWriter() {
        String path = "/iotdb2txt.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testIoTDBReader2StreamWriter() {
        String path = "/iotdb2stream.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testMySQLReader2StreamWriter() {
        String path = "/mysql2stream.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testMySQLReader2txtWriter() {
        String path = "/mysql2txt.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testStreamReader2TxtWriter() {
        String path = "/stream2txt.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testStreamReader2StreamWriter() {
        String path = "/stream2stream.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
