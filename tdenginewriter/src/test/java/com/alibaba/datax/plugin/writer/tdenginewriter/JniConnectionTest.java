package com.alibaba.datax.plugin.writer.tdenginewriter;

import org.junit.Test;

import java.util.Properties;

public class JniConnectionTest {

    @Test
    public void test() throws Exception {
        JniConnection connection = new JniConnection(new Properties());

        connection.open("192.168.56.105", 6030, "test", "root", "taosdata");

        String json = "{\"metric\":\"weather_temperature\",\"timestamp\":1609430400000,\"value\":123,\"tags\":{\"location\":\"beijing\",\"id\":\"t123\"}}";
        connection.insertOpentsdbJson(json);

        connection.close();
    }

}