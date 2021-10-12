package com.alibaba.datax.plugin.writer;

import org.junit.Test;

import java.util.Properties;

public class JniConnectionTest {

    @Test
    public void test() {
        JniConnection connection = new JniConnection(new Properties());

        connection.open("192.168.56.105", 6030, "log", "root", "taosdata");

        String json = "{\"metric\":\"weather.temperature\",\"timestamp\":1609430400000,\"value\":123,\"tags\":{\"location\":\"beijing\",\"id\":123}}";
        connection.insertOpentsdbJson(json);

        connection.close();
    }

}