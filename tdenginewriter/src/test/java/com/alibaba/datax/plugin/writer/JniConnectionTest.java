package com.alibaba.datax.plugin.writer;

import org.junit.Test;

import java.util.Properties;

public class JniConnectionTest {

    @Test
    public void test() {
        JniConnection connection = new JniConnection(new Properties());

        long psql = connection.open("192.168.56.107", 6030, "log", "root", "taosdata");
        System.out.println("psql: " + psql);

        connection.close();
    }

}