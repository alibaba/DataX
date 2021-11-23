package com.alibaba.datax.core;


import org.junit.Test;

public class TestMysql2TDengine {

    @Test
    public void test() {
        System.out.println(System.getProperty("java.library.path"));
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/main/job/mysql2tdengine.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        try {
            Engine.entry(params);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}