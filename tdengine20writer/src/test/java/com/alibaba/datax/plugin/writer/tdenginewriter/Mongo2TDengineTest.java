package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Test;

public class Mongo2TDengineTest {

    @Test
    public void case01() throws Throwable {

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/mongo2t.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }
}