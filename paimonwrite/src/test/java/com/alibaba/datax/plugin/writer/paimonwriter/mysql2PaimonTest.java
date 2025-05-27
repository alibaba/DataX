package com.alibaba.datax.plugin.writer.paimonwriter;

import com.alibaba.datax.core.Engine;
import org.junit.Test;

public class mysql2PaimonTest {

    private static final String host = "localhost";

    @Test
    public void case01() throws Throwable {

        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/mysql_to_paimon.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);

    }


}
