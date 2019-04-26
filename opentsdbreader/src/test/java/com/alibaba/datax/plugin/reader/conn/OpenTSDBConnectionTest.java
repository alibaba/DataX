package com.alibaba.datax.plugin.reader.conn;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：OpenTSDB Connection4TSDB Test
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
@Ignore
public class OpenTSDBConnectionTest {

    private static final String OPENTSDB_ADDRESS = "http://localhost:8242";

    @Test
    public void testVersion() {
        String version = new OpenTSDBConnection(OPENTSDB_ADDRESS).version();
        Assert.assertNotNull(version);
    }

    @Test
    public void testIsSupported() {
        Assert.assertTrue(new OpenTSDBConnection(OPENTSDB_ADDRESS).isSupported());
    }
}
