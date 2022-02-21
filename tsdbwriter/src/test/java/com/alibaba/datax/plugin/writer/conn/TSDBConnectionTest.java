package com.alibaba.datax.plugin.writer.conn;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDBConnection Test
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
@Ignore
public class TSDBConnectionTest {

    private static final String TSDB_ADDRESS = "http://localhost:8240";

    @Test
    public void testVersion() {
        String version = new TSDBConnection(TSDB_ADDRESS,null,null,null).version();
        Assert.assertNotNull(version);
    }

    @Test
    public void testIsSupported() {
        Assert.assertTrue(new TSDBConnection(TSDB_ADDRESS,null,null,null).isSupported());
    }
}
