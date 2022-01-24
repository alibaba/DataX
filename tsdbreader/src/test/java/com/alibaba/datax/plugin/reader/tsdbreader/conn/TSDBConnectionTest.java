package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Connection4TSDB Test
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
@Ignore
public class TSDBConnectionTest {

    private static final String TSDB_ADDRESS = "http://localhost:8242";

    @Test
    public void testVersion() {
        String version = new TSDBConnection(TSDB_ADDRESS,null,null).version();
        Assert.assertNotNull(version);
    }

    @Test
    public void testIsSupported() {
        Assert.assertTrue(new TSDBConnection(TSDB_ADDRESS,null,null).isSupported());
    }
}
