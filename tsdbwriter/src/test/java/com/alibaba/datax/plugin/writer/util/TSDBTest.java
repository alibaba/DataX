package com.alibaba.datax.plugin.writer.util;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Test
 *
 * @author Benedict Jin
 * @since 2019-04-11
 */
@Ignore
public class TSDBTest {

    @Test
    public void testVersion() {
        String version = TSDBUtils.version(Const.TSDB_ADDRESS,null,null);
        Assert.assertNotNull(version);
        System.out.println(version);

        version = TSDBUtils.version(Const.OPENTSDB_ADDRESS,null,null);
        Assert.assertNotNull(version);
        System.out.println(version);
    }
}
