package com.alibaba.datax.plugin.writer.util;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：HttpUtils Test
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
@Ignore
public class HttpUtilsTest {

    @Test
    public void testSimpleCase() throws Exception {
        String url = "https://httpbin.org/post";
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("foo", "bar");

        String rsp = HttpUtils.post(url, null,null,params);
        System.out.println(rsp);
        Assert.assertNotNull(rsp);
    }

    @Test
    public void testGet() throws Exception {
        String url = String.format("%s/api/version", Const.OPENTSDB_ADDRESS);
        String rsp = HttpUtils.get(url,null,null);
        System.out.println(rsp);
        Assert.assertNotNull(rsp);
    }
}
