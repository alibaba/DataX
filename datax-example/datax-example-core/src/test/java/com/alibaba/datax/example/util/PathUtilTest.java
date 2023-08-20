package com.alibaba.datax.example.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * {@code Author} FuYouJ
 * {@code Date} 2023/8/19 21:38
 */

public class PathUtilTest {

    @Test
    public void testParseClassPathFile() {
        String path = "/pathTest.json";
        String absolutePathFromClassPath = PathUtil.getAbsolutePathFromClassPath(path);
        Assert.assertNotNull(absolutePathFromClassPath);
    }
}
