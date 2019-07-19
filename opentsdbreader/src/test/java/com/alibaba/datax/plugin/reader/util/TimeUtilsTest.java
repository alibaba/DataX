package com.alibaba.datax.plugin.reader.util;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：com.alibaba.datax.common.util
 *
 * @author Benedict Jin
 * @since 2019-04-22
 */
public class TimeUtilsTest {

    @Test
    public void testIsSecond() {
        Assert.assertFalse(TimeUtils.isSecond(System.currentTimeMillis()));
        Assert.assertTrue(TimeUtils.isSecond(System.currentTimeMillis() / 1000));
    }

    @Test
    public void testGetTimeInHour() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse("2019-04-18 15:32:33");
        long timeInHour = TimeUtils.getTimeInHour(date.getTime());
        Assert.assertEquals("2019-04-18 15:00:00", sdf.format(timeInHour));
    }
}
