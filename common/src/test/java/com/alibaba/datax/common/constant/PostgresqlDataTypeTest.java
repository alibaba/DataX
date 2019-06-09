

package com.alibaba.datax.common.constant;

import org.junit.Test;

/**
 * Summary：<p></p>
 * Author : Martin
 * Since  : 2019/5/30 20:52
 */
public class PostgresqlDataTypeTest
{
    @Test
    public void testArrayDataType()
            throws Exception
    {
        final PostgresqlDataType text = PostgresqlDataType.of("_text");
        System.out.println("text = " + text.getTypeName());
    }
}
