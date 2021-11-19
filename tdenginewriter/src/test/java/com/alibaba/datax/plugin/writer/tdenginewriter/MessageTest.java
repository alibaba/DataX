package com.alibaba.datax.plugin.writer.tdenginewriter;

import org.junit.Test;

import java.util.Locale;
import java.util.ResourceBundle;

import org.junit.Assert;

public class MessageTest {
    @Test
    public void testChineseMessage() {
        Locale local = new Locale("zh", "CN");
        ResourceBundle bundle = ResourceBundle.getBundle("tdenginewritermsg", local);
        String msg = bundle.getString("try_get_schema_fromdb");
        Assert.assertEquals("无法从配置文件获取表结构信息，尝试从数据库获取", msg);
    }

    @Test
    public void  testDefaultMessage() {
        ResourceBundle bundle = ResourceBundle.getBundle("tdenginewritermsg", Locale.getDefault());
        String msg = bundle.getString("try_get_schema_fromdb");
        System.out.println(msg);
    }
}
