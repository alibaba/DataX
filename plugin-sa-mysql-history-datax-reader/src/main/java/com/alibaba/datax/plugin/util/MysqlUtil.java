package com.alibaba.datax.plugin.util;


import cn.hutool.core.lang.Assert;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;

@Slf4j
public class MysqlUtil {

    private static String url;

    private static String user;

    private static String password;

    private static String driverClassName = "com.mysql.jdbc.Driver";

    private static DataSource defaultDataSource;

    public static void setUrl(String url) {
        MysqlUtil.url = url;
    }

    public static void setUser(String user) {
        MysqlUtil.user = user;
    }

    public static void setPassword(String password) {
        MysqlUtil.password = password;
    }

    public static void setDriverClassName(String driverClassName) {
        MysqlUtil.driverClassName = driverClassName;
    }

    public static DataSource getDataSource(String url, String user, String password, String driverClassName) {
        DruidDataSource datasource = new DruidDataSource();
        datasource.setUrl(url);
        datasource.setUsername(user);
        datasource.setPassword(password);
        datasource.setDriverClassName(driverClassName);
        return datasource;
    }

    public static DataSource getDataSource(String url, String user, String password) {
        return MysqlUtil.getDataSource(url, user, password, "com.mysql.jdbc.Driver");
    }

    public static DataSource defaultDataSource() {
        Assert.notBlank(url, "url不能为空");
        Assert.notBlank(driverClassName, "driverClassName不能为空");
        if (defaultDataSource == null) {
            synchronized (MysqlUtil.class) {
                if (defaultDataSource == null) {
                    defaultDataSource = getDataSource(url, user, password);
                }
            }
        }
        return defaultDataSource;
    }

}
