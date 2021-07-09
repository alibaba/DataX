package cn.sensorsdata.datax.plugin.util;


import cn.hutool.core.lang.Assert;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Slf4j
public class HiveUtil {

    private static String url;

    private static String user;

    private static String password;

    private static String driverClassName = "org.apache.hive.jdbc.HiveDriver";

    private static DataSource defaultDataSource;

    private static JdbcTemplate defaultJdbcTemplate;

    public static void setUrl(String url) {
        HiveUtil.url = url;
    }

    public static void setUser(String user) {
        HiveUtil.user = user;
    }

    public static void setPassword(String password) {
        HiveUtil.password = password;
    }

    public static void setDriverClassName(String driverClassName) {
        HiveUtil.driverClassName = driverClassName;
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
        return HiveUtil.getDataSource(url, user, password, "org.apache.hive.jdbc.HiveDriver");
    }

    public static JdbcTemplate getJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


    public static DataSource defaultDataSource() {
        Assert.notBlank(url, "url不能为空");
        Assert.notBlank(driverClassName, "driverClassName不能为空");
        if (defaultDataSource == null) {
            synchronized (HiveUtil.class) {
                if (defaultDataSource == null) {
                    defaultDataSource = getDataSource(url, user, password);
                }
            }
        }
        return defaultDataSource;
    }

    public static JdbcTemplate defaultJdbcTemplate() {
        if (defaultJdbcTemplate == null) {
            synchronized (HiveUtil.class) {
                if (defaultJdbcTemplate == null) {
                    defaultJdbcTemplate = getJdbcTemplate(defaultDataSource());
                }
            }
        }
        return defaultJdbcTemplate;
    }

}
