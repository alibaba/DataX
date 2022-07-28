package cn.com.bluemoon.metadata.base.util;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/JavaDriverFactory.class */
public class JavaDriverFactory {
    private static Driver driver = null;

    public static void init(String uri, String userName, String passwd) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(userName, passwd));
    }

    public static void close() {
        if (driver != null) {
            driver.close();
        }
    }

    public static Driver getDriver() {
        return driver;
    }
}
