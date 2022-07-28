package cn.com.bluemoon.metadata.base.util;

import cn.com.bluemoon.metadata.common.exception.DescribeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/JedisPoolUtil.class */
public class JedisPoolUtil {
    private static volatile JedisPool jedisPool = null;
    private static String host;
    private static Integer port;
    private static String password;

    static {
        InputStream inputStream = ModelIdUtils.class.getResourceAsStream("/redis.properties");
        Properties prop = new Properties();
        try {
            prop.load(inputStream);
            host = prop.getProperty("redis.host");
            port = Integer.valueOf(Integer.parseInt(prop.getProperty("redis.port")));
            password = prop.getProperty("redis.auth");
        } catch (IOException e) {
            throw new DescribeException("读取Redis配置文件失败!", 500);
        }
    }

    private JedisPoolUtil() {
    }

    public static JedisPool getJedisPoolInstance() {
        if (null == jedisPool) {
            synchronized (JedisPoolUtil.class) {
                if (null == jedisPool) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(30);
                    poolConfig.setMaxIdle(10);
                    poolConfig.setMaxWaitMillis(100000);
                    poolConfig.setTestOnBorrow(true);
                    if (StringUtils.isEmpty(password)) {
                        password = null;
                    }
                    jedisPool = new JedisPool(poolConfig, host, port.intValue(), 2000, password);
                }
            }
        }
        return jedisPool;
    }

    public static void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    public static Jedis getJedis() {
        return getJedisPoolInstance().getResource();
    }
}
