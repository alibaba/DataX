package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RedisWriterHelper {


    /**
     * 检查连接redis是否通
     * @param originalConfig 从json文件获取的配置
     */
    public static void checkConnection(Configuration originalConfig){
        String mode = originalConfig.getNecessaryValue(Key.REDISMODE, CommonErrorCode.CONFIG_ERROR);
        String addr = originalConfig.getNecessaryValue(Key.ADDRESS, CommonErrorCode.CONFIG_ERROR);
        String auth = originalConfig.getString(Key.AUTH);

        if(Constant.CLUSTER.equalsIgnoreCase(mode)){
            JedisCluster jedisCluster = getJedisCluster(addr, auth);
            jedisCluster.set("testConnet","test");
            jedisCluster.expire("testConnet",1);
            try {
                jedisCluster.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }else if(Constant.STANDALONE.equalsIgnoreCase(mode)){
            Jedis jedis = getJedis(addr, auth);
            jedis.set("testConnet","test");
            jedis.expire("testConnet",1);
            jedis.close();
        }else {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
                    String.format("您提供配置文件有误，[%s] redis的redismode必须是standalone或cluster .", mode));

        }
    }

    /**
     * 获取Jedis
     * @param addr 地址，ip:port
     * @param auth 密码
     * @return Jedis
     */
    public static Jedis getJedis(String addr, String auth) {
        String[] split = addr.split(":");
        Jedis jedis = new Jedis(split[0], Integer.parseInt(split[1]));
        if(StringUtils.isNoneBlank(auth)){
            jedis.auth(auth);
        }
        return jedis;
    }

    /**
     * 获取JedisCluster
     * @param addr 地址，ip:port,ip:port,ip:port
     * @param auth 密码
     * @return JedisCluster
     */
    public static JedisCluster getJedisCluster(String addr, String auth) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        JedisCluster jedisCluster;
        Set<HostAndPort> nodes = new HashSet<>();
        String[] split = addr.split(",");
        for (int i = 0; i < split.length; i++) {
            String node = split[i];
            String[] hostPort = node.split(":");
            nodes.add(new HostAndPort(hostPort[0],Integer.parseInt(hostPort[1])));
        }
        if(StringUtils.isBlank(auth)) {
            jedisCluster = new JedisCluster(nodes,10000, 10000, 3,jedisPoolConfig);
        }else {
            jedisCluster =  new JedisCluster(nodes, 10000, 10000, 3, auth, jedisPoolConfig);
        }

        return jedisCluster;
    }


    /**
     * 获取Pipeline
     *
     * @param obj
     * @return
     */
    public static PipelineBase getPipeLine(Object obj) {
        if (obj instanceof Jedis) {
            return ((Jedis) obj).pipelined();
        } else if (obj instanceof JedisCluster) {
            JedisCluster jedis = ((JedisCluster) obj);
            return JedisClusterPipeline.pipelined(jedis);
        }
        return null;
    }

    /**
     * pipline写入数据到redis
     *
     * @param obj
     */
    public static void syscData(Object obj) {
        if (obj instanceof Pipeline) {
            ((Pipeline) obj).sync();

        } else if (obj instanceof JedisClusterPipeline) {
            ((JedisClusterPipeline) obj).sync();
        }
    }

    /**
     * 关闭pipline资源
     *
     * @param obj
     */
    public static void close(Object obj) {
        if (obj instanceof Jedis) {
            ((Jedis) obj).close();
        } else if (obj instanceof JedisCluster) {
            try {
                ((JedisCluster) obj).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
