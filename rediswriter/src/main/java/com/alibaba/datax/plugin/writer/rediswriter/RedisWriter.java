package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.JedisClusterCRC16;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RedisWriter extends Writer {


    public static class Task extends com.alibaba.datax.common.spi.Writer.Task {

        /**
         * slot 对应cluster Redis 节点
         */
        private final Map<Integer, Jedis> cluster = new HashMap<Integer, Jedis>();


        private final Map<Jedis, AtomicLong> nodeCounterMap = new HashMap<Jedis, AtomicLong>();

        /**
         * 单机redis
         */
        private Jedis jedis;

        private static final Logger LOG = LoggerFactory.getLogger(RedisWriter.Job.class);


        /**
         * 每次批量处理数量
         */
        private long batchSize = 1000L;


        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Configuration pluginJobConf = this.getPluginJobConf();

            boolean isCluster = pluginJobConf.getBool("redisCluster", false);
            if (isCluster) {
                this.clusterWrite(lineReceiver);
            } else {
                this.standaloneWrite(lineReceiver);
            }

        }

        @Override
        public void init() {
            Configuration pluginJobConf = this.getPluginJobConf();
            List connections = pluginJobConf.getList("connection");
            boolean isCluster = pluginJobConf.getBool("redisCluster", false);
            int timeout = pluginJobConf.getInt("timeout", 60000);
            this.batchSize = pluginJobConf.getLong("batchSize", 1000L);
            if (connections.size() == 0) {
                throw new RuntimeException("请添加redis 连接");
            } else {
                Map connection = (Map) connections.get(0);
                URI uri = URI.create(connection.get("uri").toString());
                String host = uri.getHost();
                int port = uri.getPort();
                this.jedis = new Jedis(host, port, timeout, timeout);

                //如果是redis cluster,将获取cluster主机节点对应的slot槽
                if (isCluster) {
                    StringBuilder sb = new StringBuilder("\r\nRedis Cluster 节点分配\r\n");
                    List<Object> slots = this.jedis.clusterSlots();

                    for (Object slot : slots) {

                        List list = (List) slot;
                        //slot 开始节点
                        Long start = (Long) list.get(0);
                        //slot 结束节点
                        Long end = (Long) list.get(1);
                        //slot 对应主机信息
                        List hostInfo = (List) list.get(2);

                        String nodeHost = new String((byte[]) hostInfo.get(0));
                        Long nodePort = (Long) hostInfo.get(1);
                        Jedis node = new Jedis(nodeHost, nodePort.intValue(), timeout, timeout);
                        for (int i = start.intValue(); i <= end.intValue(); i++) {
                            this.cluster.put(i, node);
                        }

                        this.nodeCounterMap.put(node, new AtomicLong());
                        sb.append(nodeHost)
                                .append(":")
                                .append(nodePort)
                                .append("\t")
                                .append("slot:")
                                .append(start)
                                .append("-").append(end)
                                .append("\r\n");

                    }
                    LOG.info(sb.toString());
                } else {
                    String auth = (String) connection.get("auth");
                    if (StringUtils.isNotBlank(auth)) {
                        this.jedis.auth(auth);
                    }
                }

            }
            prepare();
        }

        /**
         * 判断是否携带格式化redis 数据库
         */
        @Override
        public void prepare() {
            Boolean isFlushDB = getPluginJobConf().getBool("flushDB", false);
            if (isFlushDB) {
                flushDB();
            }
        }


        public void destroy() {
            if (this.jedis != null) {
                this.jedis.close();
            }

            this.nodeCounterMap.clear();
            for (Jedis jedis : new HashSet<Jedis>(this.cluster.values())) {
                jedis.close();
            }

            this.cluster.clear();

        }

        /**
         * 单机或proxy 写入模式
         *
         * @param lineReceiver
         */
        private void standaloneWrite(RecordReceiver lineReceiver) {
            AtomicLong counter = new AtomicLong(0L);
            Client client = this.jedis.getClient();
            Record fromReader;
            while ((fromReader = lineReceiver.getFromReader()) != null) {
                Column dbColumn = fromReader.getColumn(0);
                Column type = fromReader.getColumn(1);
                Column expireColumn = fromReader.getColumn(2);
                Column keyColumn = fromReader.getColumn(3);
                Column valueColumn = fromReader.getColumn(4);
                Long db = dbColumn.asLong();
                long expire = expireColumn.asLong();
                byte[] key = keyColumn.asBytes();
                byte[] value = valueColumn.asBytes();
                client.select(db.intValue());
                restore(client, key, value, expire, counter);
            }

            if (counter.get() % this.batchSize != 0L) {
                flushAndCheckReply(client);
            }
        }


        /**
         * redis cluster 集群写入
         *
         * @param lineReceiver
         */
        private void clusterWrite(RecordReceiver lineReceiver) {

            Record fromReader;
            while ((fromReader = lineReceiver.getFromReader()) != null) {
                Column expireColumn = fromReader.getColumn(2);
                Column keyColumn = fromReader.getColumn(3);
                Column valueColumn = fromReader.getColumn(4);
                Long expire = expireColumn.asLong();
                byte[] key = keyColumn.asBytes();
                byte[] value = valueColumn.asBytes();
                int slot = JedisClusterCRC16.getSlot(key);
                Jedis node = this.cluster.get(slot);
                Client client = node.getClient();
                AtomicLong nodeCounter = nodeCounterMap.get(node);
                restore(client, key, value, expire, nodeCounter);
            }

            for (Entry<Jedis, AtomicLong> entry : nodeCounterMap.entrySet()) {
                AtomicLong nodeCounter = entry.getValue();
                Jedis node = entry.getKey();
                if (nodeCounter.get() % this.batchSize != 0L) {
                    Client client = node.getClient();
                    flushAndCheckReply(client);
                }
            }

        }

        private void restore(Client client, byte[] key, byte[] value, long expire, AtomicLong currentCounter) {

            client.restore(key, 0, value);

            if (expire > 0) {
                client.expireAt(key, expire);
            }


            long count = currentCounter.incrementAndGet();

            if (count % this.batchSize == 0L) {
                flushAndCheckReply(client);
            }
        }

        private static final AtomicBoolean FLUSH_FLAG = new AtomicBoolean(false);


        private void flushDB() {
            synchronized (FLUSH_FLAG) {
                if (FLUSH_FLAG.get()) {
                    return;
                }

                boolean isCluster = getPluginJobConf().getBool("redisCluster", false);


                if (isCluster) {
                    for (Jedis jedis : new HashSet<Jedis>(cluster.values())) {
                        Client client = jedis.getClient();
                        LOG.info("格式化:" + client.getHost() + ":" + client.getPort());
                        jedis.flushAll();
                    }
                } else {
                    if (this.jedis != null) {
                        Client client = jedis.getClient();
                        LOG.info("格式化:" + client.getHost() + ":" + client.getPort());
                        jedis.flushAll();
                    }
                }

                FLUSH_FLAG.set(true);
            }
        }

        /**
         * 发送并检查异常
         *
         * @param client
         */
        private void flushAndCheckReply(Client client) {
            List<Object> allReply = client.getAll();
            for (Object o : allReply) {
                if (o instanceof JedisDataException) {
                    throw (JedisDataException) o;
                }
            }
        }
    }


    public static class Job extends com.alibaba.datax.common.spi.Writer.Job {

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return Collections.singletonList(getPluginJobConf());
        }

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
        }

    }
}