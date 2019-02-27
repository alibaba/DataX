package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.DumpRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbVisitor;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

import static com.moilioncircle.redis.replicator.Constants.*;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS;

public class RedisReader extends Reader {

    public static class Task extends com.alibaba.datax.common.spi.Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(RedisReader.Job.class);

        /**
         * 包含key正则
         */
        private final List<Pattern> includePatterns = new ArrayList<Pattern>();

        /**
         * 排除key正则
         */
        private final List<Pattern> excludePatterns = new ArrayList<Pattern>();

        /**
         * 包含DB
         */
        private final Set<Integer> includeDB = new HashSet<Integer>();

        /**
         * 记录redis 比较大的key 用于展示
         */
        private Map<String, Integer> bigKey = new TreeMap<String, Integer>();

        /**
         * value达到64m阀值，将记录该key
         */
        private int keyThresholdLength = 64 * 1024 * 1024;


        /**
         * 用于记录数据类型分布
         */
        private final Map<String, Long> collectTypeMap = new HashMap<String, Long>();

        @Override
        public void startRead(final RecordSender recordSender) {
            Configuration pluginJobConf = super.getPluginJobConf();

            try {
                List connections = pluginJobConf.getList("connection");

                for (Object obj : connections) {
                    Map connection = (Map) obj;
                    URI uri = URI.create(connection.get("uri").toString());
                    File file = new File("./" + UUID.randomUUID().toString() + ".rdb");
                    if ("http".equals(uri.getScheme())) {
                        this.download(uri, file);
                    } else if ("tcp".equals(uri.getScheme())) {
                        String auth = "";
                        if (connection.get("auth") != null) {
                            auth = "?authPassword=" + connection.get("auth").toString();
                        }

                        this.dump(uri.toString().replace("tcp://", "redis://") + auth, file);
                    } else {
                        file = new File(uri);
                    }

                    LOG.info("loading " + file.getAbsolutePath());
                    RedisReplicator r = new RedisReplicator(file, FileType.RDB, com.moilioncircle.redis.replicator.Configuration.defaultSetting());
                    r.setRdbVisitor(new DumpRdbVisitor(r));
                    r.addEventListener(new EventListener() {
                        public void onEvent(Replicator replicator, Event event) {
                            if (event instanceof DumpKeyValuePair) {
                                DumpKeyValuePair dkv = (DumpKeyValuePair) event;
                                Long dbNumber = dkv.getDb().getDbNumber();
                                byte[] key = dkv.getKey();
                                long expire = dkv.getExpiredMs() == null ? 0 : dkv.getExpiredMs();

                                //记录较大的key
                                recordBigKey(dbNumber, dkv.getValueRdbType(), dkv.getKey(), dkv.getValue());

                                //记录数据类型
                                collectType(dkv.getValueRdbType());

                                if (Task.this.matchDB(dbNumber.intValue()) && Task.this.matchKey(key)) {
                                    Record record = recordSender.createRecord();
                                    record.addColumn(new LongColumn(dbNumber));
                                    record.addColumn(new LongColumn(dkv.getValueRdbType()));
                                    record.addColumn(new LongColumn(expire));
                                    record.addColumn(new BytesColumn(key));
                                    record.addColumn(new BytesColumn(dkv.getValue()));
                                    recordSender.sendToWriter(record);
                                }

                            }
                        }
                    });
                    r.open();
                    r.close();
                }

            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public void init() {
            Configuration pluginJobConf = this.getPluginJobConf();
            List<Object> include = pluginJobConf.getList("include");
            List<Object> exclude = pluginJobConf.getList("exclude");
            List<Object> db = pluginJobConf.getList("db");
            this.keyThresholdLength = pluginJobConf.getInt("keyThresholdLength", 64 * 1024 * 1024);
            if (include != null) {
                for (Object reg : include) {
                    Pattern pattern = Pattern.compile(reg.toString());
                    includePatterns.add(pattern);
                }
            }

            if (exclude != null) {
                for (Object reg : exclude) {
                    Pattern pattern = Pattern.compile(reg.toString());
                    excludePatterns.add(pattern);
                }
            }

            if (db != null) {
                for (Object num : db) {
                    includeDB.add(Integer.parseInt(String.valueOf(num)));
                }
            }


        }

        /**
         * 记录较大的key 用于展示
         *
         * @param db
         * @param type
         * @param key
         * @param value
         */
        private void recordBigKey(Long db, int type, byte[] key, byte[] value) {
            if (value.length > keyThresholdLength) {
                bigKey.put(db + "\t" + new String(key, Charset.forName("utf-8")), value.length);
            }
        }


        @Override
        public void destroy() {
            StringBuilder sb = new StringBuilder("Redis中较大的key:\r\n");

            for (Map.Entry<String, Integer> entry : bigKey.entrySet()) {
                sb.append(entry.getKey())
                        .append("\t")
                        .append(entry.getValue())
                        .append("\r\n");
            }

            LOG.info("\r\n" + sb.toString());

            sb = new StringBuilder("Redis数据类型分布:\r\n");

            for (Map.Entry<String, Long> entry : collectTypeMap.entrySet()) {
                sb.append(entry.getKey())
                        .append("\t")
                        .append(entry.getValue())
                        .append("\r\n");
            }

            LOG.info("\r\n" + sb.toString());
        }

        /**
         * 判断是否匹配key
         *
         * @param bytes key
         * @return
         */
        private boolean matchKey(byte[] bytes) {
            if (includePatterns.isEmpty() && excludePatterns.isEmpty()) return true;

            String key = new String(bytes, Charset.forName("utf-8"));

            for (Pattern pattern : includePatterns) {
                boolean isMatch = pattern.matcher(key).find();
                if (isMatch) return true;
            }

            for (Pattern pattern : excludePatterns) {
                boolean isMatch = pattern.matcher(key).find();
                if (isMatch) return false;
            }

            return false;
        }

        /**
         * 判断是否包含相关db
         *
         * @param db
         * @return
         */
        private boolean matchDB(int db) {
            return this.includeDB.isEmpty() || this.includeDB.contains(db);
        }

        /**
         * 通过sync命令远程下载redis server rdb文件
         *
         * @param uri     redis地址
         * @param outFile 输出路径
         * @throws IOException
         * @throws URISyntaxException
         */
        private void dump(String uri, File outFile) throws IOException, URISyntaxException {
            final OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile));
            final RawByteListener rawByteListener = new RawByteListener() {
                public void handle(byte... rawBytes) {
                    try {
                        out.write(rawBytes);
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            };
            Replicator replicator = new RedisReplicator(uri);
            replicator.setRdbVisitor(new SkipRdbVisitor(replicator));
            replicator.addEventListener(new EventListener() {
                public void onEvent(Replicator replicator, Event event) {
                    if (event instanceof PreRdbSyncEvent) {
                        replicator.addRawByteListener(rawByteListener);
                    }

                    if (event instanceof PostRdbSyncEvent) {
                        replicator.removeRawByteListener(rawByteListener);

                        try {
                            out.close();
                            replicator.close();
                        } catch (IOException e) {
                            LOG.warn(e.getMessage(), e);
                        }
                    }

                }
            });
            replicator.open();
        }

        /**
         * 下载远程rdb文件
         *
         * @param uri     rdb路径
         * @param outFile 输出路径
         * @throws IOException
         */
        private void download(URI uri, File outFile) throws IOException {
            CloseableHttpClient httpClient = this.getHttpClient();
            CloseableHttpResponse response = httpClient.execute(new HttpGet(uri));
            HttpEntity entity = response.getEntity();
            InputStream in = entity.getContent();
            FileOutputStream out = new FileOutputStream(outFile);
            byte[] bytes = new byte[4096 * 1000];

            int len;
            System.err.print("Downloading");
            while ((len = in.read(bytes)) != -1) {
                out.write(bytes, 0, len);
                out.flush();
                System.out.print(".");
            }

            out.close();
            in.close();
        }

        private CloseableHttpClient getHttpClient() {
            return HttpClientBuilder.create().build();
        }


        private void collectType(int type) {
            String name = getTypeName(type);
            Long count = collectTypeMap.get(name);
            if (count == null) {
                collectTypeMap.put(name, 1L);
            } else {
                collectTypeMap.put(name, count + 1);
            }

        }

        private String getTypeName(int type) {
            switch (type) {
                case RDB_TYPE_STRING:
                    return "string";
                case RDB_TYPE_LIST:
                    return "list";
                case RDB_TYPE_SET:
                    return "set";
                case RDB_TYPE_ZSET:
                    return "zset";
                case RDB_TYPE_ZSET_2:
                    return "zset2";
                case RDB_TYPE_HASH:
                    return "hash";
                case RDB_TYPE_HASH_ZIPMAP:
                    return "hash_zipmap";
                case RDB_TYPE_LIST_ZIPLIST:
                    return "list_ziplist";
                case RDB_TYPE_SET_INTSET:
                    return "set_intset";
                case RDB_TYPE_ZSET_ZIPLIST:
                    return "zset_ziplist";
                case RDB_TYPE_HASH_ZIPLIST:
                    return "hash_ziplist";
                case RDB_TYPE_LIST_QUICKLIST:
                    return "list_quicklist";
                case RDB_TYPE_MODULE:
                    return "module";
                case RDB_TYPE_MODULE_2:
                    return "module2";
                case RDB_TYPE_STREAM_LISTPACKS:
                    return "stream_listpacks";
                default:
                    return "other";
            }
        }
    }

    public static class Job extends com.alibaba.datax.common.spi.Reader.Job {

        @Override
        public List<Configuration> split(int adviceNumber) {
            return Collections.singletonList(super.getPluginJobConf());
        }

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
        }
    }
}
