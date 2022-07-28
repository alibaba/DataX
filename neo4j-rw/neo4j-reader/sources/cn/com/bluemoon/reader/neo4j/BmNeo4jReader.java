package cn.com.bluemoon.reader.neo4j;

import cn.com.bluemoon.metadata.base.util.JavaDriverFactory;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4jReader-1.0-SNAPSHOT.jar:cn/com/bluemoon/reader/neo4j/BmNeo4jReader.class */
public class BmNeo4jReader extends Reader {

    /* loaded from: neo4jReader-1.0-SNAPSHOT.jar:cn/com/bluemoon/reader/neo4j/BmNeo4jReader$Job.class */
    public static class Job extends Reader.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        public void init() {
            log.info("开始BmNeo4jReaderJob的初始化！");
            this.conf = getPluginJobConf();
            try {
                BmNeo4jReaderParamHelper.validParameters(this.conf.getMap("", String.class));
            } catch (Exception e) {
                log.error("Neo4jWriter params:{}", this.conf.toJSON());
                throw new RuntimeException(e);
            }
        }

        public void prepare() {
        }

        public List<Configuration> split(int adviceNumber) {
            return SimpleBmNeo4jReaderSplitUtils.doSplit(getPluginJobConf(), adviceNumber);
        }

        public void post() {
        }

        public void destroy() {
        }
    }

    /* loaded from: neo4jReader-1.0-SNAPSHOT.jar:cn/com/bluemoon/reader/neo4j/BmNeo4jReader$Task.class */
    public static class Task extends Reader.Task {
        private Configuration rawConf;
        private Map<String, Object> taskConfig;
        private String dbUserName;
        private String dbPassword;
        private String queryCql;
        private String neo4jUrl;

        public void init() {
            this.rawConf = getPluginJobConf();
            this.taskConfig = this.rawConf.getMap("", Object.class);
            this.dbUserName = this.taskConfig.get("username").toString();
            this.dbPassword = this.taskConfig.get("password").toString();
            this.neo4jUrl = this.taskConfig.get("neo4jUrl").toString();
            this.queryCql = this.taskConfig.get("queryCql").toString();
            JavaDriverFactory.init(this.neo4jUrl, this.dbUserName, this.dbPassword);
        }

        public void prepare() {
        }

        public void startRead(RecordSender recordSender) {
            Neo4jWriterHelper.read(recordSender, getTaskPluginCollector(), this.queryCql);
        }

        public void post() {
        }

        public void destroy() {
            JavaDriverFactory.close();
        }
    }
}
