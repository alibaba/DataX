package cn.com.bluemoon.writer.neo4j;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.config.CreateTypeConfigGenerator;
import cn.com.bluemoon.metadata.base.util.JavaDriverFactory;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4jWriter-1.0-SNAPSHOT.jar:cn/com/bluemoon/writer/neo4j/BmNeo4jWriter.class */
public class BmNeo4jWriter extends Writer {

    /* loaded from: neo4jWriter-1.0-SNAPSHOT.jar:cn/com/bluemoon/writer/neo4j/BmNeo4jWriter$Job.class */
    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        public void init() {
            log.info("开始BmNeo4jWriterJob的初始化！");
            this.conf = getPluginJobConf();
            try {
                CypherParamHelper.validParameters(this.conf.getMap("", String.class));
            } catch (Exception e) {
                log.error("Neo4jWriter params:{}", this.conf.toJSON());
                throw new RuntimeException(e);
            }
        }

        public void prepare() {
        }

        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.conf.clone());
            }
            return configurations;
        }

        public void post() {
        }

        public void destroy() {
        }
    }

    /* loaded from: neo4jWriter-1.0-SNAPSHOT.jar:cn/com/bluemoon/writer/neo4j/BmNeo4jWriter$Task.class */
    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private Map<String, Object> taskConfig;
        private Configuration rawConf;
        private String dbUrl;
        private String dbUserName;
        private String dbPassword;
        private int batchSize;
        private List<String> columns;
        private CreateTypeConfig createTypeConfig = null;
        private int columnNumber = 0;

        public void init() {
            this.rawConf = getPluginJobConf();
            this.columns = this.rawConf.getList("column", String.class);
            this.batchSize = this.rawConf.getInt("batchSize", 10).intValue();
            this.taskConfig = this.rawConf.getMap("", Object.class);
            this.dbUrl = this.taskConfig.get("uri").toString();
            this.dbUserName = this.taskConfig.get("username").toString();
            this.dbPassword = this.taskConfig.get("password").toString();
            this.columnNumber = this.columns.size();
            this.createTypeConfig = CreateTypeConfigGenerator.generate(this.taskConfig);
            JavaDriverFactory.init(this.dbUrl, this.dbUserName, this.dbPassword);
        }

        public void startWrite(RecordReceiver recordReceiver) {
            Neo4jWriterHelper.write(recordReceiver, getTaskPluginCollector(), this.columns, this.columnNumber, this.createTypeConfig, this.batchSize);
        }

        public void destroy() {
            JavaDriverFactory.close();
        }
    }
}
