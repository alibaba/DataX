package cn.com.bluemoon.reader.neo4j;

import com.alibaba.datax.common.util.Configuration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4jReader-1.0-SNAPSHOT.jar:cn/com/bluemoon/reader/neo4j/SimpleBmNeo4jReaderSplitUtils.class */
public class SimpleBmNeo4jReaderSplitUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleBmNeo4jReaderSplitUtils.class);

    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber) {
        LOG.info("开始切分任务");
        List<Object> conns = originalSliceConfig.getList("connection", Object.class);
        List<Configuration> splittedConfigs = new ArrayList<>();
        int len = conns.size();
        for (int i = 0; i < len; i++) {
            Configuration sliceConfig = originalSliceConfig.clone();
            Configuration connConf = Configuration.from(conns.get(i).toString());
            sliceConfig.remove("connection");
            List<String> sqls = connConf.getList("queryCql", String.class);
            String neo4jUrl = (String) connConf.getList("neo4jUrl", String.class).get(0);
            for (String querySql : sqls) {
                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set("queryCql", querySql);
                tempSlice.set("neo4jUrl", neo4jUrl);
                splittedConfigs.add(tempSlice);
            }
        }
        return splittedConfigs;
    }
}
