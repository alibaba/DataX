package cn.com.bluemoon.writer.neo4j;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.executor.Neo4jWriterExecutor;
import cn.com.bluemoon.metadata.base.util.JavaDriverFactory;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Driver;

/* loaded from: neo4jWriter-1.0-SNAPSHOT.jar:cn/com/bluemoon/writer/neo4j/Neo4jWriterHelper.class */
public class Neo4jWriterHelper {
    public static void write(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, List<String> columns, int columnNumber, CreateTypeConfig createTypeConfig, int batchSize) {
        List<Record> writeBuffer = new ArrayList<>(batchSize);
        Driver driver = JavaDriverFactory.getDriver();
        while (true) {
            try {
                try {
                    Record record = recordReceiver.getFromReader();
                    if (record == null) {
                        if (!writeBuffer.isEmpty()) {
                            doBatchInsert(driver, writeBuffer, columns, columnNumber, createTypeConfig);
                            writeBuffer.clear();
                        }
                        return;
                    } else if (record.getColumnNumber() != columnNumber) {
                        throw DataXException.asDataXException(Neo4jWriterErrorCode.CONF_ERROR, String.format("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的端要写入的字段数:%s 不相等. 请检查您的配置并作出修改.", Integer.valueOf(record.getColumnNumber()), Integer.valueOf(columnNumber)));
                    } else {
                        writeBuffer.add(record);
                        if (writeBuffer.size() >= batchSize) {
                            doBatchInsert(driver, writeBuffer, columns, columnNumber, createTypeConfig);
                            writeBuffer.clear();
                        }
                    }
                } catch (Exception e) {
                    throw DataXException.asDataXException(Neo4jWriterErrorCode.WRITE_DATA_ERROR, e);
                }
            } finally {
                writeBuffer.clear();
            }
        }
    }

    private static void doBatchInsert(Driver driver, List<Record> writeBuffer, List<String> columns, int columnNumber, CreateTypeConfig createTypeConfig) throws ClassNotFoundException {
        List<Map<String, String>> allRecs = new ArrayList<>(writeBuffer.size());
        for (Record record : writeBuffer) {
            allRecs.add(record2Map(record, columns, columnNumber));
        }
        Neo4jWriterExecutor.batchExecute(driver, createTypeConfig, transfer(allRecs));
    }

    private static List<Map<String, Object>> transfer(List<Map<String, String>> allRecs) {
        List<Map<String, Object>> res = Lists.newArrayList();
        for (Map<String, String> allRec : allRecs) {
            Map<String, Object> map = Maps.newHashMap();
            for (String s : allRec.keySet()) {
                map.put(s, allRec.get(s));
            }
            res.add(map);
        }
        return res;
    }

    private static Map<String, String> record2Map(Record record, List<String> columns, int columnNumber) {
        Map<String, String> item = new HashMap<>();
        for (int i = 0; i < columnNumber; i++) {
            item.put(columns.get(i), record.getColumn(i).asString());
        }
        return item;
    }
}
