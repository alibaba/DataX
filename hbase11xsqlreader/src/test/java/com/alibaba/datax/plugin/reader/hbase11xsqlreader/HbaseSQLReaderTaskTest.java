package com.alibaba.datax.plugin.reader.hbase11xsqlreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HbaseSQLReaderTaskTest {

    private String jsonStr = "{\n" +
            "        \"hbaseConfig\": {\n" +
            "            \"hbase.zookeeper.quorum\": \"hb-proxy-pub-xxx-001.hbase.rds.aliyuncs.com,hb-proxy-pub-xxx-002.hbase.rds.aliyuncs.com,hb-proxy-pub-xxx-003.hbase.rds.aliyuncs.com\"\n" +
            "        },\n" +
            "        \"table\": \"TABLE1\",\n" +
            "        \"column\": []\n" +
            "    }";

    private List<Configuration> generateSplitConfig() throws IOException, InterruptedException {
        Configuration config = Configuration.from(jsonStr);
        HbaseSQLReaderConfig readerConfig = HbaseSQLHelper.parseConfig(config);
        List<Configuration> splits = HbaseSQLHelper.split(readerConfig);
        System.out.println("split size = " + splits.size());
        return splits;
    }

    @Test
    public void testReadRecord() throws Exception {
        List<Configuration> splits = this.generateSplitConfig();

        int allRecordNum = 0;
        for (int i = 0; i < splits.size(); i++) {
            RecordSender recordSender = mock(RecordSender.class);
            when(recordSender.createRecord()).thenReturn(new DefaultRecord());
            Record record = recordSender.createRecord();

            HbaseSQLReaderTask hbase11SQLReaderTask = new HbaseSQLReaderTask(splits.get(i));
            hbase11SQLReaderTask.init();
            hbase11SQLReaderTask.prepare();

            int num = 0;
            while (true) {
                boolean hasLine = false;
                try {
                    hasLine = hbase11SQLReaderTask.readRecord(record);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
                if (!hasLine)
                    break;
                num++;
                if (num % 100 == 0)
                    System.out.println("record num is :" + num + ",record is " + record.toString());
                when(recordSender.createRecord()).thenReturn(new DefaultRecord());
                String recordStr = "";
                for (int j = 0; j < record.getColumnNumber(); j++) {
                    recordStr += record.getColumn(j).asString() + ",";
                }
                recordSender.sendToWriter(record);
                record = recordSender.createRecord();
            }
            System.out.println("split id is " + i + ",record num = " + num);
            allRecordNum += num;
            recordSender.flush();
            hbase11SQLReaderTask.destroy();
        }
        System.out.println("all record num = " + allRecordNum);
        assertEquals(10000, allRecordNum);
    }
}
