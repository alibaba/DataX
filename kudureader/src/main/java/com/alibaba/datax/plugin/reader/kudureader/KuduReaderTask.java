package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.CharEncoding;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author daizihao
 * @create 2021-01-19 15:25
 **/
public class KuduReaderTask {
    private final static Logger LOG = LoggerFactory.getLogger(KuduReaderTask.class);

    private List<Configuration> columnConfigs;
    private boolean isReadAllColumns = false;
//    private KuduTable table;
//    private String splitPk;
    private KuduScanner scanner;

    public KuduClient kuduClient;


    public KuduReaderTask(Configuration configuration) {
        this.kuduClient = KuduReaderHelper.getKuduClient(configuration.getString(Key.KUDU_CONFIG));
//        this.table = KuduReaderHelper.getKuduTable(configuration, kuduClient);
//        this.splitPk = configuration.getString(Key.SPLIT_PK);
//        Long upperValue = configuration.getLong(Key.SPLIT_PK_UPPER);
//        Long lowerValue = configuration.getLong(Key.SPLIT_PK_LOWER);
        this.columnConfigs = configuration.getListConfiguration(Key.COLUMN);
        List<String> columnNames = KuduReaderHelper.getColumnNames(configuration);
        if(columnNames == null){
            isReadAllColumns = true;
        }

        try {
            this.scanner = KuduScanToken.deserializeIntoScanner(configuration.getString(Key.SPLIT_PK_TOKEN).getBytes(CharEncoding.ISO_8859_1), kuduClient);
        } catch (IOException e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.TOKEN_DESERIALIZE_ERROR, e.getMessage());
        }


    }

    public void startRead(RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        try {
            while (scanner.hasMoreRows()){
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    //封装Record并发送给writer
                    KuduReaderHelper.transportOneRecord(columnConfigs,result,recordSender, taskPluginCollector, isReadAllColumns);
                }
            }
            recordSender.flush();
        }catch (Exception e){
            LOG.error("read exception! the task will exit!");
            throw DataXException.asDataXException(KuduReaderErrorcode.READ_KUDU_ERROR, e.getMessage());
        }

    }
}
