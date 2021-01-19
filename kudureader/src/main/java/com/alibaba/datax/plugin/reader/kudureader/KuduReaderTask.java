package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.util.Configuration;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author daizihao
 * @create 2021-01-19 15:25
 **/
public class KuduReaderTask {
    private final static Logger LOG = LoggerFactory.getLogger(KuduReaderTask.class);

    private List<String> columns;
    private KuduTable table;

    public KuduClient kuduClient;


    public KuduReaderTask(Configuration configuration) {
        this.kuduClient = KuduReaderHelper.getKuduClient(configuration.getString(Key.KUDU_CONFIG));
        this.table = KuduReaderHelper.getKuduTable(configuration, kuduClient);
        this.columns = configuration.getList(Key.COLUMN, String.class);
    }

    public void startRead() {

    }
}
