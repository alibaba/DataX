package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author daizihao
 * @create 2021-01-15 16:18
 **/
public class KuduReaderHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KuduReaderHelper.class);

    public static void validateParameter(Configuration configuration) {
        LOG.info("Start validating parameters！");
        configuration.getNecessaryValue(Key.KUDU_CONFIG, KuduReaderErrorcode.REQUIRED_VALUE);
        configuration.getNecessaryValue(Key.TABLE, KuduReaderErrorcode.REQUIRED_VALUE);
        configuration.getNecessaryValue(Key.COLUMN, KuduReaderErrorcode.REQUIRED_VALUE);
    }

    public static Map<String, Object> getKuduConfiguration(String kuduConfig) {
        if (StringUtils.isBlank(kuduConfig)) {
            throw DataXException.asDataXException(KuduReaderErrorcode.REQUIRED_VALUE,
                    "Connection configuration information required.");
        }
        Map<String, Object> kConfiguration;
        try {
            kConfiguration = JSON.parseObject(kuduConfig, HashMap.class);
            Validate.isTrue(kConfiguration != null, "kuduConfig is null!");
            kConfiguration.put(Key.KUDU_ADMIN_TIMEOUT, kConfiguration.getOrDefault(Key.KUDU_ADMIN_TIMEOUT, Constant.ADMIN_TIMEOUTMS));
            kConfiguration.put(Key.KUDU_SESSION_TIMEOUT, kConfiguration.getOrDefault(Key.KUDU_SESSION_TIMEOUT, Constant.SESSION_TIMEOUTMS));
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_CONNECTION_ERROR, e);
        }

        return kConfiguration;
    }

    public static KuduClient getKuduClient(String kuduConfig) {
        Map<String, Object> conf = KuduReaderHelper.getKuduConfiguration(kuduConfig);
        KuduClient kuduClient = null;
        try {
            String masterAddress = (String) conf.get(Key.KUDU_MASTER);
            kuduClient = new KuduClient.KuduClientBuilder(masterAddress)
                    .defaultAdminOperationTimeoutMs((Long) conf.get(Key.KUDU_ADMIN_TIMEOUT))
                    .defaultOperationTimeoutMs((Long) conf.get(Key.KUDU_SESSION_TIMEOUT))
                    .build();
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_CONNECTION_ERROR, e);
        }
        return kuduClient;
    }

    public static KuduTable getKuduTable(Configuration configuration, KuduClient kuduClient) {
        String tableName = configuration.getString(Key.TABLE);

        KuduTable table = null;
        try {
            table = kuduClient.openTable(tableName);
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_TABLE_ERROR, e);
        }
        return table;
    }

    public static boolean isTableExists(Configuration configuration) {
        String tableName = configuration.getString(Key.TABLE);
        String kuduConfig = configuration.getString(Key.KUDU_CONFIG);
        KuduClient kuduClient = KuduReaderHelper.getKuduClient(kuduConfig);
        try {
            return kuduClient.tableExists(tableName);
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_CONNECTION_ERROR,
                    "Please check the table name, the general format is [namespace::database.tablename]!");
        } finally {
            KuduReaderHelper.closeClient(kuduClient);
        }
    }
    public static void closeClient(KuduClient kuduClient) {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (KuduException e) {
            LOG.warn("The \"kudu client\" was not stopped gracefully. !");

        }
    }


}
