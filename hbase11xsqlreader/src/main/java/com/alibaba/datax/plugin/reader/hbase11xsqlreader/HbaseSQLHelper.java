package com.alibaba.datax.plugin.reader.hbase11xsqlreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HbaseSQLHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSQLHelper.class);

    public static org.apache.hadoop.conf.Configuration generatePhoenixConf(HbaseSQLReaderConfig readerConfig) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        String table = readerConfig.getTableName();
        List<String> columns = readerConfig.getColumns();
        String zkUrl = readerConfig.getZkUrl();

        PhoenixConfigurationUtil.setInputClass(conf, PhoenixRecordWritable.class);
        PhoenixConfigurationUtil.setInputTableName(conf, table);

        if (!columns.isEmpty()) {
            PhoenixConfigurationUtil.setSelectColumnNames(conf, columns.toArray(new String[columns.size()]));
        }
        PhoenixEmbeddedDriver.ConnectionInfo info = null;
        try {
            info = PhoenixEmbeddedDriver.ConnectionInfo.create(zkUrl);
        } catch (SQLException e) {
            throw DataXException.asDataXException(
                    HbaseSQLReaderErrorCode.GET_PHOENIX_CONNECTIONINFO_ERROR, "通过zkURL获取phoenix的connectioninfo出错，请检查hbase集群服务是否正常", e);
        }
        conf.set(HConstants.ZOOKEEPER_QUORUM, info.getZookeeperQuorum());
        if (info.getPort() != null)
            conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, info.getPort());
        if (info.getRootNode() != null)
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, info.getRootNode());
        return conf;
    }

    public static List<String> getPColumnNames(String connectionString, String tableName) throws SQLException {
        Connection con =
                DriverManager.getConnection(connectionString);
        PhoenixConnection phoenixConnection = con.unwrap(PhoenixConnection.class);
        MetaDataClient metaDataClient = new MetaDataClient(phoenixConnection);
        PTable table = metaDataClient.updateCache("", tableName).getTable();
        List<String> columnNames = new ArrayList<String>();
        for (PColumn pColumn : table.getColumns()) {
            if (!pColumn.getName().getString().equals(SaltingUtil.SALTING_COLUMN_NAME))
                columnNames.add(pColumn.getName().getString());
            else
                LOG.info(tableName + " is salt table");
        }
        return columnNames;
    }


    public static List<Configuration> split(HbaseSQLReaderConfig readerConfig) {
        PhoenixInputFormat inputFormat = new PhoenixInputFormat<PhoenixRecordWritable>();
        org.apache.hadoop.conf.Configuration conf = generatePhoenixConf(readerConfig);
        JobID jobId = new JobID(Key.MOCK_JOBID_IDENTIFIER, Key.MOCK_JOBID);
        JobContextImpl jobContext = new JobContextImpl(conf, jobId);
        List<Configuration> resultConfigurations = new ArrayList<Configuration>();
        List<InputSplit> rawSplits = null;
        try {
            rawSplits = inputFormat.getSplits(jobContext);
            LOG.info("split size is " + rawSplits.size());
            for (InputSplit split : rawSplits) {
                Configuration cfg = readerConfig.getOriginalConfig().clone();

                byte[] splitSer = HadoopSerializationUtil.serialize((PhoenixInputSplit) split);
                String splitBase64Str = org.apache.commons.codec.binary.Base64.encodeBase64String(splitSer);
                cfg.set(Key.SPLIT_KEY, splitBase64Str);
                resultConfigurations.add(cfg);
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(
                    HbaseSQLReaderErrorCode.GET_PHOENIX_SPLITS_ERROR, "获取表的split信息时出现了异常，请检查hbase集群服务是否正常," + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw DataXException.asDataXException(
                    HbaseSQLReaderErrorCode.GET_PHOENIX_SPLITS_ERROR, "获取表的split信息时被中断，请重试，若还有问题请联系datax管理员," + e.getMessage(), e);
        }

        return resultConfigurations;
    }

    public static HbaseSQLReaderConfig parseConfig(Configuration cfg) {
        return HbaseSQLReaderConfig.parse(cfg);
    }

    public static Pair<String, String> getHbaseConfig(String hbaseCfgString) {
        assert hbaseCfgString != null;
        Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseCfgString, new TypeReference<Map<String, String>>() {
        });
        String zkQuorum = hbaseConfigMap.get(Key.HBASE_ZK_QUORUM);
        String znode = hbaseConfigMap.get(Key.HBASE_ZNODE_PARENT);
        if(znode == null)
            znode = "";
        return new Pair<String, String>(zkQuorum, znode);
    }
}
