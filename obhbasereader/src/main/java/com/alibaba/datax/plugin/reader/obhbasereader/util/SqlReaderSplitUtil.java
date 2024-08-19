package com.alibaba.datax.plugin.reader.obhbasereader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SplitedSlice;
import com.alibaba.datax.plugin.reader.obhbasereader.Key;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ExecutorTemplate;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartInfo;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartitionSplitUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlReaderSplitUtil {
    public static final String SAMPLE_SQL_TEMPLATE = "SELECT `hex` FROM (SELECT `hex`,K  , bucket, ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY K) rn FROM(SELECT %s `hex`, K ,NTILE(%s) OVER "
            + "(ORDER BY K ) bucket FROM (SELECT hex(K) as `hex`, K FROM %s SAMPLE BLOCK(%s)) a) b) c WHERE rn = 1 GROUP BY K ORDER BY K";
    public static final String MIDDLE_RANGE_TEMPLATE = "((K) > (unhex('%s'))) AND ((K) <= (unhex('%s')))";
    public static final String MIN_MAX_RANGE_TEMPLATE = "((K)<= (unhex('%s'))) or ((K) > (unhex('%s')))";
    private static final Logger LOG = LoggerFactory.getLogger(SqlReaderSplitUtil.class);

    public static List<Configuration> splitSingleTable(Configuration configuration, String tableName, String columnFamily, int eachTableShouldSplittedNumber, boolean readByPartition) {
        List<String> partitionList = Lists.newArrayList();
        String tableNameWithCf = tableName + "$" + columnFamily;
        PartInfo partInfo = PartitionSplitUtil.getObMySQLPartInfoBySQL(configuration, tableNameWithCf);
        if (partInfo.isPartitionTable()) {
            partitionList.addAll(partInfo.getPartList());
        }
        // read all partitions and split job only by partition
        if (readByPartition) {
            LOG.info("table: [{}] will read only by partition", tableNameWithCf);
            return splitSingleTableByPartition(configuration, partitionList);
        }

        if (eachTableShouldSplittedNumber <= 1) {
            LOG.info("total enable splitted number of table: [{}] is {}, no need to split", tableNameWithCf, eachTableShouldSplittedNumber);
            return Lists.newArrayList(configuration);
        }

        // If user specified some partitions to be read,
        List<String> userSetPartitions = configuration.getList(Key.PARTITION_NAME, String.class);
        if (CollectionUtils.isNotEmpty(userSetPartitions)) {
            Set<String> partSet = new HashSet<>(partitionList);
            // If partition name does not exist in the table, throw exception directly. Case is sensitive.
            userSetPartitions.forEach(e -> Preconditions.checkArgument(partSet.contains(e), "partition %s does not exist in table: %s", e, tableNameWithCf));
            partitionList.clear();
            partitionList.addAll(userSetPartitions);
        }

        if (partitionList.isEmpty()) {
            LOG.info("table: [{}] is not partitioned, just split table by rowKey.", tableNameWithCf);
            List<Configuration> splitConfs = splitSingleTableByRowKey(configuration, tableNameWithCf, eachTableShouldSplittedNumber);
            LOG.info("total split count of non-partitioned table :[{}] is {}", tableNameWithCf, splitConfs.size());
            return splitConfs;
        } else {
            ExecutorTemplate<List<Configuration>> template = new ExecutorTemplate<>("split-rows-by-rowkey-" + tableNameWithCf + "-", eachTableShouldSplittedNumber);
            int splitNumPerPartition = (int) Math.ceil(1.0d * eachTableShouldSplittedNumber / partitionList.size());
            LOG.info("table: [{}] is partitioned, split table by rowKey in parallel. splitNumPerPartition is {}", tableNameWithCf, splitNumPerPartition);
            for (String partName : partitionList) {
                try {
                    template.submit(() -> {
                        Configuration tempConf = configuration.clone();
                        tempConf.set(Key.PARTITION_NAME, partName);
                        return splitSingleTableByRowKey(tempConf, tableNameWithCf, splitNumPerPartition);
                    });
                } catch (Throwable th) {
                    LOG.error("submit split task of table: [{}-{}] failed, reason: {}", tableNameWithCf, partName, th.getMessage(), th);
                }
            }
            List<Configuration> splitConfs = template.waitForResult().stream().flatMap(Collection::stream).collect(Collectors.toList());
            LOG.info("total split count of partitioned table :[{}] is {}", tableNameWithCf, splitConfs.size());
            return splitConfs;
        }
    }

    private static List<Configuration> splitSingleTableByPartition(Configuration configuration, List<String> partList) {
        if (partList == null || partList.isEmpty()) {
            return Lists.newArrayList(configuration);
        }
        List<Configuration> confList = new ArrayList<>();
        for (String partName : partList) {
            LOG.info("read sub task: reading from partition " + partName);
            Configuration conf = configuration.clone();
            conf.set(Key.PARTITION_NAME, partName);
            confList.add(conf);
        }
        return confList;
    }

    /**
     * @param configuration
     * @param tableNameWithCf
     * @param eachTableShouldSplittedNumber
     * @return
     */
    public static List<Configuration> splitSingleTableByRowKey(Configuration configuration, String tableNameWithCf, int eachTableShouldSplittedNumber) {
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String hint = configuration.getString(Key.READER_HINT, com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_READ_HINT);
        String partInfo = "";
        String partName = configuration.getString(Key.PARTITION_NAME, null);
        if (partName != null) {
            partInfo = " partition(" + partName + ")";
        }
        tableNameWithCf += partInfo;
        int fetchSize = configuration.getInt(Constant.FETCH_SIZE, com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_FETCH_SIZE);
        Double percentage = configuration.getDouble(Key.SAMPLE_PERCENTAGE, 0.1);
        List<SplitedSlice> slices = new ArrayList<>();
        List<Configuration> pluginParams = new ArrayList<>();
        // set ob_query_timeout and ob_trx_timeout to a large time in case timeout
        int queryTimeoutSeconds = 60 * 60 * 48;
        try (Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcURL, username, password)) {
            String setQueryTimeout = "set ob_query_timeout=" + (queryTimeoutSeconds * 1000 * 1000L);
            String setTrxTimeout = "set ob_trx_timeout=" + ((queryTimeoutSeconds + 5) * 1000 * 1000L);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(setQueryTimeout);
                stmt.execute(setTrxTimeout);
            } catch (Exception e) {
                LOG.warn("set ob_query_timeout and set ob_trx_timeout failed. reason: {}", e.getMessage(), e);
            }
            slices = getSplitSqlBySample(conn, tableNameWithCf, fetchSize, percentage, eachTableShouldSplittedNumber, hint);
        } catch (Throwable e) {
            LOG.warn("query rowkey range failed of table: {}. reason: {}. the table will not be splitted.", tableNameWithCf, e.getMessage(), e);
        }

        if (!slices.isEmpty()) {
            for (SplitedSlice slice : slices) {
                Configuration tempConfig = configuration.clone();
                tempConfig.set(Key.RANGE, slice.getRange());
                pluginParams.add(tempConfig);
            }
        } else {
            Configuration tempConfig = configuration.clone();
            pluginParams.add(tempConfig);
        }
        return pluginParams;
    }

    /**
     * 按照采样方法切分，不能直接顺序切分否则可能导致原本属于一行的数据被切分为两行
     *
     * @param conn
     * @param tableName
     * @param fetchSize
     * @param percentage
     * @param adviceNum
     * @param hint
     * @return List<SplitedSlice>
     * @throws SQLException
     */
    private static List<SplitedSlice> getSplitSqlBySample(Connection conn, String tableName, int fetchSize, double percentage, int adviceNum, String hint) throws SQLException {
        String splitSql = String.format(SAMPLE_SQL_TEMPLATE, hint, adviceNum, tableName, percentage);
        LOG.info("split pk [sql={}] is running... ", splitSql);
        List<String> boundList = new ArrayList<>();
        try (ResultSet rs = DBUtil.query(conn, splitSql, fetchSize)) {
            while (rs.next()) {
                boundList.add(rs.getString(1));
            }
        }
        if (boundList.size() == 0) {
            return new ArrayList<>();
        }
        List<SplitedSlice> rangeSql = new ArrayList<>();
        for (int i = 0; i < boundList.size() - 1; i++) {
            String range = String.format(MIDDLE_RANGE_TEMPLATE, boundList.get(i), boundList.get(i + 1));
            SplitedSlice slice = new SplitedSlice(boundList.get(i), boundList.get(i + 1), range);
            rangeSql.add(slice);
        }
        String range = String.format(MIN_MAX_RANGE_TEMPLATE, boundList.get(0), boundList.get(boundList.size() - 1));
        SplitedSlice slice = new SplitedSlice(null, null, range);
        rangeSql.add(slice);
        return rangeSql;
    }
}
