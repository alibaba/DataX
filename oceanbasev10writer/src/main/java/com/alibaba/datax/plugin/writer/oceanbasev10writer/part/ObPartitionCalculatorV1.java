package com.alibaba.datax.plugin.writer.oceanbasev10writer.part;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;
import com.alipay.oceanbase.obproxy.data.TableEntryKey;
import com.alipay.oceanbase.obproxy.util.ObPartitionIdCalculator;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OceanBase 1.x和2.x的分区计算
 *
 * @author cjyyz
 * @date 2023/02/07
 * @since
 */
public class ObPartitionCalculatorV1 implements IObPartCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(ObPartitionCalculatorV1.class);

    /**
     * 分区键的位置
     */
    private List<Integer> partIndexes;

    /**
     * 表的全部字段名
     */
    private List<String> columnNames;

    /**
     * ocj partition calculator
     */
    private ObPartitionIdCalculator calculator;

    /**
     * @param connectInfo
     * @param table
     * @param columns
     */
    public ObPartitionCalculatorV1(ServerConnectInfo connectInfo, String table, List<String> columns) {

        initCalculator(connectInfo, table);

        if (Objects.isNull(calculator)) {
            LOG.warn("partCalculator is null");
            return;
        }

        this.partIndexes = new ArrayList<>(columns.size());
        this.columnNames = new ArrayList<>(columns);

        for (int i = 0; i < columns.size(); ++i) {
            String columnName = columns.get(i);
            if (calculator.isPartitionKeyColumn(columnName)) {
                LOG.info(columnName + " is partition key.");
                partIndexes.add(i);
            }
        }
    }

    /**
     * @param record
     * @return Long
     */
    @Override
    public Long calculate(Record record) {
        if (Objects.isNull(calculator)) {
            return null;
        }

        for (Integer i : partIndexes) {
            calculator.addColumn(columnNames.get(i), record.getColumn(i).asString());
        }
        return calculator.calculate();
    }

    /**
     * @param connectInfo
     * @param table
     */
    private void initCalculator(ServerConnectInfo connectInfo, String table) {

        LOG.info(String.format("create tableEntryKey with clusterName %s, tenantName %s, databaseName %s, tableName %s",
            connectInfo.clusterName, connectInfo.tenantName, connectInfo.databaseName, table));
        TableEntryKey tableEntryKey = new TableEntryKey(connectInfo.clusterName, connectInfo.tenantName,
            connectInfo.databaseName, table);

        int retry = 0;

        do {
            try {
                if (retry > 0) {
                    TimeUnit.SECONDS.sleep(1);
                    LOG.info("retry create new part calculator {} times", retry);
                }
                LOG.info("create partCalculator with address: " + connectInfo.ipPort);
                calculator = new ObPartitionIdCalculator(connectInfo.ipPort, tableEntryKey);
            } catch (Exception ex) {
                ++retry;
                LOG.warn("create new part calculator failed, retry: {}", ex.getMessage());
            }
        } while (calculator == null && retry < 3);
    }
}