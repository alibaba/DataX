package com.alibaba.datax.plugin.writer.oceanbasev10writer.part;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.oceanbase.partition.calculator.ObPartIdCalculator;
import com.oceanbase.partition.calculator.enums.ObServerMode;
import com.oceanbase.partition.calculator.helper.TableEntryExtractor;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.oceanbase.partition.calculator.model.TableEntryKey;
import com.oceanbase.partition.metadata.desc.ObPartColumn;
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
    private ObPartIdCalculator calculator;

    TableEntry tableEntry=null;

    /**
     * @param connectInfo
     * @param table
     * @param columns
     * @param isOracleCompatibleMode
     */
    public ObPartitionCalculatorV1(ServerConnectInfo connectInfo, String table, List<String> columns,Boolean isOracleCompatibleMode) {

        initCalculator(connectInfo, table,isOracleCompatibleMode);

        if (Objects.isNull(calculator)) {
            LOG.warn("partCalculator is null");
            return;
        }

        this.partIndexes = new ArrayList<>(columns.size());
        this.columnNames = new ArrayList<>(columns);

        for (int i = 0; i < columns.size(); ++i) {
            String columnName = columns.get(i);
            if (isPartitionKeyColumn(columnName)) {
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
        List<Object> records = new ArrayList<>();
        for (Integer i : partIndexes) {
            calculator.addRefColumn(columnNames.get(i), record.getColumn(i).asString());
            records.add(record.getColumn(i).asString());
        }
        return calculator.calculatePartId(records.toArray());
    }

    /**
     * @param connectInfo
     * @param table
     * @param isOracleCompatibleMode
     */
    private void initCalculator(ServerConnectInfo connectInfo, String table, Boolean isOracleCompatibleMode) {

        LOG.info(String.format("create tableEntryKey with clusterName %s, tenantName %s, databaseName %s, tableName %s",
                connectInfo.clusterName, connectInfo.tenantName, connectInfo.databaseName, table));
        TableEntryKey tableEntryKey=null;
        if (isOracleCompatibleMode){
            tableEntryKey=new TableEntryKey(connectInfo.clusterName, connectInfo.tenantName,
                    connectInfo.databaseName, table, ObServerMode.fromOracle("3.2.3.3"));
        }else {
            tableEntryKey=new TableEntryKey(connectInfo.clusterName, connectInfo.tenantName,
                    connectInfo.databaseName, table, ObServerMode.fromMySql("3.2.3.3"));
        }

        int retry = 0;

        do {
            try {
                if (retry > 0) {
                    TimeUnit.SECONDS.sleep(1);
                    LOG.info("retry create new part calculator {} times", retry);
                }
                LOG.info("create partCalculator with address: " + connectInfo.ipPort);
                Connection conn = DriverManager.getConnection(connectInfo.jdbcUrl, connectInfo.userName, connectInfo.password);
                TableEntry entry = new TableEntryExtractor().queryTableEntry(conn, tableEntryKey, false);
                calculator = new ObPartIdCalculator(entry);
            } catch (Exception ex) {
                ++retry;
                LOG.warn("create new part calculator failed, retry: {}", ex.getMessage());
            }
        } while (calculator == null && retry < 3);
    }

    /**
     * @param name
     * @return
     */
    public boolean isPartitionKeyColumn(String name) {
        if (tableEntry.isNonPartitionTable()) {
            return false;
        } else {
            String realName = getRealColumnName(name);
            Iterator partColumnIterator = this.tableEntry.getTablePart().getPartColumns().iterator();

            ObPartColumn column;
            do {
                if (!partColumnIterator.hasNext()) {
                    return false;
                }

                column = (ObPartColumn) partColumnIterator.next();
            } while(!column.getColumnName().equalsIgnoreCase(realName));

            return true;
        }
    }

    /**
     * @param name
     * @return
     */
    private static String getRealColumnName(String name) {
        String realColumnName = name;
        if (name != null && name.length() > 2 && name.startsWith("`") && name.endsWith("`")) {
            realColumnName = name.substring(1, name.length() - 1);
        }

        return realColumnName;
    }


}