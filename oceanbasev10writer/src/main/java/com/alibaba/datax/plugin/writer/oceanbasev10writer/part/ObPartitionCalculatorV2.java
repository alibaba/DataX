package com.alibaba.datax.plugin.writer.oceanbasev10writer.part;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.DbUtils;
import com.oceanbase.partition.calculator.ObPartIdCalculator;
import com.oceanbase.partition.calculator.enums.ObPartLevel;
import com.oceanbase.partition.calculator.enums.ObServerMode;
import com.oceanbase.partition.calculator.helper.TableEntryExtractor;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.oceanbase.partition.calculator.model.TableEntryKey;
import com.oceanbase.partition.calculator.model.Version;
import com.oceanbase.partition.metadata.desc.ObPartColumn;
import com.oceanbase.partition.metadata.desc.ObTablePart;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OceanBase 3.x和4.x的分区计算
 *
 * @author cjyyz
 * @date 2023/02/07
 * @since
 */
public class ObPartitionCalculatorV2 implements IObPartCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(ObPartitionCalculatorV2.class);

    /**
     * OB的模式以及版本信息
     */
    private ObServerMode mode;

    /**
     * ob-partition-calculator 分区计算组件
     */
    private ObPartIdCalculator calculator;

    /**
     * 记录columns的字段名和在record中的位置。
     * 当目标表结构的分区键是生成列时，calculator 需要从改结构中获取到生成列所依赖的字段的值
     * e.g.
     * create table t1 (
     * c1 varchar(20),
     * c2 varchar(20) generated always as (substr(`c1`,1,8))
     * )partition by key(c2) partitions 5
     *
     * 此时，columnNameIndexMap包含的元素是 c1:0
     * 需要将c1字段的值从columnNameIndexMap中添加到{@link com.oceanbase.partition.calculator.ObPartIdCalculator#getRefColumnValues()}
     */
    private Map<String, Integer> columnNameIndexMap;

    /**
     * @param connectInfo
     * @param table
     * @param mode
     */
    public ObPartitionCalculatorV2(ServerConnectInfo connectInfo, String table, ObServerMode mode, List<String> columns) {
        this.mode = mode;
        this.columnNameIndexMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnNameIndexMap.put(columns.get(i).toLowerCase(), i);
        }
        initCalculator(connectInfo, table);
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
        if (!calculator.getTableEntry().isPartitionTable()) {
            return 0L;
        }
        return calculator.calculatePartId(filterNullableColumns(record));
    }

    /**
     * 初始化分区计算组件
     *
     * @param connectInfo
     * @param table
     */
    private void initCalculator(ServerConnectInfo connectInfo, String table) {
        TableEntryKey tableEntryKey = new TableEntryKey(connectInfo.clusterName, connectInfo.tenantName, connectInfo.databaseName, table, mode);
        boolean subsequentFromV4 = !mode.getVersion().isOlderThan(new Version("4.0.0.0"));
        try {
            TableEntry tableEntry;
            try (Connection conn = getConnection(connectInfo, subsequentFromV4)){
                TableEntryExtractor extractor = new TableEntryExtractor();
                tableEntry = extractor.queryTableEntry(conn, tableEntryKey,subsequentFromV4);
            }
            this.calculator = new ObPartIdCalculator(false, tableEntry, subsequentFromV4);
        } catch (Exception e) {
            LOG.warn("create new part calculator failed. reason: {}", e.getMessage());
        }
    }

    private Connection getConnection(ServerConnectInfo connectInfo, boolean subsequentFromV4) throws Exception {
        // OceanBase 4.0.0.0及之后版本均使用业务租户连接计算分区
        if (subsequentFromV4) {
            return DBUtil.getConnection(DataBaseType.OceanBase, connectInfo.jdbcUrl, connectInfo.getFullUserName(), connectInfo.password);
        }
        // OceanBase 4.0.0.0之前版本使用sys租户连接计算分区
        return DbUtils.buildSysConn(connectInfo.jdbcUrl, connectInfo.clusterName);
    }

    /**
     * 只选择分区字段值传入分区计算组件
     *
     * @param record
     * @return Object[]
     */
    private Object[] filterNullableColumns(Record record) {
        final ObTablePart tablePart = calculator.getTableEntry().getTablePart();

        final Object[] filteredRecords = new Object[record.getColumnNumber()];

        if (tablePart.getLevel().getIndex() > ObPartLevel.LEVEL_ZERO.getIndex()) {
            // 从record中添加非生成列的一级分区值到filteredRecords数组中
            for (ObPartColumn partColumn : tablePart.getPartColumns()) {
                if (partColumn.getColumnExpr() == null) {
                    int metaIndex = partColumn.getColumnIndex();
                    String columnName = partColumn.getColumnName().toLowerCase();
                    int idxInRecord = columnNameIndexMap.get(columnName);
                    filteredRecords[metaIndex] = record.getColumn(idxInRecord).asString();
                }

            }
            // 从record中添加生成列的一级分区值到calculator的redColumnMap中,ObTablePart.getRefPartColumns中的字段名均为小写
            for (ObPartColumn partColumn : tablePart.getRefPartColumns()) {
                String columnName = partColumn.getColumnName();
                int index = columnNameIndexMap.get(columnName);
                calculator.addRefColumn(columnName, record.getColumn(index).asString());
            }
        }

        if (tablePart.getLevel().getIndex() >= ObPartLevel.LEVEL_TWO.getIndex()) {
            // 从record中添加非生成列的二级分区值到filteredRecords数组中
            for (ObPartColumn partColumn : tablePart.getSubPartColumns()) {
                if (partColumn.getColumnExpr() == null) {
                    int metaIndex = partColumn.getColumnIndex();
                    String columnName = partColumn.getColumnName().toLowerCase();
                    int idxInRecord = columnNameIndexMap.get(columnName);
                    filteredRecords[metaIndex] = record.getColumn(idxInRecord).asString();
                }

            }
            // 从record中添加生成列的二级分区值到calculator的redColumnMap中,ObTablePart.getRefSubPartColumns中的字段名均为小写
            for (ObPartColumn partColumn : tablePart.getRefSubPartColumns()) {
                String columnName = partColumn.getColumnName();
                int index = columnNameIndexMap.get(columnName);
                calculator.addRefColumn(columnName, record.getColumn(index).asString());
            }
        }
        return filteredRecords;
    }
}