package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsUtil;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReaderProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderProxy.class);
    private static boolean IS_DEBUG = LOG.isDebugEnabled();

    private RecordSender recordSender;
    private TableTunnel.DownloadSession downloadSession;
    private Map<String, OdpsType> columnTypeMap;
    private List<Pair<String, ColumnType>> parsedColumns;
    private String partition;
    private boolean isPartitionTable;

    private long start;
    private long count;
    private boolean isCompress;

    public ReaderProxy(RecordSender recordSender, TableTunnel.DownloadSession downloadSession,
            Map<String, OdpsType> columnTypeMap,
            List<Pair<String, ColumnType>> parsedColumns, String partition,
            boolean isPartitionTable, long start, long count, boolean isCompress) {
        this.recordSender = recordSender;
        this.downloadSession = downloadSession;
        this.columnTypeMap = columnTypeMap;
        this.parsedColumns = parsedColumns;
        this.partition = partition;
        this.isPartitionTable = isPartitionTable;
        this.start = start;
        this.count = count;
        this.isCompress = isCompress;
    }

    // warn: odps 分区列和正常列不能重名, 所有列都不不区分大小写
    public void doRead() {
        try {
            LOG.info("start={}, count={}",start, count);
            //RecordReader recordReader = downloadSession.openRecordReader(start, count, isCompress);
            RecordReader recordReader = OdpsUtil.getRecordReader(downloadSession, start, count, isCompress);

            Record odpsRecord;
            Map<String, String> partitionMap = this
                    .parseCurrentPartitionValue();

            int retryTimes = 1;
            while (true) {
                try {
                    odpsRecord = recordReader.read();
                } catch(Exception e) {
                    //odps read 异常后重试10次
                    LOG.warn("warn : odps read exception: {}", e.getMessage());
                    if(retryTimes < 10) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ignored) {
                        }
                        recordReader = downloadSession.openRecordReader(start, count, isCompress);
                        LOG.warn("odps-read-exception, 重试第{}次", retryTimes);
                        retryTimes++;
                        continue;
                    } else {
                        throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_READ_EXCEPTION, e);
                    }
                }
                //记录已经读取的点
                start++;
                count--;

                if (odpsRecord != null) {

                    com.alibaba.datax.common.element.Record dataXRecord = recordSender
                            .createRecord();
                    // warn: for PARTITION||NORMAL columnTypeMap's key
                    // sets(columnName) is big than parsedColumns's left
                    // sets(columnName), always contain
                    for (Pair<String, ColumnType> pair : this.parsedColumns) {
                        String columnName = pair.getLeft();
                        switch (pair.getRight()) {
                            case PARTITION:
                                String partitionColumnValue = this
                                        .getPartitionColumnValue(partitionMap,
                                                columnName);
                                this.odpsColumnToDataXField(odpsRecord, dataXRecord,
                                        this.columnTypeMap.get(columnName),
                                        partitionColumnValue, true);
                                break;
                            case NORMAL:
                                this.odpsColumnToDataXField(odpsRecord, dataXRecord,
                                        this.columnTypeMap.get(columnName), columnName,
                                        false);
                                break;
                            case CONSTANT:
                                dataXRecord.addColumn(new StringColumn(columnName));
                                break;
                            default:
                                break;
                        }
                    }
                    recordSender.sendToWriter(dataXRecord);
                } else {
                    break;
                }
            }
            //fixed, 避免recordReader.close失败，跟鸣天确认过，可以不用关闭RecordReader
            try {
                recordReader.close();
            } catch (Exception e) {
                LOG.warn("recordReader close exception", e);
            }
        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            // warn: if dirty
            throw DataXException.asDataXException(
                    OdpsReaderErrorCode.READ_DATA_FAIL, e);
        }
    }

    private Map<String, String> parseCurrentPartitionValue() {
        Map<String, String> partitionMap = new HashMap<String, String>();
        if (this.isPartitionTable) {
            String[] splitedPartition = this.partition.split(",");
            for (String eachPartition : splitedPartition) {
                String[] partitionDetail = eachPartition.split("=");
                // warn: check partition like partition=1
                if (2 != partitionDetail.length) {
                    throw DataXException
                            .asDataXException(
                                    OdpsReaderErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您的分区 [%s] 解析出现错误,解析后正确的配置方式类似为 [ pt=1,dt=1 ].",
                                            eachPartition));
                }
                // warn: translate to lower case, it's more comfortable to
                // compare whit user's input columns
                String partitionName = partitionDetail[0].toLowerCase();
                String partitionValue = partitionDetail[1];
                partitionMap.put(partitionName, partitionValue);
            }
        }
        if (IS_DEBUG) {
            LOG.debug(String.format("partition value details: %s",
                    com.alibaba.fastjson.JSON.toJSONString(partitionMap)));
        }
        return partitionMap;
    }

    private String getPartitionColumnValue(Map<String, String> partitionMap,
            String partitionColumnName) {
        // warn: to lower case
        partitionColumnName = partitionColumnName.toLowerCase();
        // it's will never happen, but add this checking
        if (!partitionMap.containsKey(partitionColumnName)) {
            String errorMessage = String.format(
                    "表所有分区信息为: %s 其中找不到 [%s] 对应的分区值.",
                    com.alibaba.fastjson.JSON.toJSONString(partitionMap),
                    partitionColumnName);
            throw DataXException.asDataXException(
                    OdpsReaderErrorCode.READ_DATA_FAIL, errorMessage);
        }
        return partitionMap.get(partitionColumnName);
    }

    /**
     * TODO warn: odpsRecord 的 String 可能获取出来的是 binary
     * 
     * warn: there is no dirty data in reader plugin, so do not handle dirty
     * data with TaskPluginCollector
     * 
     * warn: odps only support BIGINT && String partition column actually
     * 
     * @param odpsRecord
     *            every line record of odps table
     * @param dataXRecord
     *            every datax record, to be send to writer. method getXXX() case sensitive
     * @param type
     *            odps column type
     * @param columnNameValue
     *            for partition column it's column value, for normal column it's
     *            column name
     * @param isPartitionColumn
     *            true means partition column and false means normal column
     * */
    private void odpsColumnToDataXField(Record odpsRecord,
            com.alibaba.datax.common.element.Record dataXRecord, OdpsType type,
            String columnNameValue, boolean isPartitionColumn) {
        switch (type) {
        case BIGINT: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new LongColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new LongColumn(odpsRecord
                        .getBigint(columnNameValue)));
            }
            break;
        }
        case BOOLEAN: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new BoolColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new BoolColumn(odpsRecord
                        .getBoolean(columnNameValue)));
            }
            break;
        }
        case DATETIME: {
            if (isPartitionColumn) {
                try {
                    dataXRecord.addColumn(new DateColumn(ColumnCast
                            .string2Date(new StringColumn(columnNameValue))));
                } catch (ParseException e) {
                    LOG.error(String.format("", this.partition));
                    String errMessage = String.format(
                            "您读取分区 [%s] 出现日期转换异常, 日期的字符串表示为 [%s].",
                            this.partition, columnNameValue);
                    LOG.error(errMessage);
                    throw DataXException.asDataXException(
                            OdpsReaderErrorCode.READ_DATA_FAIL, errMessage, e);
                }
            } else {
                dataXRecord.addColumn(new DateColumn(odpsRecord
                        .getDatetime(columnNameValue)));
            }

            break;
        }
        case DOUBLE: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new DoubleColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new DoubleColumn(odpsRecord
                        .getDouble(columnNameValue)));
            }
            break;
        }
        case DECIMAL: {
            if(isPartitionColumn) {
                dataXRecord.addColumn(new DoubleColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new DoubleColumn(odpsRecord.getDecimal(columnNameValue)));
            }
            break;
        }
        case STRING: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new StringColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new StringColumn(odpsRecord
                        .getString(columnNameValue)));
            }
            break;
        }
        default:
            throw DataXException
                    .asDataXException(
                            OdpsReaderErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "DataX 抽取 ODPS 数据不支持字段类型为:[%s]. 目前支持抽取的字段类型有：bigint, boolean, datetime, double, decimal, string. "
                                            + "您可以选择不抽取 DataX 不支持的字段或者联系 ODPS 管理员寻求帮助.",
                                    type));
        }
    }

}
