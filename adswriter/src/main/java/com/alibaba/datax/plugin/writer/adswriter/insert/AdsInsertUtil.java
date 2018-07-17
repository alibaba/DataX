package com.alibaba.datax.plugin.writer.adswriter.insert;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.AdsException;
import com.alibaba.datax.plugin.writer.adswriter.AdsWriterErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.ads.ColumnInfo;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.load.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.Constant;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AdsInsertUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(AdsInsertUtil.class);

    public static TableInfo getAdsTableInfo(Configuration conf) {
        AdsHelper adsHelper = AdsUtil.createAdsHelper(conf);
        TableInfo tableInfo= null;
        try {
            tableInfo = adsHelper.getTableInfo(conf.getString(Key.ADS_TABLE));
        } catch (AdsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.GET_ADS_TABLE_MEATA_FAILED, e);
        }
        return tableInfo;
    }

    /*
     * 返回列顺序为ads建表列顺序
     * */
    public static List<String> getAdsTableColumnNames(Configuration conf) {
        List<String> tableColumns = new ArrayList<String>();
        AdsHelper adsHelper = AdsUtil.createAdsHelper(conf);
        TableInfo tableInfo= null;
        String adsTable = conf.getString(Key.ADS_TABLE);
        try {
            tableInfo = adsHelper.getTableInfo(adsTable);
        } catch (AdsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.GET_ADS_TABLE_MEATA_FAILED, e);
        }

        List<ColumnInfo> columnInfos = tableInfo.getColumns();
        for(ColumnInfo columnInfo: columnInfos) {
            tableColumns.add(columnInfo.getName());
        }

        LOG.info("table:[{}] all columns:[\n{}\n].", adsTable, StringUtils.join(tableColumns, ","));
        return tableColumns;
    }

    public static Map<String, Pair<Integer,String>> getColumnMetaData
            (Configuration configuration, List<String> userColumns) {
        Map<String, Pair<Integer,String>> columnMetaData = new HashMap<String, Pair<Integer,String>>();
        List<ColumnInfo> columnInfoList = getAdsTableColumns(configuration);
        for(String column : userColumns) {
            if (column.startsWith(Constant.ADS_QUOTE_CHARACTER) && column.endsWith(Constant.ADS_QUOTE_CHARACTER)) {
                column = column.substring(1, column.length() - 1);
            }
            for (ColumnInfo columnInfo : columnInfoList) {
                if(column.equalsIgnoreCase(columnInfo.getName())) {
                    Pair<Integer,String> eachPair = new ImmutablePair<Integer, String>(columnInfo.getDataType().sqlType, columnInfo.getDataType().name);
                    columnMetaData.put(columnInfo.getName(), eachPair);
                }
            }
        }
        return columnMetaData;
    }
    
    public static Map<String, Pair<Integer,String>> getColumnMetaData(TableInfo tableInfo, List<String> userColumns){
        Map<String, Pair<Integer,String>> columnMetaData = new HashMap<String, Pair<Integer,String>>();
        List<ColumnInfo> columnInfoList = tableInfo.getColumns();
        for(String column : userColumns) {
            if (column.startsWith(Constant.ADS_QUOTE_CHARACTER) && column.endsWith(Constant.ADS_QUOTE_CHARACTER)) {
                column = column.substring(1, column.length() - 1);
            }
            for (ColumnInfo columnInfo : columnInfoList) {
                if(column.equalsIgnoreCase(columnInfo.getName())) {
                    Pair<Integer,String> eachPair = new ImmutablePair<Integer, String>(columnInfo.getDataType().sqlType, columnInfo.getDataType().name);
                    columnMetaData.put(columnInfo.getName(), eachPair);
                }
            }
        }
        return columnMetaData;
    }

    /*
     * 返回列顺序为ads建表列顺序
     * */
    public static List<ColumnInfo> getAdsTableColumns(Configuration conf) {
        AdsHelper adsHelper = AdsUtil.createAdsHelper(conf);
        TableInfo tableInfo= null;
        String adsTable = conf.getString(Key.ADS_TABLE);
        try {
            tableInfo = adsHelper.getTableInfo(adsTable);
        } catch (AdsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.GET_ADS_TABLE_MEATA_FAILED, e);
        }

        List<ColumnInfo> columnInfos = tableInfo.getColumns();

        return columnInfos;
    }

    public static void dealColumnConf(Configuration originalConfig, List<String> tableColumns) {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
        } else {
            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, tableColumns);
            } else if (userConfiguredColumns.size() > tableColumns.size()) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), tableColumns.size()));
            } else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);
                // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
                // ListUtil.makeSureBInA(tableColumns, userConfiguredColumns, true);
                // 支持关键字和保留字, ads列是不区分大小写的
                List<String> removeQuotedColumns = new ArrayList<String>();
                for (String each : userConfiguredColumns) {
                    if (each.startsWith(Constant.ADS_QUOTE_CHARACTER) && each.endsWith(Constant.ADS_QUOTE_CHARACTER)) {
                        removeQuotedColumns.add(each.substring(1, each.length() - 1));
                    } else {
                        removeQuotedColumns.add(each);
                    }
                }
                ListUtil.makeSureBInA(tableColumns, removeQuotedColumns, false);
            }
        }
    }
}
