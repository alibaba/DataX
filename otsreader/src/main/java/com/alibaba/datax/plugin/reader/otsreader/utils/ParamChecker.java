package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.*;
import com.alicloud.openservices.tablestore.model.*;

import java.util.*;

public class ParamChecker {

    private static void throwNotExistException() {
        throw new IllegalArgumentException("missing the key.");
    }

    private static void throwStringLengthZeroException() {
        throw new IllegalArgumentException("input the key is empty string.");
    }

    public static String checkStringAndGet(Configuration param, String key, boolean isTrim) throws OTSCriticalException {
        try {
            String value = param.getString(key);
            if (isTrim) {
                value = value != null ? value.trim() : null;
            }
            if (null == value) {
                throwNotExistException();
            } else if (value.length() == 0) {
                throwStringLengthZeroException();
            }
            return value;
        } catch(RuntimeException e) {
            throw new OTSCriticalException("Parse '"+ key +"' fail, " + e.getMessage(), e);
        }
    }

    public static Direction checkDirectionAndEnd(TableMeta meta, List<PrimaryKeyColumn> begin, List<PrimaryKeyColumn> end) {
        Direction direction = null;
        int cmp = Common.compareRangeBeginAndEnd(meta, begin, end) ;

        if (cmp > 0) {
            direction = Direction.BACKWARD;
        } else if (cmp < 0) {
            direction = Direction.FORWARD;
        } else {
            throw new IllegalArgumentException("Value of 'range-begin' equal value of 'range-end'.");
        }
        return direction;
    }

    public static List<PrimaryKeyColumn> checkInputPrimaryKeyAndGet(TableMeta meta, List<PrimaryKeyValue> range) {
        if (meta.getPrimaryKeyMap().size() != range.size()) {
            throw new IllegalArgumentException(String.format(
                    "Input size of values not equal size of primary key. input size:%d, primary key size:%d .",
                    range.size(), meta.getPrimaryKeyMap().size()));
        }
        List<PrimaryKeyColumn> pk = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, PrimaryKeyType> e: meta.getPrimaryKeyMap().entrySet()) {
            PrimaryKeyValue value = range.get(i);
            if (e.getValue() != value.getType() && value != PrimaryKeyValue.INF_MIN && value != PrimaryKeyValue.INF_MAX) {
                throw new IllegalArgumentException(
                        "Input range type not match primary key. Input type:" + value.getType() + ", Primary Key Type:"+ e.getValue() +", Index:" + i
                );
            } else {
                pk.add(new PrimaryKeyColumn(e.getKey(), value));
            }
            i++;
        }
        return pk;
    }

    public static OTSRange checkRangeAndGet(Configuration param) throws OTSCriticalException {
        try {
            OTSRange range = new OTSRange();
            Map<String, Object> value = param.getMap(Key.RANGE);
            // 用户可以不用配置range，默认表示导出全表
            if (value == null) {
                return range;
            }

            /**
             * Range格式：{
             *  "begin":[],
             *  "end":[]
             * }
             */

            // begin
            // 如果不存在，表示从表开始位置读取
            Object arrayObj = value.get(Constant.ConfigKey.Range.BEGIN);
            if (arrayObj != null) {
                range.setBegin(ParamParser.parsePrimaryKeyColumnArray(arrayObj));
            }

            // end
            // 如果不存在，表示读取到表的结束位置
            arrayObj = value.get(Constant.ConfigKey.Range.END);
            if (arrayObj != null) {
                range.setEnd(ParamParser.parsePrimaryKeyColumnArray(arrayObj));
            }

            // split
            // 如果不存在，表示不做切分
            arrayObj = value.get(Constant.ConfigKey.Range.SPLIT);
            if (arrayObj != null) {
                range.setSplit(ParamParser.parsePrimaryKeyColumnArray(arrayObj));
            }

            return range;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'range' fail, " + e.getMessage(), e);
        }

    }

    public static TimeRange checkTimeRangeAndGet(Configuration param) throws OTSCriticalException {
        try {

            long begin = Constant.ConfigDefaultValue.TimeRange.MIN;
            long end = Constant.ConfigDefaultValue.TimeRange.MAX;

            Map<String, Object> value = param.getMap(Constant.ConfigKey.TIME_RANGE);
            // 用户可以不用配置time range，默认表示导出全表
            if (value == null) {
                return new TimeRange(begin, end);
            }

            /**
             * TimeRange格式：{
             *  "begin":,
             *  "end":
             * }
             */

            // begin
            // 如果不存在，表示从表开始位置读取
            Object obj = value.get(Constant.ConfigKey.TimeRange.BEGIN);
            if (obj != null) {
                begin = ParamParser.parseTimeRangeItem(obj, Constant.ConfigKey.TimeRange.BEGIN);
            }

            // end
            // 如果不存在，表示读取到表的结束位置
            obj = value.get(Constant.ConfigKey.TimeRange.END);
            if (obj != null) {
                end = ParamParser.parseTimeRangeItem(obj, Constant.ConfigKey.TimeRange.END);
            }

            TimeRange range = new TimeRange(begin, end);
            return range;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'timeRange' fail, " + e.getMessage(), e);
        }
    }

    private static void checkColumnByMode(List<OTSColumn> columns , OTSMode mode) {
        if (mode == OTSMode.MULTI_VERSION) {
            for (OTSColumn c : columns) {
                if (c.getColumnType() != OTSColumn.OTSColumnType.NORMAL) {
                    throw new IllegalArgumentException("in mode:'multiVersion', the 'column' only support specify column_name not const column.");
                }
            }
        } else {
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("in mode:'normal', the 'column' must specify at least one column_name or const column.");
            }
        }
    }

    public static List<OTSColumn> checkOTSColumnAndGet(Configuration param, OTSMode mode) throws OTSCriticalException {
        try {
            List<Object> value = param.getList(Key.COLUMN);
            // 用户可以不用配置Column
            if (value == null) {
                value = Collections.emptyList();
            }

            /**
             * Column格式：[
             *  {"Name":"pk1"},
             *  {"type":"Binary","value" : "base64()"}
             * ]
             */
            List<OTSColumn> columns = ParamParser.parseOTSColumnArray(value);
            checkColumnByMode(columns, mode);
            return columns;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'column' fail, " + e.getMessage(), e);
        }
    }

    public static List<OTSColumn> checkTimeseriesColumnAndGet(Configuration param) throws OTSCriticalException {
        try {
            List<Object> value = param.getList(Key.COLUMN);
            List<OTSColumn> columns = ParamParser.parseOTSColumnArray(value);

            List<ColumnType> columnTypes = checkColumnTypeAndGet(param);
            List<Boolean> isTags = checkColumnIsTagAndGet(param);

            for (int i = 0; i < columns.size(); i++) {
                columns.get(i).setValueType(columnTypes.get(i));
                columns.get(i).setTimeseriesTag(isTags.get(i));
            }

            checkColumnByMode(columns, OTSMode.NORMAL);
            return columns;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'column' fail, " + e.getMessage(), e);
        }
    }

    public static List<ColumnType> checkColumnTypeAndGet(Configuration param) throws OTSCriticalException {
        try {
            List<Object> value = param.getList(Key.COLUMN);
            List<ColumnType> columnTypes = ParamParser.parseColumnTypeArray(value);
            return columnTypes;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'type of column' fail, " + e.getMessage(), e);
        }
    }

    public static List<Boolean> checkColumnIsTagAndGet(Configuration param) throws OTSCriticalException {
        try {
            List<Object> value = param.getList(Key.COLUMN);
            List<Boolean> columnIsTag = ParamParser.parseColumnIsTagArray(value);
            return columnIsTag;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'isTag of column' fail, " + e.getMessage(), e);
        }
    }

    public static OTSMode checkModeAndGet(Configuration param) throws OTSCriticalException {
        try {
            String modeValue = param.getString(Key.MODE, "normal");
            if (modeValue.equalsIgnoreCase(Constant.ConfigDefaultValue.Mode.NORMAL)) {
                return OTSMode.NORMAL;
            } else if (modeValue.equalsIgnoreCase(Constant.ConfigDefaultValue.Mode.MULTI_VERSION)) {
                return OTSMode.MULTI_VERSION;
            } else {
                throw new IllegalArgumentException("the 'mode' only support 'normal' and 'multiVersion' not '"+ modeValue +"'.");
            }
        } catch(RuntimeException e) {
            throw new OTSCriticalException("Parse 'mode' fail, " + e.getMessage(), e);
        }
    }

    public static void checkTimeseriesMode(OTSMode mode, Boolean isNewVersion) throws OTSCriticalException {
        if (mode == OTSMode.MULTI_VERSION){
            throw new OTSCriticalException("Timeseries table do not support mode : multiVersion." );
        } else if (!isNewVersion){
            throw new OTSCriticalException("Timeseries table is only supported in newVersion, please set \"newVersion\": \"true\"." );
        }
    }

    public static List<PrimaryKeyColumn> checkAndGetPrimaryKey(
            List<PrimaryKeyColumn> pk,
            List<PrimaryKeySchema> pkSchema,
            String jsonKey){
        List<PrimaryKeyColumn> result = new ArrayList<PrimaryKeyColumn>();
        if(pk != null) {
            if (pk.size() > pkSchema.size()) {
                throw new IllegalArgumentException("The '"+ jsonKey +"', input primary key column size more than table meta, input size: "+ pk.size()
                        +", meta pk size:" + pkSchema.size());
            } else {
                //类型检查
                for (int i = 0; i < pk.size(); i++) {
                    PrimaryKeyValue pkc = pk.get(i).getValue();
                    PrimaryKeySchema pkcs = pkSchema.get(i);

                    if (!pkc.isInfMin() && !pkc.isInfMax() ) {
                        if (pkc.getType() != pkcs.getType()) {
                            throw new IllegalArgumentException(
                                    "The '"+ jsonKey +"', input primary key column type mismath table meta, input type:"+ pkc.getType()
                                    +", meta pk type:"+ pkcs.getType()
                                    +", index:" + i);
                        }
                    }
                    result.add(new PrimaryKeyColumn(pkcs.getName(), pkc));
                }
            }
            return result;
        } else {
            return new ArrayList<PrimaryKeyColumn>();
        }
    }

    /**
     * 检查split的类型是否和PartitionKey一致
     * @param points
     * @param pkSchema
     */
    private static List<PrimaryKeyColumn> checkAndGetSplit(
            List<PrimaryKeyColumn> points,
            List<PrimaryKeySchema> pkSchema){
        List<PrimaryKeyColumn> result = new ArrayList<PrimaryKeyColumn>();
        if (points == null) {
            return result;
        }

        // check 类型是否和PartitionKey一致即可
        PrimaryKeySchema partitionKeySchema = pkSchema.get(0);
        for (int i = 0 ; i < points.size(); i++) {
            PrimaryKeyColumn p = points.get(i);
            if (!p.getValue().isInfMin() && !p.getValue().isInfMax()) {
                if (p.getValue().getType() != partitionKeySchema.getType()) {
                    throw new IllegalArgumentException("The 'split', input primary key column type is mismatch partition key, input type: "+ p.getValue().getType().toString()
                            +", partition key type:" + partitionKeySchema.getType().toString()
                            +", index:" + i);
                }
            }
            result.add(new PrimaryKeyColumn(partitionKeySchema.getName(), p.getValue()));
        }

        return result;
    }

    public static void fillPrimaryKey(List<PrimaryKeySchema> pkSchema, List<PrimaryKeyColumn> pk, PrimaryKeyValue fillValue) {
        for(int i = pk.size(); i < pkSchema.size(); i++) {
            pk.add(new PrimaryKeyColumn(pkSchema.get(i).getName(), fillValue));
        }
    }

    private static void fillBeginAndEnd(
            List<PrimaryKeyColumn> begin,
            List<PrimaryKeyColumn> end,
            List<PrimaryKeySchema> pkSchema) {
        if (begin.isEmpty()) {
            fillPrimaryKey(pkSchema, begin, PrimaryKeyValue.INF_MIN);
        }
        if (end.isEmpty()) {
            fillPrimaryKey(pkSchema, end, PrimaryKeyValue.INF_MAX);
        }
        int cmp = CompareHelper.comparePrimaryKeyColumnList(begin, end);
        if (cmp == 0) {
            // begin.size()和end.size()理论上必然相等，但是考虑到语义的清晰性，显示的给出begin.size() == end.size()
            if (begin.size() == end.size() && begin.size() < pkSchema.size()) {
                fillPrimaryKey(pkSchema, begin, PrimaryKeyValue.INF_MIN);
                fillPrimaryKey(pkSchema, end, PrimaryKeyValue.INF_MAX);
            } else {
                throw new IllegalArgumentException("The 'begin' can not be equal with 'end'.");
            }
        } else if (cmp < 0) { // 升序
            fillPrimaryKey(pkSchema, begin, PrimaryKeyValue.INF_MIN);
            fillPrimaryKey(pkSchema, end, PrimaryKeyValue.INF_MAX);
        } else { // 降序
            fillPrimaryKey(pkSchema, begin, PrimaryKeyValue.INF_MAX);
            fillPrimaryKey(pkSchema, end, PrimaryKeyValue.INF_MIN);
        }
    }

    private static void checkBeginAndEndAndSplit(
            List<PrimaryKeyColumn> begin,
            List<PrimaryKeyColumn> end,
            List<PrimaryKeyColumn> split) {
        int cmp = CompareHelper.comparePrimaryKeyColumnList(begin, end);

        if (!split.isEmpty()) {
            if (cmp < 0) { // 升序
                // 检查是否是升序
                for (int i = 0 ; i < split.size() - 1; i++) {
                    PrimaryKeyColumn before = split.get(i);
                    PrimaryKeyColumn after = split.get(i + 1);
                    if (before.compareTo(after) >=0) { // 升序
                        throw new IllegalArgumentException("In 'split', the item value is not increasing, index: " + i);
                    }
                }
                if (begin.get(0).compareTo(split.get(0)) >= 0) {
                    throw new IllegalArgumentException("The 'begin' must be less than head of 'split'.");
                }
                if (split.get(split.size() - 1).compareTo(end.get(0)) >= 0) {
                    throw new IllegalArgumentException("tail of 'split' must be less than 'end'.");
                }
            } else if (cmp > 0) {// 降序
                // 检查是否是降序
                for (int i = 0 ; i < split.size() - 1; i++) {
                    PrimaryKeyColumn before = split.get(i);
                    PrimaryKeyColumn after = split.get(i + 1);
                    if (before.compareTo(after) <= 0) { // 升序
                        throw new IllegalArgumentException("In 'split', the item value is not descending, index: " + i);
                    }
                }
                if (begin.get(0).compareTo(split.get(0)) <= 0) {
                    throw new IllegalArgumentException("The 'begin' must be large than head of 'split'.");
                }
                if (split.get(split.size() - 1).compareTo(end.get(0)) <= 0) {
                    throw new IllegalArgumentException("tail of 'split' must be large than 'end'.");
                }
            } else {
                throw new IllegalArgumentException("The 'begin' can not equal with 'end'.");
            }
        }
    }

    /**
     * 填充不完整的PK
     * 检查Begin、End、Split 3者之间的关系是否符合预期
     * @param begin
     * @param end
     * @param split
     */
    private static void fillAndcheckBeginAndEndAndSplit(
            List<PrimaryKeyColumn> begin,
            List<PrimaryKeyColumn> end,
            List<PrimaryKeyColumn> split,
            List<PrimaryKeySchema> pkSchema
            ) {

        fillBeginAndEnd(begin, end, pkSchema);
        checkBeginAndEndAndSplit(begin, end, split);
    }

    public static void checkAndSetOTSRange(OTSRange range, TableMeta meta) throws OTSCriticalException {
        try {
            List<PrimaryKeySchema> pkSchema = meta.getPrimaryKeyList();

            // 检查是begin和end否和PK类型一致
            range.setBegin(checkAndGetPrimaryKey(range.getBegin(), pkSchema, Constant.ConfigKey.Range.BEGIN));
            range.setEnd(checkAndGetPrimaryKey(range.getEnd(), pkSchema, Constant.ConfigKey.Range.END));
            range.setSplit(checkAndGetSplit(range.getSplit(), pkSchema));

            // 1.填充Begin和End
            // 2.检查begin,end,split顺序是否正确
            fillAndcheckBeginAndEndAndSplit(range.getBegin(), range.getEnd(), range.getSplit(), pkSchema);
        } catch(RuntimeException e) {
            throw new OTSCriticalException("Parse 'range' fail, " + e.getMessage(), e);
        }
    }

    public static void checkAndSetColumn(List<OTSColumn> columns, TableMeta meta, OTSMode mode) throws OTSCriticalException {
        try {
            if (mode == OTSMode.MULTI_VERSION) {
                Set<String> uniqueColumn = new HashSet<String>();
                Map<String, PrimaryKeyType> pk = meta.getPrimaryKeyMap();
                for (OTSColumn c : columns) {
                    // 是否包括PK列
                    if (pk.get(c.getName()) != null) {
                        throw new IllegalArgumentException("in mode:'multiVersion', the 'column' can not include primary key column, input:"+ c.getName() +".");
                    }
                    // 是否有重复列
                    if (uniqueColumn.contains(c.getName())) {
                        throw new IllegalArgumentException("in mode:'multiVersion', the 'column' can not include  same column, input:"+ c.getName() +".");
                    } else {
                        uniqueColumn.add(c.getName());
                    }
                }
            }

        } catch(RuntimeException e) {
            throw new OTSCriticalException("Parse 'column' fail, " + e.getMessage(), e);
        }
    }

    public static void normalCheck(OTSConf conf) {
        // 旧版本不支持multiVersion模式
        if(!conf.isNewVersion() && conf.getMode() == OTSMode.MULTI_VERSION){
            throw new IllegalArgumentException("in mode:'multiVersion' :The old version do not support multiVersion mode.  Please add config in otsreader: \"newVersion\":\"true\" .");
        }
    }

    public static void checkAndSetOTSConf(OTSConf conf, TableMeta meta) throws OTSCriticalException {
        normalCheck(conf);
        checkAndSetOTSRange(conf.getRange(), meta);
        checkAndSetColumn(conf.getColumn(), meta, conf.getMode());
    }

}
