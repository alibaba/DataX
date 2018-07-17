package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSPrimaryKeyColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

public class ParamChecker {

    private static void throwNotExistException(String key) {
        throw new IllegalArgumentException("The param '" + key + "' is not exist.");
    }

    private static void throwStringLengthZeroException(String key) {
        throw new IllegalArgumentException("The param length of '" + key + "' is zero.");
    }

    private static void throwEmptyException(String key) {
        throw new IllegalArgumentException("The param  '" + key + "' is empty.");
    }

    private static void throwNotListException(String key) {
        throw new IllegalArgumentException("The param  '" + key + "' is not a json array.");
    }

    private static void throwNotMapException(String key) {
        throw new IllegalArgumentException("The param  '" + key + "' is not a json map.");
    }

    public static String checkStringAndGet(Configuration param, String key) {
        String value = param.getString(key);
        if (null == value) {
            throwNotExistException(key);
        } else if (value.length() == 0) {
            throwStringLengthZeroException(key);
        }
        return value;
    }

    public static List<Object> checkListAndGet(Configuration param, String key, boolean isCheckEmpty) {
        List<Object> value = null;
        try {
            value = param.getList(key);
        } catch (ClassCastException e) {
            throwNotListException(key);
        }
        if (null == value) {
            throwNotExistException(key);
        } else if (isCheckEmpty && value.isEmpty()) {
            throwEmptyException(key);
        }
        return value;
    }

    public static List<Object> checkListAndGet(Map<String, Object> range, String key) {
        Object obj =  range.get(key);
        if (null == obj) {
            return null;
        }
        return checkListAndGet(range, key, false);
    }

    public static List<Object> checkListAndGet(Map<String, Object> range, String key, boolean isCheckEmpty) {
        Object obj =  range.get(key);
        if (null == obj) {
            throwNotExistException(key);
        }
        if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> value = (List<Object>)obj;
            if (isCheckEmpty && value.isEmpty()) {
                throwEmptyException(key);
            }
            return value;
        } else {
            throw new IllegalArgumentException("Can not parse list of '" + key + "' from map.");
        }
    }

    public static List<Object> checkListAndGet(Map<String, Object> range, String key, List<Object> defaultList) {
        Object obj =  range.get(key);
        if (null == obj) {
            return defaultList;
        }
        if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> value = (List<Object>)obj;
            return value;
        } else {
            throw new IllegalArgumentException("Can not parse list of '" + key + "' from map.");
        }
    }

    public static Map<String, Object> checkMapAndGet(Configuration param, String key, boolean isCheckEmpty) {
        Map<String, Object> value = null;
        try {
            value = param.getMap(key);
        } catch (ClassCastException e) {
            throwNotMapException(key);
        }
        if (null == value) {
            throwNotExistException(key);
        } else if (isCheckEmpty && value.isEmpty()) {
            throwEmptyException(key);
        }
        return value;
    }

    public static RowPrimaryKey checkInputPrimaryKeyAndGet(TableMeta meta, List<PrimaryKeyValue> range) {
        if (meta.getPrimaryKey().size() != range.size()) {
            throw new IllegalArgumentException(String.format(
                    "Input size of values not equal size of primary key. input size:%d, primary key size:%d .",
                    range.size(), meta.getPrimaryKey().size()));
        }
        RowPrimaryKey pk = new RowPrimaryKey();
        int i = 0;
        for (Entry<String, PrimaryKeyType> e: meta.getPrimaryKey().entrySet()) {
            PrimaryKeyValue value = range.get(i);
            if (e.getValue() != value.getType() && value != PrimaryKeyValue.INF_MIN && value != PrimaryKeyValue.INF_MAX) {
                throw new IllegalArgumentException(
                        "Input range type not match primary key. Input type:" + value.getType() + ", Primary Key Type:"+ e.getValue() +", Index:" + i
                        );
            } else {
                pk.addPrimaryKeyColumn(e.getKey(), value);
            }
            i++;
        }
        return pk;
    }

    public static OTSRange checkRangeAndGet(TableMeta meta, List<PrimaryKeyValue> begin, List<PrimaryKeyValue> end) {
        OTSRange range = new OTSRange();
        if (begin.size() == 0 && end.size() == 0) {
            RowPrimaryKey beginRow = new RowPrimaryKey();
            RowPrimaryKey endRow = new RowPrimaryKey();
            for (String name : meta.getPrimaryKey().keySet()) {
                beginRow.addPrimaryKeyColumn(name, PrimaryKeyValue.INF_MIN);
                endRow.addPrimaryKeyColumn(name, PrimaryKeyValue.INF_MAX);
            }
            range.setBegin(beginRow);
            range.setEnd(endRow);
        } else {
            RowPrimaryKey beginRow = checkInputPrimaryKeyAndGet(meta, begin);
            RowPrimaryKey endRow = checkInputPrimaryKeyAndGet(meta, end);
            range.setBegin(beginRow);
            range.setEnd(endRow);
        }
        return range;
    }

    public static Direction checkDirectionAndEnd(TableMeta meta, RowPrimaryKey begin, RowPrimaryKey end) {
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

    /**
     * 检查类型是否一致，是否重复，方向是否一致
     * @param direction
     * @param before
     * @param after
     */
    private static void checkDirection(Direction direction, PrimaryKeyValue before, PrimaryKeyValue after) {
        int cmp = Common.primaryKeyValueCmp(before, after);
        if (cmp > 0) { // 反向
            if (direction == Direction.FORWARD) {
                throw new IllegalArgumentException("Input direction of 'range-split' is FORWARD, but direction of 'range' is BACKWARD.");
            }
        } else if (cmp < 0) { // 正向
            if (direction == Direction.BACKWARD) {
                throw new IllegalArgumentException("Input direction of 'range-split' is BACKWARD, but direction of 'range' is FORWARD.");
            }
        } else { // 重复列
            throw new IllegalArgumentException("Multi same column in 'range-split'.");
        }
    }

    /**
     * 检查 points中的所有点是否是在Begin和end之间
     * @param begin
     * @param end
     * @param points
     */
    private static void checkPointsRange(Direction direction, PrimaryKeyValue begin, PrimaryKeyValue end, List<PrimaryKeyValue> points) {
        if (direction == Direction.FORWARD) {
            if (!(Common.primaryKeyValueCmp(begin, points.get(0)) < 0 && Common.primaryKeyValueCmp(end, points.get(points.size() - 1)) > 0)) {
                throw new IllegalArgumentException("The item of 'range-split' is not within scope of 'range-begin' and 'range-end'.");
            }
        } else {
            if (!(Common.primaryKeyValueCmp(begin, points.get(0)) > 0 && Common.primaryKeyValueCmp(end, points.get(points.size() - 1)) < 0)) {
                throw new IllegalArgumentException("The item of 'range-split' is not within scope of 'range-begin' and 'range-end'.");
            }
        }
    }

    /**
     * 1.检测用户的输入类型是否和PartitionKey一致
     * 2.顺序是否和Range一致
     * 3.是否有重复列
     * 4.检查points的范围是否在range内
     * @param meta
     * @param points
     */
    public static void checkInputSplitPoints(TableMeta meta, OTSRange range, Direction direction, List<PrimaryKeyValue> points) {
        if (null == points || points.isEmpty()) {
            return;
        }

        OTSPrimaryKeyColumn part = Common.getPartitionKey(meta);

        // 处理第一个
        PrimaryKeyValue item = points.get(0);
        if ( item.getType() != part.getType()) {
            throw new IllegalArgumentException("Input type of 'range-split' not match partition key. "
                    + "Item of 'range-split' type:" + item.getType()+ ", Partition type:" + part.getType());
        }

        for (int i = 0 ; i < points.size() - 1; i++) {
            PrimaryKeyValue before = points.get(i);
            PrimaryKeyValue after = points.get(i + 1);
            checkDirection(direction, before, after);
        }

        PrimaryKeyValue begin = range.getBegin().getPrimaryKey().get(part.getName());
        PrimaryKeyValue end   = range.getEnd().getPrimaryKey().get(part.getName());

        checkPointsRange(direction, begin, end, points);
    }
}
