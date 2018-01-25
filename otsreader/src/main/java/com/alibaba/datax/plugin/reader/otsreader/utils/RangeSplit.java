package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSPrimaryKeyColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

/**
 * 主要提供对范围的解析
 */
public class RangeSplit {

    private static String bigIntegerToString(BigInteger baseValue,
            BigInteger bValue, BigInteger multi, int lenOfString) {
        BigInteger tmp = bValue;
        StringBuilder sb = new StringBuilder();
        for (int tmpLength = 0; tmpLength < lenOfString; tmpLength++) {
            sb.insert(0,
                    (char) (baseValue.add(tmp.remainder(multi)).intValue()));
            tmp = tmp.divide(multi);
        }
        return sb.toString();
    }

    /**
     * 切分String的Unicode Unit
     * 
     * 注意：该方法只支持begin小于end
     * 
     * @param beginStr
     * @param endStr
     * @param count
     * @return
     */
    private static List<String> splitCodePoint(int begin, int end, int count) {
        if (begin >= end) {
            throw new IllegalArgumentException("Only support begin < end.");
        }

        List<String> results = new ArrayList<String>();
        BigInteger beginBig = BigInteger.valueOf(begin);
        BigInteger endBig = BigInteger.valueOf(end);
        BigInteger countBig = BigInteger.valueOf(count);
        BigInteger multi = endBig.subtract(beginBig).add(BigInteger.ONE);
        BigInteger range = endBig.subtract(beginBig);
        BigInteger interval = BigInteger.ZERO;
        int length = 1;

        BigInteger tmpBegin = BigInteger.ZERO;
        BigInteger tmpEnd = endBig.subtract(beginBig);

        // 扩大之后的数值
        BigInteger realBegin = tmpBegin;
        BigInteger realEnd = tmpEnd;

        while (range.compareTo(countBig) < 0) { // 不够切分
            realEnd = realEnd.multiply(multi).add(tmpEnd);
            range = realEnd.subtract(realBegin);
            length++;
        }

        interval = range.divide(countBig);

        BigInteger cur = realBegin;

        for (int i = 0; i < (count - 1); i++) {
            results.add(bigIntegerToString(beginBig, cur, multi, length));
            cur = cur.add(interval);
        }
        results.add(bigIntegerToString(beginBig, realEnd, multi, length));
        return results;
    }

    /**
     * 注意： 当begin和end相等时，函数将返回空的List
     * 
     * @param begin
     * @param end
     * @param count
     * @return
     */
    public static List<String> splitStringRange(String begin, String end, int count) {

        if (count <= 1) {
            throw new IllegalArgumentException("Input count <= 1 .");
        }

        List<String> results = new ArrayList<String>();

        int beginValue = 0;
        if (!begin.isEmpty()) {
            beginValue = begin.codePointAt(0);
        }
        int endValue = 0;
        if (!end.isEmpty()) {
            endValue = end.codePointAt(0);
        }

        int cmp = beginValue - endValue;

        if (cmp == 0) {
            return results;
        }

        results.add(begin);

        Comparator<String> comparator = new Comparator<String>(){
            public int compare(String arg0, String arg1) {
                return arg0.compareTo(arg1);
            }
        };

        List<String> tmp = null;

        if (cmp > 0) { // 如果是逆序，则 reverse Comparator
            comparator = Collections.reverseOrder(comparator);
            tmp = splitCodePoint(endValue, beginValue, count);
        } else { // 正序
            tmp = splitCodePoint(beginValue, endValue, count);
        }

        Collections.sort(tmp, comparator); 

        for (String value : tmp) {
            if (comparator.compare(value, begin) > 0 && comparator.compare(value, end)  < 0) {
                results.add(value);
            }
        }

        results.add(end);

        return results;
    }
    
    /**
     * begin 一定要小于 end
     * @param begin
     * @param end
     * @param count
     * @return
     */
    private static List<Long> splitIntegerRange(BigInteger bigBegin, BigInteger bigEnd, BigInteger bigCount) {
        List<Long> is = new ArrayList<Long>();
        
        BigInteger interval = (bigEnd.subtract(bigBegin)).divide(bigCount);
        BigInteger cur = bigBegin;
        BigInteger i = BigInteger.ZERO;
        while (cur.compareTo(bigEnd) < 0 && i.compareTo(bigCount) < 0) {
            is.add(cur.longValue());
            cur = cur.add(interval);
            i = i.add(BigInteger.ONE);
        }
        is.add(bigEnd.longValue());
        return is;
    }

    /**
     * 切分数值类型 注意： 当begin和end相等时，函数将返回空的List
     * 
     * @param begin
     * @param end
     * @param count
     * @return
     */
    public static List<Long> splitIntegerRange(long begin, long end, int count) {

        if (count <= 1) {
            throw new IllegalArgumentException("Input count <= 1 .");
        }
        List<Long> is = new ArrayList<Long>();

        BigInteger bigBegin = BigInteger.valueOf(begin);
        BigInteger bigEnd = BigInteger.valueOf(end);
        BigInteger bigCount = BigInteger.valueOf(count);

        BigInteger abs = (bigEnd.subtract(bigBegin)).abs();
        
        if (abs.compareTo(BigInteger.ZERO) == 0) { // partition key 相等的情况
            return is;
        }

        if (bigCount.compareTo(abs) > 0) {
            bigCount = abs;
        }
        
        if (bigEnd.subtract(bigBegin).compareTo(BigInteger.ZERO) > 0) { // 正向
            return splitIntegerRange(bigBegin, bigEnd, bigCount);
        } else { // 逆向
            List<Long> tmp = splitIntegerRange(bigEnd, bigBegin, bigCount);
            
            Comparator<Long> comparator = new Comparator<Long>(){
                public int compare(Long arg0, Long arg1) {
                    return arg0.compareTo(arg1);
                }
            };
            
            Collections.sort(tmp,Collections.reverseOrder(comparator));
            return tmp;
        }
    }

    public static List<PrimaryKeyValue> splitRangeByPrimaryKeyType(
            PrimaryKeyType type, PrimaryKeyValue begin, PrimaryKeyValue end,
            int count) {
        List<PrimaryKeyValue> result = new ArrayList<PrimaryKeyValue>();
        if (type == PrimaryKeyType.STRING) {
            List<String> points = splitStringRange(begin.asString(),
                    end.asString(), count);
            for (String s : points) {
                result.add(PrimaryKeyValue.fromString(s));
            }
        } else {
            List<Long> points = splitIntegerRange(begin.asLong(), end.asLong(),
                    count);
            for (Long l : points) {
                result.add(PrimaryKeyValue.fromLong(l));
            }
        }
        return result;
    }

    public static List<OTSRange> rangeSplitByCount(TableMeta meta,
            RowPrimaryKey begin, RowPrimaryKey end, int count) {
        List<OTSRange> results = new ArrayList<OTSRange>();

        OTSPrimaryKeyColumn partitionKey = Common.getPartitionKey(meta);

        PrimaryKeyValue beginPartitionKey = begin.getPrimaryKey().get(
                partitionKey.getName());
        PrimaryKeyValue endPartitionKey = end.getPrimaryKey().get(
                partitionKey.getName());

        // 第一，先对PartitionKey列进行拆分

        List<PrimaryKeyValue> ranges = RangeSplit.splitRangeByPrimaryKeyType(
                partitionKey.getType(), beginPartitionKey, endPartitionKey,
                count);

        if (ranges.isEmpty()) {
            return results;
        }

        int size = ranges.size();
        for (int i = 0; i < size - 1; i++) {
            RowPrimaryKey bPk = new RowPrimaryKey();
            RowPrimaryKey ePk = new RowPrimaryKey();

            bPk.addPrimaryKeyColumn(partitionKey.getName(), ranges.get(i));
            ePk.addPrimaryKeyColumn(partitionKey.getName(), ranges.get(i + 1));

            results.add(new OTSRange(bPk, ePk));
        }

        // 第二，填充非PartitionKey的ParimaryKey列
        // 注意：在填充过程中，需要使用用户给定的Begin和End来替换切分出来的第一个Range
        // 的Begin和最后一个Range的End

        List<String> keys = new ArrayList<String>(meta.getPrimaryKey().size());
        keys.addAll(meta.getPrimaryKey().keySet());

        for (int i = 0; i < results.size(); i++) {
            for (int j = 1; j < keys.size(); j++) {
                OTSRange c = results.get(i);
                RowPrimaryKey beginPK = c.getBegin();
                RowPrimaryKey endPK = c.getEnd();
                String key = keys.get(j);
                if (i == 0) { // 第一行
                    beginPK.addPrimaryKeyColumn(key,
                            begin.getPrimaryKey().get(key));
                    endPK.addPrimaryKeyColumn(key, PrimaryKeyValue.INF_MIN);
                } else if (i == results.size() - 1) {// 最后一行
                    beginPK.addPrimaryKeyColumn(key, PrimaryKeyValue.INF_MIN);
                    endPK.addPrimaryKeyColumn(key, end.getPrimaryKey().get(key));
                } else {
                    beginPK.addPrimaryKeyColumn(key, PrimaryKeyValue.INF_MIN);
                    endPK.addPrimaryKeyColumn(key, PrimaryKeyValue.INF_MIN);
                }
            }
        }
        return results;
    }

    private static List<PrimaryKeyValue> getCompletePK(int num,
            PrimaryKeyValue value) {
        List<PrimaryKeyValue> values = new ArrayList<PrimaryKeyValue>();
        for (int j = 0; j < num; j++) {
            if (j == 0) {
                values.add(value);
            } else {
                // 这里在填充PK时，系统需要选择特定的值填充于此
                // 系统默认填充INF_MIN
                values.add(PrimaryKeyValue.INF_MIN);
            }
        }
        return values;
    }

    /**
     * 根据输入的范围begin和end，从target中取得对应的point
     * @param begin
     * @param end
     * @param target
     * @return
     */
    public static List<PrimaryKeyValue> getSplitPoint(PrimaryKeyValue begin, PrimaryKeyValue end, List<PrimaryKeyValue> target) {
        List<PrimaryKeyValue> result = new ArrayList<PrimaryKeyValue>();

        int cmp = Common.primaryKeyValueCmp(begin, end); 

        if (cmp == 0) {
            return result;
        }

        result.add(begin);

        Comparator<PrimaryKeyValue> comparator = new Comparator<PrimaryKeyValue>(){
            public int compare(PrimaryKeyValue arg0, PrimaryKeyValue arg1) {
                return Common.primaryKeyValueCmp(arg0, arg1);
            }
        };

        if (cmp > 0) { // 如果是逆序，则 reverse Comparator
            comparator = Collections.reverseOrder(comparator);
        }

        Collections.sort(target, comparator); 

        for (PrimaryKeyValue value:target) {
            if (comparator.compare(value, begin) > 0 && comparator.compare(value, end) < 0) {
                result.add(value);
            }
        }
        result.add(end);

        return result;
    }

    public static List<OTSRange> rangeSplitByPoint(TableMeta meta, RowPrimaryKey beginPK, RowPrimaryKey endPK,
            List<PrimaryKeyValue> splits) {
        
        List<OTSRange> results = new ArrayList<OTSRange>();

        int pkCount = meta.getPrimaryKey().size();

        String partName = Common.getPartitionKey(meta).getName();
        PrimaryKeyValue begin = beginPK.getPrimaryKey().get(partName);
        PrimaryKeyValue end = endPK.getPrimaryKey().get(partName);

        List<PrimaryKeyValue> newSplits = getSplitPoint(begin, end, splits);

        if (newSplits.isEmpty()) {
            return results;
        }

        for (int i = 0; i < newSplits.size() - 1; i++) {
            OTSRange item = new OTSRange(
                    ParamChecker.checkInputPrimaryKeyAndGet(meta,
                            getCompletePK(pkCount, newSplits.get(i))),
                            ParamChecker.checkInputPrimaryKeyAndGet(meta,
                                    getCompletePK(pkCount, newSplits.get(i + 1))));
            results.add(item);
        }
        // replace first and last
        OTSRange first = results.get(0);
        OTSRange last = results.get(results.size() - 1);

        first.setBegin(beginPK);
        last.setEnd(endPK);
        return results;
    }
}
