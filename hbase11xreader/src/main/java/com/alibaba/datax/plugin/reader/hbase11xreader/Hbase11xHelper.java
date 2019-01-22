package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 工具类
 * Created by shf on 16/3/7.
 */
public class Hbase11xHelper {

    private static final Logger LOG = LoggerFactory.getLogger(Hbase11xHelper.class);
    private static org.apache.hadoop.hbase.client.Connection H_CONNECTION = null;

    public synchronized static org.apache.hadoop.hbase.client.Connection getHbaseConnection(String hbaseConfig) {
        if(H_CONNECTION != null && !H_CONNECTION.isClosed()){
            return H_CONNECTION;
        }
        if (StringUtils.isBlank(hbaseConfig)) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.REQUIRED_VALUE, "读 Hbase 时需要配置hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
        }
        org.apache.hadoop.conf.Configuration hConfiguration = HBaseConfiguration.create();
        try {
            Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseConfig, new TypeReference<Map<String, String>>() {});
            // 用户配置的 key-value 对 来表示 hbaseConfig
            Validate.isTrue(hbaseConfigMap != null && hbaseConfigMap.size() !=0, "hbaseConfig不能为空Map结构!");
            for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet()) {
                hConfiguration.set(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.GET_HBASE_CONNECTION_ERROR, e);
        }
        try {
            H_CONNECTION = ConnectionFactory.createConnection(hConfiguration);
        } catch (Exception e) {
            Hbase11xHelper.closeConnection(H_CONNECTION);
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.GET_HBASE_CONNECTION_ERROR, e);
        }
        return H_CONNECTION;
    }


    public static Table getTable(com.alibaba.datax.common.util.Configuration configuration){
        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        org.apache.hadoop.hbase.client.Table hTable = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin,hTableName);
            hTable = hConnection.getTable(hTableName);
        } catch (Exception e) {
            Hbase11xHelper.closeTable(hTable);
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.GET_HBASE_TABLE_ERROR, e);
        }
        return hTable;
    }

   public static RegionLocator getRegionLocator(com.alibaba.datax.common.util.Configuration configuration){
       String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
       String userTable = configuration.getString(Key.TABLE);
       org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
       TableName hTableName = TableName.valueOf(userTable);
       org.apache.hadoop.hbase.client.Admin admin = null;
       RegionLocator regionLocator = null;
       try {
           admin = hConnection.getAdmin();
           Hbase11xHelper.checkHbaseTable(admin,hTableName);
           regionLocator = hConnection.getRegionLocator(hTableName);
       } catch (Exception e) {
           Hbase11xHelper.closeRegionLocator(regionLocator);
           Hbase11xHelper.closeAdmin(admin);
           Hbase11xHelper.closeConnection(hConnection);
           throw DataXException.asDataXException(Hbase11xReaderErrorCode.GET_HBASE_REGINLOCTOR_ERROR, e);
       }
       return regionLocator;

   }

    public synchronized static void closeConnection(Connection hConnection) {
        try {
            if(null != hConnection)
                hConnection.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.CLOSE_HBASE_CONNECTION_ERROR, e);
        }
    }

    public static void closeAdmin(Admin admin){
        try {
            if(null != admin)
                admin.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.CLOSE_HBASE_ADMIN_ERROR, e);
        }
    }

    public static void closeTable(Table table){
        try {
            if(null != table)
                table.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.CLOSE_HBASE_TABLE_ERROR, e);
        }
    }

    public static void closeResultScanner(ResultScanner resultScanner){
        if(null != resultScanner) {
            resultScanner.close();
        }
    }

    public static void closeRegionLocator(RegionLocator regionLocator){
        try {
            if(null != regionLocator)
                regionLocator.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.CLOSE_HBASE_REGINLOCTOR_ERROR, e);
        }
    }


    public static  void checkHbaseTable(Admin admin,  TableName hTableName) throws IOException {
        if(!admin.tableExists(hTableName)){
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "HBase源头表" + hTableName.toString()
                    + "不存在, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(!admin.isTableAvailable(hTableName)){
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "HBase源头表" +hTableName.toString()
                    + " 不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(admin.isTableDisabled(hTableName)){
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "HBase源头表" +hTableName.toString()
                    + "is disabled, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
    }


    public static byte[] convertUserStartRowkey(com.alibaba.datax.common.util.Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);
            return Hbase11xHelper.stringToBytes(startRowkey, isBinaryRowkey);
        }
    }

    public static byte[] convertUserEndRowkey(com.alibaba.datax.common.util.Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);
            return Hbase11xHelper.stringToBytes(endRowkey, isBinaryRowkey);
        }
    }

    /**
     * 注意：convertUserStartRowkey 和 convertInnerStartRowkey，前者会受到 isBinaryRowkey 的影响，只用于第一次对用户配置的 String 类型的 rowkey 转为二进制时使用。而后者约定：切分时得到的二进制的 rowkey 回填到配置中时采用
     */
    public static byte[] convertInnerStartRowkey(Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(startRowkey);
    }

    public static byte[] convertInnerEndRowkey(Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(endRowkey);
    }


    private static byte[] stringToBytes(String rowkey, boolean isBinaryRowkey) {
        if (isBinaryRowkey) {
            return Bytes.toBytesBinary(rowkey);
        } else {
            return Bytes.toBytes(rowkey);
        }
    }


    public static boolean isRowkeyColumn(String columnName) {
        return Constant.ROWKEY_FLAG.equalsIgnoreCase(columnName);
    }


    /**
     * 用于解析 Normal 模式下的列配置
     */
    public static List<HbaseColumnCell> parseColumnOfNormalMode(List<Map> column) {
        List<HbaseColumnCell> hbaseColumnCells = new ArrayList<HbaseColumnCell>();

        HbaseColumnCell oneColumnCell;

        for (Map<String, String> aColumn : column) {
            ColumnType type = ColumnType.getByTypeName(aColumn.get(Key.TYPE));
            String columnName = aColumn.get(Key.NAME);
            String columnValue = aColumn.get(Key.VALUE);
            String dateformat = aColumn.get(Key.FORMAT);

            if (type == ColumnType.DATE) {

                if(dateformat == null){
                    dateformat = Constant.DEFAULT_DATA_FORMAT;
                }
                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "Hbasereader 在 normal 方式读取时则要么是 type + name + format 的组合，要么是type + value + format 的组合. 而您的配置非这两种组合，请检查并修改.");

                oneColumnCell = new HbaseColumnCell
                        .Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .dateformat(dateformat)
                        .build();
            } else {
                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "Hbasereader 在 normal 方式读取时，其列配置中，如果类型不是时间，则要么是 type + name 的组合，要么是type + value 的组合. 而您的配置非这两种组合，请检查并修改.");
                oneColumnCell = new HbaseColumnCell.Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .build();
            }

            hbaseColumnCells.add(oneColumnCell);
        }

        return hbaseColumnCells;
    }

    //将多竖表column变成<familyQualifier,<>>形式
    public static HashMap<String,HashMap<String,String>> parseColumnOfMultiversionMode(List<Map> column){

        HashMap<String,HashMap<String,String>> familyQualifierMap = new HashMap<String,HashMap<String,String>>();
        for (Map<String, String> aColumn : column) {
            String type = aColumn.get(Key.TYPE);
            String columnName = aColumn.get(Key.NAME);
            String dateformat = aColumn.get(Key.FORMAT);

            ColumnType.getByTypeName(type);
            Validate.isTrue(StringUtils.isNotBlank(columnName), "Hbasereader 中，column 需要配置列名称name,格式为 列族:列名，您的配置为空,请检查并修改.");

            String familyQualifier;
            if( !Hbase11xHelper.isRowkeyColumn(columnName)){
                String[] cfAndQualifier = columnName.split(":");
                if ( cfAndQualifier.length != 2) {
                    throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + columnName);
                }
                familyQualifier = StringUtils.join(cfAndQualifier[0].trim(),":",cfAndQualifier[1].trim());
            }else{
                familyQualifier = columnName.trim();
            }

            HashMap<String,String> typeAndFormat = new  HashMap<String,String>();
            typeAndFormat.put(Key.TYPE,type);
            typeAndFormat.put(Key.FORMAT,dateformat);
            familyQualifierMap.put(familyQualifier,typeAndFormat);
        }
        return familyQualifierMap;
    }

    public static List<Configuration> split(Configuration configuration) {
        byte[] startRowkeyByte = Hbase11xHelper.convertUserStartRowkey(configuration);
        byte[] endRowkeyByte = Hbase11xHelper.convertUserEndRowkey(configuration);

        /* 如果用户配置了 startRowkey 和 endRowkey，需要确保：startRowkey <= endRowkey */
        if (startRowkeyByte.length != 0 && endRowkeyByte.length != 0
                && Bytes.compareTo(startRowkeyByte, endRowkeyByte) > 0) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 中 startRowkey 不得大于 endRowkey.");
        }
        RegionLocator regionLocator = Hbase11xHelper.getRegionLocator(configuration);
        List<Configuration> resultConfigurations ;
        try {
            Pair<byte[][], byte[][]> regionRanges = regionLocator.getStartEndKeys();
            if (null == regionRanges) {
                throw DataXException.asDataXException(Hbase11xReaderErrorCode.SPLIT_ERROR, "获取源头 Hbase 表的 rowkey 范围失败.");
            }
            resultConfigurations = Hbase11xHelper.doSplit(configuration, startRowkeyByte, endRowkeyByte,
                    regionRanges);

            LOG.info("HBaseReader split job into {} tasks.", resultConfigurations.size());
            return resultConfigurations;
        } catch (Exception e) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.SPLIT_ERROR, "切分源头 Hbase 表失败.", e);
        }finally {
            Hbase11xHelper.closeRegionLocator(regionLocator);
        }
    }


    private static List<Configuration> doSplit(Configuration config, byte[] startRowkeyByte,
                                               byte[] endRowkeyByte, Pair<byte[][], byte[][]> regionRanges) {

        List<Configuration> configurations = new ArrayList<Configuration>();

        for (int i = 0; i < regionRanges.getFirst().length; i++) {

            byte[] regionStartKey = regionRanges.getFirst()[i];
            byte[] regionEndKey = regionRanges.getSecond()[i];

            // 当前的region为最后一个region
            // 如果最后一个region的start Key大于用户指定的userEndKey,则最后一个region，应该不包含在内
            // 注意如果用户指定userEndKey为"",则此判断应该不成立。userEndKey为""表示取得最大的region
            if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0
                    && (endRowkeyByte.length != 0 && (Bytes.compareTo(
                    regionStartKey, endRowkeyByte) > 0))) {
                continue;
            }

            // 如果当前的region不是最后一个region，
            // 用户配置的userStartKey大于等于region的endkey,则这个region不应该含在内
            if ((Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) != 0)
                    && (Bytes.compareTo(startRowkeyByte, regionEndKey) >= 0)) {
                continue;
            }

            // 如果用户配置的userEndKey小于等于 region的startkey,则这个region不应该含在内
            // 注意如果用户指定的userEndKey为"",则次判断应该不成立。userEndKey为""表示取得最大的region
            if (endRowkeyByte.length != 0
                    && (Bytes.compareTo(endRowkeyByte, regionStartKey) <= 0)) {
                continue;
            }

            Configuration p = config.clone();

            String thisStartKey = getStartKey(startRowkeyByte, regionStartKey);

            String thisEndKey = getEndKey(endRowkeyByte, regionEndKey);

            p.set(Key.START_ROWKEY, thisStartKey);
            p.set(Key.END_ROWKEY, thisEndKey);

            LOG.debug("startRowkey:[{}], endRowkey:[{}] .", thisStartKey, thisEndKey);

            configurations.add(p);
        }

        return configurations;
    }

    private static String getEndKey(byte[] endRowkeyByte, byte[] regionEndKey) {
        if (endRowkeyByte == null) {// 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException("userEndKey should not be null!");
        }

        byte[] tempEndRowkeyByte;

        if (endRowkeyByte.length == 0) {
            tempEndRowkeyByte = regionEndKey;
        } else if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
            // 为最后一个region
            tempEndRowkeyByte = endRowkeyByte;
        } else {
            if (Bytes.compareTo(endRowkeyByte, regionEndKey) > 0) {
                tempEndRowkeyByte = regionEndKey;
            } else {
                tempEndRowkeyByte = endRowkeyByte;
            }
        }

        return Bytes.toStringBinary(tempEndRowkeyByte);
    }

    private static String getStartKey(byte[] startRowkeyByte, byte[] regionStarKey) {
        if (startRowkeyByte == null) {// 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException(
                    "userStartKey should not be null!");
        }

        byte[] tempStartRowkeyByte;

        if (Bytes.compareTo(startRowkeyByte, regionStarKey) < 0) {
            tempStartRowkeyByte = regionStarKey;
        } else {
            tempStartRowkeyByte = startRowkeyByte;
        }
        return Bytes.toStringBinary(tempStartRowkeyByte);
    }


    public static void validateParameter(com.alibaba.datax.common.util.Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HBASE_CONFIG, Hbase11xReaderErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE, Hbase11xReaderErrorCode.REQUIRED_VALUE);

        Hbase11xHelper.validateMode(originalConfig);

        //非必选参数处理
        String encoding = originalConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        if (!Charset.isSupported(encoding)) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, String.format("Hbasereader 不支持您所配置的编码:[%s]", encoding));
        }
        originalConfig.set(Key.ENCODING, encoding);
        // 处理 range 的配置
        String startRowkey = originalConfig.getString(Constant.RANGE + "." + Key.START_ROWKEY);

        //此处判断需要谨慎：如果有 key range.startRowkey 但是没有值，得到的 startRowkey 是空字符串，而不是 null
        if (startRowkey != null && startRowkey.length() != 0) {
            originalConfig.set(Key.START_ROWKEY, startRowkey);
        }

        String endRowkey = originalConfig.getString(Constant.RANGE + "." + Key.END_ROWKEY);
        //此处判断需要谨慎：如果有 key range.endRowkey 但是没有值，得到的 endRowkey 是空字符串，而不是 null
        if (endRowkey != null && endRowkey.length() != 0) {
            originalConfig.set(Key.END_ROWKEY, endRowkey);
        }
        Boolean isBinaryRowkey = originalConfig.getBool(Constant.RANGE + "." + Key.IS_BINARY_ROWKEY,false);
        originalConfig.set(Key.IS_BINARY_ROWKEY, isBinaryRowkey);

        //scan cache
        int scanCacheSize = originalConfig.getInt(Key.SCAN_CACHE_SIZE,Constant.DEFAULT_SCAN_CACHE_SIZE);
        originalConfig.set(Key.SCAN_CACHE_SIZE,scanCacheSize);

        int scanBatchSize = originalConfig.getInt(Key.SCAN_BATCH_SIZE,Constant.DEFAULT_SCAN_BATCH_SIZE);
        originalConfig.set(Key.SCAN_BATCH_SIZE,scanBatchSize);
    }

    private static String validateMode(com.alibaba.datax.common.util.Configuration  originalConfig) {
        String mode = originalConfig.getNecessaryValue(Key.MODE,Hbase11xReaderErrorCode.REQUIRED_VALUE);
        List<Map> column = originalConfig.getList(Key.COLUMN, Map.class);
        if (column == null || column.isEmpty()) {
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.REQUIRED_VALUE, "您配置的column为空,Hbase必须配置 column，其形式为：column:[{\"name\": \"cf0:column0\",\"type\": \"string\"},{\"name\": \"cf1:column1\",\"type\": \"long\"}]");
        }
        ModeType modeType = ModeType.getByTypeName(mode);
        switch (modeType) {
            case Normal: {
                // normal 模式不需要配置 maxVersion，需要配置 column，并且 column 格式为 Map 风格
                String maxVersion = originalConfig.getString(Key.MAX_VERSION);
                Validate.isTrue(maxVersion == null, "您配置的是 normal 模式读取 hbase 中的数据，所以不能配置无关项：maxVersion");
                // 通过 parse 进行 column 格式的进一步检查
                Hbase11xHelper.parseColumnOfNormalMode(column);
                break;
            }
            case MultiVersionFixedColumn:{
                // multiVersionFixedColumn 模式需要配置 maxVersion
                checkMaxVersion(originalConfig, mode);

                Hbase11xHelper.parseColumnOfMultiversionMode(column);
                break;
            }
            default:
                throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE,
                        String.format("HbaseReader不支持该 mode 类型:%s", mode));
        }
        return mode;
    }

    // 检查 maxVersion 是否存在，并且值是否合法
    private static void checkMaxVersion(Configuration configuration, String mode) {
        Integer maxVersion = configuration.getInt(Key.MAX_VERSION);
        Validate.notNull(maxVersion, String.format("您配置的是 %s 模式读取 hbase 中的数据，所以必须配置：maxVersion", mode));
        boolean isMaxVersionValid = maxVersion == -1 || maxVersion > 1;
        Validate.isTrue(isMaxVersionValid, String.format("您配置的是 %s 模式读取 hbase 中的数据，但是配置的 maxVersion 值错误. maxVersion规定：-1为读取全部版本，不能配置为0或者1（因为0或者1，我们认为用户是想用 normal 模式读取数据，而非 %s 模式读取，二者差别大），大于1则表示读取最新的对应个数的版本", mode, mode));
    }
}
