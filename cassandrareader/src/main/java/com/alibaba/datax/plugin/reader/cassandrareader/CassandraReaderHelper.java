package com.alibaba.datax.plugin.reader.cassandrareader;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mazhenlin on 2019/8/21.
 */
public class CassandraReaderHelper {
  static CodecRegistry registry = new CodecRegistry();
  private static final Logger LOG = LoggerFactory
      .getLogger(CassandraReader.class);

  static class TypeNotSupported extends Exception{}

  static String toJSonString(Object o, DataType type ) throws Exception{
    if( o == null ) return JSON.toJSONString(null);
    switch (type.getName()) {
    case LIST:
    case MAP:
    case SET:
    case TUPLE:
    case UDT:
      return JSON.toJSONString(transferObjectForJson(o,type));

    default:
      return JSON.toJSONString(o);
    }
  }

  static Object transferObjectForJson(Object o,DataType type) throws TypeNotSupported{
    if( o == null ) return o;
    switch (type.getName()) {
    case ASCII:
    case TEXT:
    case VARCHAR:
    case BOOLEAN:
    case SMALLINT:
    case TINYINT:
    case INT:
    case BIGINT:
    case VARINT:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
    case UUID:
    case TIMEUUID:
    case TIME:
      return o;

    case BLOB:
      ByteBuffer byteBuffer = (ByteBuffer)o;
      String s = Base64.encodeBase64String(
            Arrays.copyOfRange(byteBuffer.array(),byteBuffer.position(),
                byteBuffer.limit()));
      return s;

    case DATE:
      return ((LocalDate)o).getMillisSinceEpoch();

    case TIMESTAMP:
      return ((Date)o).getTime();

    case DURATION:
      return o.toString();

    case INET:
      return ((InetAddress)o).getHostAddress();

    case LIST: {
      return transferListForJson((List)o,type.getTypeArguments().get(0));
    }

    case MAP: {
      DataType keyType = type.getTypeArguments().get(0);
      DataType valType = type.getTypeArguments().get(1);
      return transferMapForJson((Map)o,keyType,valType);
    }

    case SET: {
      return transferSetForJson((Set)o, type.getTypeArguments().get(0));
    }

    case TUPLE: {
      return transferTupleForJson((TupleValue)o,((TupleType)type).getComponentTypes());
    }

    case UDT: {
      return transferUDTForJson((UDTValue)o);
    }

    default:
      throw new TypeNotSupported();
    }

  }

  static List transferListForJson(List clist, DataType eleType) throws TypeNotSupported {
    List result = new ArrayList();
    switch (eleType.getName()) {
    case ASCII:
    case TEXT:
    case VARCHAR:
    case BOOLEAN:
    case SMALLINT:
    case TINYINT:
    case INT:
    case BIGINT:
    case VARINT:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
    case TIME:
    case UUID:
    case TIMEUUID:
      return clist;

    case BLOB:
    case DATE:
    case TIMESTAMP:
    case DURATION:
    case INET:
    case LIST:
    case MAP:
    case SET:
    case TUPLE:
    case UDT:
      for (Object item : clist) {
        Object newItem = transferObjectForJson(item, eleType);
        result.add(newItem);
      }
      break;

    default:
      throw new TypeNotSupported();
    }

    return result;
  }

  static Set transferSetForJson(Set cset,DataType eleType) throws TypeNotSupported{
    Set result = new HashSet();
    switch (eleType.getName()) {
    case ASCII:
    case TEXT:
    case VARCHAR:
    case BOOLEAN:
    case SMALLINT:
    case TINYINT:
    case INT:
    case BIGINT:
    case VARINT:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
    case TIME:
    case UUID:
    case TIMEUUID:
      return cset;

    case BLOB:
    case DATE:
    case TIMESTAMP:
    case DURATION:
    case INET:
    case LIST:
    case MAP:
    case SET:
    case TUPLE:
    case UDT:
      for (Object item : cset) {
        Object newItem = transferObjectForJson(item,eleType);
        result.add(newItem);
      }
      break;

    default:
      throw new TypeNotSupported();
    }

    return result;
  }

  static Map transferMapForJson(Map cmap,DataType keyType,DataType valueType) throws TypeNotSupported {
    Map newMap = new HashMap();
    for( Object e : cmap.entrySet() ) {
      Object k = ((Map.Entry)e).getKey();
      Object v = ((Map.Entry)e).getValue();
      Object newKey = transferObjectForJson(k,keyType);
      Object newValue = transferObjectForJson(v,valueType);
      if( !(newKey instanceof String) ) {
        newKey = JSON.toJSONString(newKey);
      }
      newMap.put(newKey,newValue);
    }
    return newMap;
  }

  static List transferTupleForJson(TupleValue tupleValue,List<DataType> componentTypes) throws TypeNotSupported {
    List l = new ArrayList();
    for (int j = 0; j < componentTypes.size(); j++ ) {
      DataType dataType = componentTypes.get(j);
      TypeToken<?> eltClass = registry.codecFor(dataType).getJavaType();
      Object ele = tupleValue.get(j,eltClass);
      l.add(transferObjectForJson(ele,dataType));
    }
    return l;
  }

  static Map transferUDTForJson(UDTValue udtValue) throws TypeNotSupported {
    Map<String,Object> newMap = new HashMap();
    int j = 0;
    for (UserType.Field f : udtValue.getType()) {
      DataType dataType = f.getType();
      TypeToken<?> eltClass = registry.codecFor(dataType).getJavaType();
      Object ele = udtValue.get(j, eltClass);
      newMap.put(f.getName(),transferObjectForJson(ele,dataType));
      j++;
    }
    return newMap;
  }

  static Record buildRecord(Record record, Row rs, ColumnDefinitions metaData, int columnNumber,
      TaskPluginCollector taskPluginCollector) {

    try {
      for (int i = 0; i < columnNumber; i++)
        try {
          if (rs.isNull(i)) {
            record.addColumn(new StringColumn());
          continue;
        }
        switch (metaData.getType(i).getName()) {

        case ASCII:
        case TEXT:
        case VARCHAR:
          record.addColumn(new StringColumn(rs.getString(i)));
          break;

        case BLOB:
          record.addColumn(new BytesColumn(rs.getBytes(i).array()));
          break;

        case BOOLEAN:
          record.addColumn(new BoolColumn(rs.getBool(i)));
          break;

        case SMALLINT:
          record.addColumn(new LongColumn((int)rs.getShort(i)));
          break;

        case TINYINT:
          record.addColumn(new LongColumn((int)rs.getByte(i)));
          break;

        case INT:
          record.addColumn(new LongColumn(rs.getInt(i)));
          break;

        case COUNTER:
        case BIGINT:
          record.addColumn(new LongColumn(rs.getLong(i)));
          break;

        case VARINT:
          record.addColumn(new LongColumn(rs.getVarint(i)));
          break;

        case FLOAT:
          record.addColumn(new DoubleColumn(rs.getFloat(i)));
          break;

        case DOUBLE:
          record.addColumn(new DoubleColumn(rs.getDouble(i)));
          break;

        case DECIMAL:
          record.addColumn(new DoubleColumn(rs.getDecimal(i)));
          break;

        case DATE:
          record.addColumn(new DateColumn(rs.getDate(i).getMillisSinceEpoch()));
          break;

        case TIME:
          record.addColumn(new LongColumn(rs.getTime(i)));
          break;

        case TIMESTAMP:
          record.addColumn(new DateColumn(rs.getTimestamp(i)));
          break;

        case UUID:
        case TIMEUUID:
          record.addColumn(new StringColumn(rs.getUUID(i).toString()));
          break;

        case INET:
          record.addColumn(new StringColumn(rs.getInet(i).getHostAddress()));
          break;

        case DURATION:
          record.addColumn(new StringColumn(rs.get(i,Duration.class).toString()));
          break;

        case LIST: {
          TypeToken listEltClass = registry.codecFor(metaData.getType(i).getTypeArguments().get(0)).getJavaType();
          List<?> l = rs.getList(i, listEltClass);
          record.addColumn(new StringColumn(toJSonString(l,metaData.getType(i))));
        }
        break;

        case MAP: {
          DataType keyType = metaData.getType(i).getTypeArguments().get(0);
          DataType valType = metaData.getType(i).getTypeArguments().get(1);
          TypeToken<?> keyEltClass = registry.codecFor(keyType).getJavaType();
          TypeToken<?> valEltClass = registry.codecFor(valType).getJavaType();
          Map<?,?> m = rs.getMap(i, keyEltClass, valEltClass);
          record.addColumn(new StringColumn(toJSonString(m,metaData.getType(i))));
        }
        break;

        case SET: {
          TypeToken<?> setEltClass = registry.codecFor(metaData.getType(i).getTypeArguments().get(0))
              .getJavaType();
          Set<?> set = rs.getSet(i, setEltClass);
          record.addColumn(new StringColumn(toJSonString(set,metaData.getType(i))));
        }
        break;

        case TUPLE: {
          TupleValue t = rs.getTupleValue(i);
          record.addColumn(new StringColumn(toJSonString(t,metaData.getType(i))));
        }
        break;

        case UDT: {
          UDTValue t = rs.getUDTValue(i);
          record.addColumn(new StringColumn(toJSonString(t,metaData.getType(i))));
        }
        break;

        default:
          throw DataXException
              .asDataXException(
                  CassandraReaderErrorCode.CONF_ERROR,
                  String.format(
                      "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], "
                          + "字段类型:[%s]. ",
                      metaData.getName(i),
                      metaData.getType(i)));
        }
      } catch (TypeNotSupported t) {
        throw DataXException
            .asDataXException(
                CassandraReaderErrorCode.CONF_ERROR,
                String.format(
                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], "
                        + "字段类型:[%s]. ",
                    metaData.getName(i),
                    metaData.getType(i)));

      }
    } catch (Exception e) {
      //TODO 这里识别为脏数据靠谱吗？
      taskPluginCollector.collectDirtyRecord(record, e);
      if (e instanceof DataXException) {
        throw (DataXException) e;
      }
      return null;
    }
    return record;
  }

  public static List<Configuration> splitJob(int adviceNumber,Configuration jobConfig,Cluster cluster) {
    List<Configuration> splittedConfigs = new ArrayList<Configuration>();
    if( adviceNumber <= 1 ) {
      splittedConfigs.add(jobConfig);
      return splittedConfigs;
    }
    String where = jobConfig.getString(Key.WHERE);
    if(where != null && where.toLowerCase().contains("token(")) {
      splittedConfigs.add(jobConfig);
      return splittedConfigs;
    }
    String partitioner = cluster.getMetadata().getPartitioner();
    if( partitioner.endsWith("RandomPartitioner")) {
      BigDecimal minToken = BigDecimal.valueOf(-1);
      BigDecimal maxToken = new BigDecimal(new BigInteger("2").pow(127));
      BigDecimal step = maxToken.subtract(minToken)
          .divide(BigDecimal.valueOf(adviceNumber),2, BigDecimal.ROUND_HALF_EVEN);
      for ( int i = 0; i < adviceNumber; i++ ) {
        BigInteger l = minToken.add(step.multiply(BigDecimal.valueOf(i))).toBigInteger();
        BigInteger r = minToken.add(step.multiply(BigDecimal.valueOf(i+1))).toBigInteger();
        if( i == adviceNumber - 1 ) {
          r = maxToken.toBigInteger();
        }
        Configuration taskConfig = jobConfig.clone();
        taskConfig.set(Key.MIN_TOKEN,l.toString());
        taskConfig.set(Key.MAX_TOKEN,r.toString());
        splittedConfigs.add(taskConfig);
      }
    }
    else if( partitioner.endsWith("Murmur3Partitioner") ) {
      BigDecimal minToken = BigDecimal.valueOf(Long.MIN_VALUE);
      BigDecimal maxToken = BigDecimal.valueOf(Long.MAX_VALUE);
      BigDecimal step = maxToken.subtract(minToken)
          .divide(BigDecimal.valueOf(adviceNumber),2, BigDecimal.ROUND_HALF_EVEN);
      for ( int i = 0; i < adviceNumber; i++ ) {
        long l = minToken.add(step.multiply(BigDecimal.valueOf(i))).longValue();
        long r = minToken.add(step.multiply(BigDecimal.valueOf(i+1))).longValue();
        if( i == adviceNumber - 1 ) {
          r = maxToken.longValue();
        }
        Configuration taskConfig = jobConfig.clone();
        taskConfig.set(Key.MIN_TOKEN,String.valueOf(l));
        taskConfig.set(Key.MAX_TOKEN,String.valueOf(r));
        splittedConfigs.add(taskConfig);
      }
    }
    else {
      splittedConfigs.add(jobConfig);
    }
    return splittedConfigs;
  }

  public static String getQueryString(Configuration taskConfig,Cluster cluster) {
    List<String> columnMeta = taskConfig.getList(Key.COLUMN,String.class);
    String keyspace = taskConfig.getString(Key.KEYSPACE);
    String table = taskConfig.getString(Key.TABLE);

    StringBuilder columns = new StringBuilder();
    for( String column : columnMeta ) {
      if(columns.length() > 0 ) {
        columns.append(",");
      }
      columns.append(column);
    }

    StringBuilder where = new StringBuilder();
    String whereString = taskConfig.getString(Key.WHERE);
    if( whereString != null && !whereString.isEmpty() ) {
      where.append(whereString);
    }
    String minToken = taskConfig.getString(Key.MIN_TOKEN);
    String maxToken = taskConfig.getString(Key.MAX_TOKEN);
    if( minToken !=null || maxToken !=null ) {
      LOG.info("range:" + minToken + "~" + maxToken);
      List<ColumnMetadata> pks = cluster.getMetadata().getKeyspace(keyspace).getTable(table).getPartitionKey();
      StringBuilder sb = new StringBuilder();
      for( ColumnMetadata pk : pks ) {
        if( sb.length() > 0 ) {
          sb.append(",");
        }
        sb.append(pk.getName());
      }
      String s = sb.toString();
      if (minToken != null && !minToken.isEmpty()) {
        if( where.length() > 0 ){
          where.append(" AND ");
        }
        where.append("token(").append(s).append(")").append(" > ").append(minToken);
      }
      if (maxToken != null && !maxToken.isEmpty()) {
        if( where.length() > 0 ){
          where.append(" AND ");
        }
        where.append("token(").append(s).append(")").append(" <= ").append(maxToken);
      }
    }

    boolean allowFiltering = taskConfig.getBool(Key.ALLOW_FILTERING,false);

    StringBuilder select = new StringBuilder();
    select.append("SELECT ").append(columns.toString()).append(" FROM ").append(table);
    if( where.length() > 0 ){
      select.append(" where ").append(where.toString());
    }
    if( allowFiltering ) {
      select.append(" ALLOW FILTERING");
    }
    select.append(";");
    return select.toString();
  }

  public static void checkConfig(Configuration jobConfig,Cluster cluster) {
    ensureStringExists(jobConfig,Key.HOST);
    ensureStringExists(jobConfig,Key.KEYSPACE);
    ensureStringExists(jobConfig,Key.TABLE);
    ensureExists(jobConfig,Key.COLUMN);

    ///keyspace,table是否存在
    String keyspace = jobConfig.getString(Key.KEYSPACE);
    if( cluster.getMetadata().getKeyspace(keyspace) == null ) {
      throw DataXException
          .asDataXException(
              CassandraReaderErrorCode.CONF_ERROR,
              String.format(
                  "配置信息有错误.keyspace'%s'不存在 .",
                  keyspace));
    }
    String table = jobConfig.getString(Key.TABLE);
    TableMetadata tableMetadata = cluster.getMetadata().getKeyspace(keyspace).getTable(table);
    if( tableMetadata == null ) {
      throw DataXException
          .asDataXException(
              CassandraReaderErrorCode.CONF_ERROR,
              String.format(
                  "配置信息有错误.表'%s'不存在 .",
                  table));
    }
    List<String> columns = jobConfig.getList(Key.COLUMN,String.class);
    for( String name : columns ) {
      if( name == null || name.isEmpty() ) {
        throw DataXException
            .asDataXException(
                CassandraReaderErrorCode.CONF_ERROR,
                String.format(
                    "配置信息有错误.列信息中需要包含'%s'字段 .",Key.COLUMN_NAME));
      }
    }
  }

  static void ensureExists(Configuration jobConfig,String keyword) {
    if( jobConfig.get(keyword) == null ) {
      throw DataXException
          .asDataXException(
              CassandraReaderErrorCode.CONF_ERROR,
              String.format(
                  "配置信息有错误.参数'%s'为必填项 .",
                  keyword));
    }
  }

  static void ensureStringExists(Configuration jobConfig,String keyword) {
    ensureExists(jobConfig,keyword);
    if( jobConfig.getString(keyword).isEmpty() ) {
      throw DataXException
          .asDataXException(
              CassandraReaderErrorCode.CONF_ERROR,
              String.format(
                  "配置信息有错误.参数'%s'不能为空 .",
                  keyword));
    }
  }

}
