package com.alibaba.datax.plugin.writer.cassandrawriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UserType.Field;
import com.google.common.base.Splitter;
import org.apache.commons.codec.binary.Base64;

/**
 * Created by mazhenlin on 2019/8/21.
 */
public class CassandraWriterHelper {
  static CodecRegistry registry = new CodecRegistry();

  public static Object parseFromString(String s, DataType sqlType ) throws Exception {
    if (s == null || s.isEmpty()) {
      if (sqlType.getName() == Name.ASCII || sqlType.getName() == Name.TEXT ||
          sqlType.getName() == Name.VARCHAR) {
        return s;
      } else {
        return null;
      }
    }
    switch (sqlType.getName()) {
    case ASCII:
    case TEXT:
    case VARCHAR:
      return s;

    case BLOB:
      if (s.length() == 0) {
        return new byte[0];
      }
      byte[] byteArray = new byte[s.length() / 2];
      for (int i = 0; i < byteArray.length; i++) {
        String subStr = s.substring(2 * i, 2 * i + 2);
        byteArray[i] = ((byte) Integer.parseInt(subStr, 16));
      }
      return ByteBuffer.wrap(byteArray);

    case BOOLEAN:
      return Boolean.valueOf(s);

    case TINYINT:
      return Byte.valueOf(s);

    case SMALLINT:
      return Short.valueOf(s);

    case INT:
      return Integer.valueOf(s);

    case BIGINT:
      return Long.valueOf(s);

    case VARINT:
      return new BigInteger(s, 10);

    case FLOAT:
      return Float.valueOf(s);

    case DOUBLE:
      return Double.valueOf(s);

    case DECIMAL:
      return new BigDecimal(s);

    case DATE: {
      String[] a = s.split("-");
      if (a.length != 3) {
        throw new Exception(String.format("DATE类型数据 '%s' 格式不正确，必须为yyyy-mm-dd格式", s));
      }
      return LocalDate.fromYearMonthDay(Integer.valueOf(a[0]), Integer.valueOf(a[1]),
          Integer.valueOf(a[2]));
    }

    case TIME:
      return Long.valueOf(s);

    case TIMESTAMP:
      return new Date(Long.valueOf(s));

    case UUID:
    case TIMEUUID:
      return UUID.fromString(s);

    case INET:
      String[] b = s.split("/");
      if (b.length < 2) {
        return InetAddress.getByName(s);
      }
      byte[] addr = InetAddress.getByName(b[1]).getAddress();
      return InetAddress.getByAddress(b[0], addr);

    case DURATION:
      return Duration.from(s);

    case LIST:
    case MAP:
    case SET:
    case TUPLE:
    case UDT:
      Object jsonObject = JSON.parse(s);
      return parseFromJson(jsonObject,sqlType);

    default:
      throw DataXException.asDataXException(CassandraWriterErrorCode.CONF_ERROR,
          "不支持您配置的列类型:" + sqlType + ", 请检查您的配置 或者 联系 管理员.");

    } // end switch

  }

  public static Object parseFromJson(Object jsonObject,DataType type) throws Exception {
    if( jsonObject == null ) return null;
    switch (type.getName()) {
    case ASCII:
    case TEXT:
    case VARCHAR:
    case BOOLEAN:
    case TIME:
      return jsonObject;

    case TINYINT:
      return ((Number)jsonObject).byteValue();

    case SMALLINT:
      return ((Number)jsonObject).shortValue();

    case INT:
      return ((Number)jsonObject).intValue();

    case BIGINT:
      return ((Number)jsonObject).longValue();

    case VARINT:
      return new BigInteger(jsonObject.toString());

    case FLOAT:
      return ((Number)jsonObject).floatValue();

    case DOUBLE:
      return ((Number)jsonObject).doubleValue();

    case DECIMAL:
      return new BigDecimal(jsonObject.toString());

    case BLOB:
      return ByteBuffer.wrap(Base64.decodeBase64((String)jsonObject));

    case DATE:
      return LocalDate.fromMillisSinceEpoch(((Number)jsonObject).longValue());

    case TIMESTAMP:
      return new Date(((Number)jsonObject).longValue());

    case DURATION:
      return Duration.from(jsonObject.toString());

    case UUID:
    case TIMEUUID:
      return UUID.fromString(jsonObject.toString());

    case INET:
      return InetAddress.getByName((String)jsonObject);

    case LIST:
      List l = new ArrayList();
      for( Object o : (JSONArray)jsonObject ) {
        l.add(parseFromJson(o,type.getTypeArguments().get(0)));
      }
      return l;

    case MAP: {
      Map m = new HashMap();
      for (Map.Entry e : ((JSONObject)jsonObject).entrySet()) {
        Object k = parseFromString((String) e.getKey(), type.getTypeArguments().get(0));
        Object v = parseFromJson(e.getValue(), type.getTypeArguments().get(1));
        m.put(k,v);
      }
      return m;
    }

    case SET:
      Set s = new HashSet();
      for( Object o : (JSONArray)jsonObject ) {
        s.add(parseFromJson(o,type.getTypeArguments().get(0)));
      }
      return s;

    case TUPLE: {
      TupleValue t = ((TupleType) type).newValue();
      int j = 0;
      for (Object e : (JSONArray)jsonObject) {
        DataType eleType = ((TupleType) type).getComponentTypes().get(j);
        t.set(j, parseFromJson(e, eleType), registry.codecFor(eleType).getJavaType());
        j++;
      }
      return t;
    }

    case UDT: {
      UDTValue t = ((UserType) type).newValue();
      UserType userType = t.getType();
      for (Map.Entry e : ((JSONObject)jsonObject).entrySet()) {
        DataType eleType = userType.getFieldType((String)e.getKey());
        t.set((String)e.getKey(), parseFromJson(e.getValue(), eleType), registry.codecFor(eleType).getJavaType());
      }
      return t;
    }

    }
    return null;
  }

  public static void setupColumn(BoundStatement ps, int pos, DataType sqlType, Column col) throws Exception {
    if (col.getRawData() != null) {
      switch (sqlType.getName()) {
      case ASCII:
      case TEXT:
      case VARCHAR:
        ps.setString(pos, col.asString());
        break;

      case BLOB:
        ps.setBytes(pos, ByteBuffer.wrap(col.asBytes()));
        break;

      case BOOLEAN:
        ps.setBool(pos, col.asBoolean());
        break;

      case TINYINT:
        ps.setByte(pos, col.asLong().byteValue());
        break;

      case SMALLINT:
        ps.setShort(pos, col.asLong().shortValue());
        break;

      case INT:
        ps.setInt(pos, col.asLong().intValue());
        break;

      case BIGINT:
        ps.setLong(pos, col.asLong());
        break;

      case VARINT:
        ps.setVarint(pos, col.asBigInteger());
        break;

      case FLOAT:
        ps.setFloat(pos, col.asDouble().floatValue());
        break;

      case DOUBLE:
        ps.setDouble(pos, col.asDouble());
        break;

      case DECIMAL:
        ps.setDecimal(pos, col.asBigDecimal());
        break;

      case DATE:
        ps.setDate(pos, LocalDate.fromMillisSinceEpoch(col.asDate().getTime()));
        break;

      case TIME:
        ps.setTime(pos, col.asLong());
        break;

      case TIMESTAMP:
        ps.setTimestamp(pos, col.asDate());
        break;

      case UUID:
      case TIMEUUID:
        ps.setUUID(pos, UUID.fromString(col.asString()));
        break;

      case INET:
        ps.setInet(pos, InetAddress.getByName(col.asString()));
        break;

      case DURATION:
        ps.set(pos, Duration.from(col.asString()), Duration.class);
        break;

      case LIST:
        ps.setList(pos, (List<?>) parseFromString(col.asString(), sqlType));
        break;

      case MAP:
        ps.setMap(pos, (Map) parseFromString(col.asString(), sqlType));
        break;

      case SET:
        ps.setSet(pos, (Set) parseFromString(col.asString(), sqlType));
        break;

      case TUPLE:
        ps.setTupleValue(pos, (TupleValue) parseFromString(col.asString(), sqlType));
        break;

      case UDT:
        ps.setUDTValue(pos, (UDTValue) parseFromString(col.asString(), sqlType));
        break;

      default:
        throw DataXException.asDataXException(CassandraWriterErrorCode.CONF_ERROR,
            "不支持您配置的列类型:" + sqlType + ", 请检查您的配置 或者 联系 管理员.");

      } // end switch
    } else {
      ps.setToNull(pos);
    }
  }

}
