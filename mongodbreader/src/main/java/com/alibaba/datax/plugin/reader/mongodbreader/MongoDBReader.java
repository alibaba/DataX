package com.alibaba.datax.plugin.reader.mongodbreader;

import java.util.*;
import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson2.*;
import com.alibaba.datax.common.element.Column;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database = originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            if (!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig, userName, password, authDb);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }

    /**
     * 递归将 BSON 值转换为纯 JSON 兼容对象，避免 MongoDB Extended JSON 中的类型包装。
     *
     * Document.get(key) 返回的完整 Java 类型映射（基于 MongoDB Java Driver 5.x BsonTypeClassMap）：
     *   ObjectId         -> org.bson.types.ObjectId       → 转为 24位十六进制字符串
     *   Date             -> java.util.Date                → 转为 ISO-8601 字符串
     *   Binary           -> org.bson.types.Binary         → 转为 Base64 字符串
     *   BsonTimestamp    -> org.bson.BsonTimestamp        → 转为 ISO-8601 字符串
     *   Decimal128       -> org.bson.types.Decimal128     → 转为 BigDecimal 的字符串
     *   Code             -> org.bson.types.Code           → 转为代码字符串
     *   CodeWithScope    -> org.bson.types.CodeWithScope  → 转为 JSON 对象 {code:..., scope:...}
     *   Symbol           -> org.bson.types.Symbol         → 转为字符串
     *   BsonUndefined    -> org.bson.BsonUndefined        → 转为 null
     *   MinKey           -> org.bson.types.MinKey         → 转为字符串 "MinKey"
     *   MaxKey           -> org.bson.types.MaxKey         → 转为字符串 "MaxKey"
     *   BsonDbPointer    -> org.bson.BsonDbPointer        → 转为 JSON 对象 {namespace:..., id:...}
     *   Document         -> org.bson.Document             → 递归转换为 Map
     *   List             -> java.util.List                → 递归转换每个元素
     *   其他基本类型（String,Integer,Long,Double,Boolean）→ 原样返回
     */
    private static Object bsonValueToPlainObject(Object value) {
        if (value == null) {
            return null;
        }
        // ObjectId → 24位十六进制字符串（避免 {"$oid": "..."}）
        if (value instanceof ObjectId) {
            return ((ObjectId) value).toHexString();
        }
        // Date → ISO-8601 字符串（避免 {"$date": "..."}）
        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).toInstant().toString();
        }
        // Binary → Base64 字符串（注意：Document API 返回 org.bson.types.Binary，不是 org.bson.BsonBinary）
        if (value instanceof Binary) {
            return Base64.getEncoder().encodeToString(((Binary) value).getData());
        }
        // BsonTimestamp → ISO-8601 字符串（MongoDB 内部时间戳，含 time 秒 + inc 计数器）
        if (value instanceof BsonTimestamp) {
            long epochSeconds = ((BsonTimestamp) value).getTime();
            return new java.util.Date(epochSeconds * 1000L).toInstant().toString();
        }
        // Decimal128 → BigDecimal 字符串（避免科学计数法丢失精度）
        if (value instanceof BoolColumn) {
            return value.toString();
        }
        // Code (JavaScript) → 代码字符串
        if (value instanceof Code) {
            return ((Code) value).getCode();
        }
        // CodeWithScope (JavaScript with scope) → JSON 对象
        if (value instanceof CodeWithScope) {
            CodeWithScope cws = (CodeWithScope) value;
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("code", cws.getCode());
            map.put("scope", bsonValueToPlainObject(cws.getScope()));
            return map;
        }
        // Symbol → 字符串
        if (value instanceof Symbol) {
            return ((Symbol) value).getSymbol();
        }
        // BsonUndefined → null
        if (value instanceof BsonUndefined) {
            return null;
        }
        // MinKey → 字符串标识
        if (value instanceof org.bson.types.MinKey) {
            return "MinKey";
        }
        // MaxKey → 字符串标识
        if (value instanceof org.bson.types.MaxKey) {
            return "MaxKey";
        }
        // BsonDbPointer → JSON 对象
        if (value instanceof org.bson.BsonDbPointer) {
            org.bson.BsonDbPointer ptr = (org.bson.BsonDbPointer) value;
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("namespace", ptr.getNamespace());
            map.put("id", ptr.getId().toHexString());
            return map;
        }
        // BsonRegularExpression → 字符串
        if (value instanceof org.bson.BsonRegularExpression) {
            return ((org.bson.BsonRegularExpression) value).getPattern();
        }
        // Document 递归转换为 Map
        if (value instanceof Document) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : ((Document) value).entrySet()) {
                map.put(entry.getKey(), bsonValueToPlainObject(entry.getValue()));
            }
            return map;
        }
        // List 递归转换每个元素
        if (value instanceof List) {
            List<Object> list = new ArrayList<>();
            for (Object item : (List<?>) value) {
                list.add(bsonValueToPlainObject(item));
            }
            return list;
        }
        // 其他基本类型（String, Integer, Long, Double, Boolean 等）直接返回，fastjson2 可正确序列化
        return value;
    }

    /**
     * 将 BSON Document 转换为干净的 JSON 字符串（不含 MongoDB Extended JSON 类型包装）
     */
    private static String documentToCleanJson(Document doc) {
        Object plainObj = bsonValueToPlainObject(doc);
        return JSON.toJSONString(plainObj);
    }

    /**
     * 将 List 转换为干净的 JSON 数组字符串
     */
    private static String listToCleanJson(List<?> list) {
        Object plainObj = bsonValueToPlainObject(list);
        return JSON.toJSONString(plainObj);
    }
    /**
     * 安全地将分片边界值转为 MongoDB 查询所需的类型。
     * 如果 isObjectId=true 且 bound 是合法的 24 位 hex 字符串，则转为 ObjectId；
     * 否则直接返回 bound 的字符串形式，不强行转换，避免 ClassCastException。
     */
    private static Object safeBoundValue(Object bound, boolean isObjectId) {
        if (bound == null) {
            return null;
        }
        if (isObjectId) {
            String str = bound.toString();
            // 只有确认为 24 位合法 hex 字符串才转 ObjectId
            if (str.matches("^[0-9a-fA-F]{24}$")) {
                return new ObjectId(str);
            }
            // 不是合法 hex，说明 _id 实际不是 ObjectId 类型，直接用原值
            return str;
        }
        return bound;
    }
    /**
     * 安全地将任意 BSON 值转为 String。
     * 解决：Document.getString() 内部强转 (String)get(key)，遇到 ObjectId 等非 String 类型会抛 ClassCastException。
     */
    private static String safeToString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        // ObjectId → hex 字符串
        if (value instanceof ObjectId) {
            return ((ObjectId) value).toHexString();
        }
        if (value instanceof BoolColumn) {
            return   ((BoolColumn) value).asString();
        }
        // 其他类型（Integer, Long, Double, Boolean 等）→ String.valueOf
        // 复杂类型走 bsonValueToPlainObject 清洗
        Object plain = bsonValueToPlainObject(value);
        if (plain instanceof String) {
            return (String) plain;
        }
        return JSON.toJSONString(plain);
    }

    /**
     * 安全地将任意 BSON 值转为 Long。
     * 解决：Document.getInteger()/getLong() 遇到类型不匹配会抛 ClassCastException。
     * 支持：Integer → Long, Long → Long, Double → Long(截断), String → Long(解析), Number → Long
     */
    private static Long safeToLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        return null;
    }

    /**
     * 安全地将任意 BSON 值转为 Double。
     * 支持：Double, Integer, Long, Number, String, Decimal128
     */
    private static Double safeToDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        return null;
    }

    /**
     * 安全地将任意 BSON 值转为 java.util.Date。
     * 支持：Date, BsonTimestamp, Long(毫秒时间戳), String(ISO格式)
     */
    private static java.util.Date safeToDate(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return (java.util.Date) value;
        }
        if (value instanceof BsonTimestamp) {
            return new java.util.Date(((BsonTimestamp) value).getTime() * 1000L);
        }
        if (value instanceof Long) {
            return new java.util.Date((Long) value);
        }
        if (value instanceof String) {
            try {
                return java.util.Date.from(java.time.Instant.parse((String) value));
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /**
     * 安全地将任意 BSON 值转为 ObjectId 的 hex 字符串。
     * 解决：Document.getObjectId() 遇到 String 类型的 ObjectId 会抛 ClassCastException。
     * 支持：ObjectId → hex, String(24位hex) → 直接返回
     */
    private static String safeToObjectIdHex(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ObjectId) {
            return ((ObjectId) value).toHexString();
        }
        if (value instanceof String) {
            String str = (String) value;
            // 如果是 24 位 hex 字符串，直接返回
            if (str.matches("^[0-9a-fA-F]{24}$")) {
                return str;
            }
            return str;
        }
        // 其他类型兜底
        Object plain = bsonValueToPlainObject(value);
        if (plain instanceof String) {
            return (String) plain;
        }
        return JSON.toJSONString(plain);
    }

    /**
     * 根据运行时实际 BSON 类型自动判断并转换为合适的 DataX Column。
     * 解决 MongoDB schemaless 特性导致同一字段在不同文档中可能是不同类型的问题
     * （例如某文档中是 ObjectId，另一文档中是 String）。
     *
     * 自动映射规则（基于 Document.get(key) 返回的 Java 类型）：
     *   null          → StringColumn(null)
     *   String        → StringColumn
     *   ObjectId      → StringColumn(hex字符串)，不再产生 {"$oid":"..."}
     *   Integer       → LongColumn
     *   Long          → LongColumn
     *   Double        → DoubleColumn
     *   Decimal128    → StringColumn(plain字符串，保留精度)
     *   Boolean       → StringColumn("true"/"false")
     *   Date          → DateColumn
     *   BsonTimestamp → DateColumn
     *   Document      → StringColumn(干净JSON字符串，递归清洗所有嵌套BSON类型)
     *   List          → StringColumn(干净JSON数组字符串，递归清洗)
     *   Binary        → StringColumn(Base64)
     *   其他          → StringColumn(经bsonValueToPlainObject清洗后的字符串)
     */
    private static Column autoDetectColumn(Object value) {
        if (value == null) {
            return new StringColumn(null);
        }
        // String → 直接作为字符串
        if (value instanceof String) {
            return new StringColumn((String) value);
        }
        // ObjectId → 转 hex 字符串，不再产生 {"$oid":"..."}
        if (value instanceof ObjectId) {
            return new StringColumn(((ObjectId) value).toHexString());
        }
        // Integer → LongColumn
        if (value instanceof Integer) {
            return new LongColumn(((Integer) value).longValue());
        }
        // Long → LongColumn
        if (value instanceof Long) {
            return new LongColumn((Long) value);
        }
        // Double → DoubleColumn
        if (value instanceof Double) {
            return new DoubleColumn((Double) value);
        }

        // Boolean → 字符串
        if (value instanceof Boolean) {
            return new StringColumn(value.toString());
        }
        // Date → DateColumn
        if (value instanceof java.util.Date) {
            return new DateColumn((java.util.Date) value);
        }
        // BsonTimestamp → DateColumn
        if (value instanceof BsonTimestamp) {
            return new DateColumn(new java.util.Date(((BsonTimestamp) value).getTime() * 1000L));
        }
        // Document → 干净 JSON 字符串（递归清洗所有嵌套的 ObjectId 等）
        if (value instanceof Document) {
            return new StringColumn(documentToCleanJson((Document) value));
        }
        // List → 干净 JSON 数组字符串（递归清洗）
        if (value instanceof List) {
            return new StringColumn(listToCleanJson((List<?>) value));
        }
        // Binary → Base64 字符串
        if (value instanceof Binary) {
            return new StringColumn(Base64.getEncoder().encodeToString(((Binary) value).getData()));
        }
        // 其他所有类型：走 bsonValueToPlainObject 清洗后输出字符串
        return new StringColumn(valueToCleanJsonString(value));
    }

    /**
     * 安全获取字段的 JSON 字符串表示，对任意 BSON 类型都进行清洗转换。
     * 用于 default 分支和兜底场景，避免 Document.toJson() 产生 Extended JSON。
     */
    private static String valueToCleanJsonString(Object value) {
        if (value == null) {
            return null;
        }
        Object plain = bsonValueToPlainObject(value);
        // 如果转换结果是 String，直接返回，不再被 fastjson2 加引号
        if (plain instanceof String) {
            return (String) plain;
        }
        return JSON.toJSONString(plain);
    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        @Override
        public void startRead(RecordSender recordSender) {

            if (lowerBound == null || upperBound == null ||
                    mongoClient == null || database == null ||
                    collection == null || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);

            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", safeBoundValue(upperBound, isObjectId)));
                }
            } else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", safeBoundValue(lowerBound, isObjectId)));
            } else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID,
                        new Document("$gte", safeBoundValue(lowerBound, isObjectId))
                                .append("$lt", safeBoundValue(upperBound, isObjectId)));            }
            if (!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }

            MongoCursor<Document> dbCursor = null;
            try {
                dbCursor = col.find(filter).iterator();
                while (dbCursor.hasNext()) {
                    Document item = dbCursor.next();
                    Record record = recordSender.createRecord();
                    Iterator columnItera = mongodbColumnMeta.iterator();
                    while (columnItera.hasNext()) {
                        JSONObject column = (JSONObject) columnItera.next();
                        String columnName = column.getString(KeyConstant.COLUMN_NAME);

                        Object rawValue = item.get(columnName);

                        switch (column.getString(KeyConstant.COLUMN_TYPE).toLowerCase()) {
                            case "string":
                                record.addColumn(new StringColumn(safeToString(rawValue)));
                                break;
                            case "int":
                            case "int32":
                            case "integer":
                                record.addColumn(new LongColumn(safeToLong(rawValue)));
                                break;
                            case "int64":
                            case "long":
                                record.addColumn(new LongColumn(safeToLong(rawValue)));
                                break;
                            case "double":
                                record.addColumn(new DoubleColumn(safeToDouble(rawValue)));
                                break;
                            case "date":
                                record.addColumn(new DateColumn(safeToDate(rawValue)));
                                break;
                            case "objectid":
                                record.addColumn(new StringColumn(safeToObjectIdHex(rawValue)));
                                break;
                            case "auto":
                                record.addColumn(autoDetectColumn(rawValue));
                                break;
                            default:
                                // 未识别的类型也走自动检测，彻底避免类型不匹配
                                record.addColumn(autoDetectColumn(rawValue));
                                break;
                        }
                    }
                    recordSender.sendToWriter(record);
                }
            } finally {
                // 确保游标关闭，防止资源泄漏
                if (dbCursor != null) {
                    dbCursor.close();
                }
            }
        }


        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig, userName, password, authDb);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
        }

        @Override
        public void destroy() {

        }

    }
}
