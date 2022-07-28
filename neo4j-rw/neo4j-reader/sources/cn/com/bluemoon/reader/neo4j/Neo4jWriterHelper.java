package cn.com.bluemoon.reader.neo4j;

import cn.com.bluemoon.metadata.base.util.JavaDriverFactory;
import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import java.sql.Date;
import java.time.ZoneId;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4jReader-1.0-SNAPSHOT.jar:cn/com/bluemoon/reader/neo4j/Neo4jWriterHelper.class */
public class Neo4jWriterHelper {
    private static final Logger log = LoggerFactory.getLogger(Neo4jWriterHelper.class);

    public static void read(RecordSender recordSender, TaskPluginCollector taskPluginCollector, String queryCql) {
        Result result = JavaDriverFactory.getDriver().session().run(new Query(queryCql));
        if (!result.hasNext()) {
            log.warn("没有读取到数据!");
            return;
        }
        for (Record record : result.list()) {
            recordSender.sendToWriter(buildDataXRecord(recordSender, record));
        }
    }

    private static com.alibaba.datax.common.element.Record buildDataXRecord(RecordSender recordSender, Record record) {
        com.alibaba.datax.common.element.Record dataXRecord = recordSender.createRecord();
        for (Pair<String, Value> field : record.fields()) {
            Value value = (Value) field.value();
            TypeConstructor constructor = value.type().constructor();
            if (constructor.equals(TypeConstructor.STRING)) {
                dataXRecord.addColumn(new StringColumn(value.asString()));
            } else if (constructor.equals(TypeConstructor.BOOLEAN)) {
                dataXRecord.addColumn(new BoolColumn(Boolean.valueOf(value.asBoolean())));
            } else if (constructor.equals(TypeConstructor.NUMBER)) {
                dataXRecord.addColumn(new DoubleColumn(Double.valueOf(value.asNumber().doubleValue())));
            } else if (constructor.equals(TypeConstructor.INTEGER)) {
                dataXRecord.addColumn(new LongColumn(Long.valueOf(value.asLong())));
            } else if (constructor.equals(TypeConstructor.FLOAT)) {
                dataXRecord.addColumn(new DoubleColumn(Double.valueOf(value.asDouble())));
            } else if (constructor.equals(TypeConstructor.DATE_TIME)) {
                dataXRecord.addColumn(new DateColumn(Date.from(value.asZonedDateTime().toInstant())));
            } else if (constructor.equals(TypeConstructor.DATE)) {
                dataXRecord.addColumn(new DateColumn(Date.from(value.asLocalDate().atStartOfDay(ZoneId.systemDefault()).toInstant())));
            } else if (constructor.equals(TypeConstructor.NULL)) {
                dataXRecord.addColumn(new StringColumn((String) null));
            } else {
                throw DataXException.asDataXException(Neo4jReaderErrorCode.REQUIRED_VALUE, String.format("您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s],  字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .", field.key(), constructor.toString()));
            }
        }
        return dataXRecord;
    }
}
