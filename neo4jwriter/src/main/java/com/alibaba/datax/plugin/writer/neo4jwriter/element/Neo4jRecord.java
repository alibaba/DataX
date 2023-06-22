package com.alibaba.datax.plugin.writer.neo4jwriter.element;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.AsValue;
import org.neo4j.driver.internal.value.MapValue;

import java.util.List;
import java.util.Map;

/**
 * 一般来说，我们会将一批对象转换成hashmap再传输给neo4j的驱动用作参数解析，驱动会将hashmap转换成org.neo4j.driver.Value
 * 过程是：List[domain] -> List[map]->List[Value]
 * 直接将Record实现AsValue接口，有1个好处：
 * 减少了一次对象转换次数,List[domain] -> List[Value]
 */
public class Neo4jRecord implements AsValue {

    private MapValue mapValue;

    public Neo4jRecord(Record record, List<String> columnNames) {

    }

    @Override
    public Value asValue() {
        return null;
    }
}
