/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.mapping;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.reader.gdbreader.model.GdbElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

public class DefaultGdbMapper {

    public static BiConsumer<GdbElement, Record> getMapper(MappingRule rule) {
        return (gdbElement, record) -> rule.getColumns().forEach(columnMappingRule -> {
            Object value = null;
            ValueType type = columnMappingRule.getValueType();
            String name = columnMappingRule.getName();
            Map<String, Object> props = gdbElement.getProperties();

            switch (columnMappingRule.getColumnType()) {
                case dstPrimaryKey:
                    value = gdbElement.getTo();
                    break;
                case srcPrimaryKey:
                    value = gdbElement.getFrom();
                    break;
                case primaryKey:
                    value = gdbElement.getId();
                    break;
                case primaryLabel:
                    value = gdbElement.getLabel();
                    break;
                case dstPrimaryLabel:
                    value = gdbElement.getToLabel();
                    break;
                case srcPrimaryLabel:
                    value = gdbElement.getFromLabel();
                    break;
                case vertexProperty:
                    value = forVertexOnePropertyValue().apply(props.get(name));
                    break;
                case edgeProperty:
                    value = forEdgePropertyValue().apply(props.get(name));
                    break;
                case edgeJsonProperty:
                    value = forEdgeJsonProperties().apply(props);
                    break;
                case vertexJsonProperty:
                    value = forVertexJsonProperties().apply(props);
                    break;
                default:
                    break;
            }
            record.addColumn(type.applyObject(value));
        });
    }


    /**
     * parser ReferenceProperty value for edge
     *
     * @return property value
     */
    private static Function<Object, Object> forEdgePropertyValue() {
        return prop -> {
            if (prop instanceof ReferenceProperty) {
                return ((ReferenceProperty) prop).value();
            }
            return null;
        };
    }

    /**
     * parser ReferenceVertexProperty value for vertex
     *
     * @return the first property value in list
     */
    private static Function<Object, Object> forVertexOnePropertyValue() {
        return props -> {
            if (props instanceof List<?>) {
                // get the first one property if more than one
                Object o = ((List) props).get(0);
                if (o instanceof ReferenceVertexProperty) {
                    return ((ReferenceVertexProperty) o).value();
                }
            }
            return null;
        };
    }

    /**
     * parser all edge properties to json string
     *
     * @return json string
     */
    private static Function<Map<String, Object>, String> forEdgeJsonProperties() {
        return props -> "{\"properties\":[" +
                props.entrySet().stream().filter(p -> p.getValue() instanceof ReferenceProperty)
                        .map(p -> "{\"k\":\"" + ((ReferenceProperty) p.getValue()).key() + "\"," +
                                "\"t\":\"" + ((ReferenceProperty) p.getValue()).value().getClass().getSimpleName().toLowerCase() + "\"," +
                                "\"v\":\"" + String.valueOf(((ReferenceProperty) p.getValue()).value()) + "\"}")
                        .collect(Collectors.joining(",")) +
                "]}";
    }

    /**
     * parser all vertex properties to json string, include set-property
     *
     * @return json string
     */
    private static Function<Map<String, Object>, String> forVertexJsonProperties() {
        return props -> "{\"properties\":[" +
                props.entrySet().stream().filter(p -> p.getValue() instanceof List<?>)
                        .map(p -> forVertexPropertyStr().apply((List<?>) p.getValue()))
                        .collect(Collectors.joining(",")) +
                "]}";
    }

    /**
     * parser one vertex property to json string item, set 'cardinality'
     *
     * @return json string item
     */
    private static Function<List<?>, String> forVertexPropertyStr() {
        return vp -> {
            final String setFlag = vp.size() > 1 ? "set" : "single";
            return vp.stream().filter(p -> p instanceof ReferenceVertexProperty)
                    .map(p -> "{\"k\":\"" + ((ReferenceVertexProperty) p).key() + "\"," +
                            "\"t\":\"" + ((ReferenceVertexProperty) p).value().getClass().getSimpleName().toLowerCase() + "\"," +
                            "\"v\":\"" + String.valueOf(((ReferenceVertexProperty) p).value()) + "\"," +
                            "\"c\":\"" + setFlag + "\"}")
                    .collect(Collectors.joining(","));
        };
    }
}
