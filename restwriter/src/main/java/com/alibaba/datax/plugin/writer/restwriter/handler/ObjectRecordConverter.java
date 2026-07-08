package com.alibaba.datax.plugin.writer.restwriter.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ClassUtils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.restwriter.conf.Field;
import com.google.common.collect.Maps;

import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.EMPTY_FIELD_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.EMPTY_RECORD_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.FIELD_CLASS_NOT_FOUND_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.FIELD_MISMATCH_WITH_COLUMN_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.TYPE_HANDLER_NOT_FOUND_EXCEPTION;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 14:25
 **/
public class ObjectRecordConverter
        implements RecordConverter<Map<String, Object>> {
    
    private final TypeHandlerRegistry registry;
    
    private final List<Field> fields;
    
    private final Map<String, Class<?>> fieldClasses;
    
    public ObjectRecordConverter(final TypeHandlerRegistry registry,
            final List<Field> fields) {
        this.registry = registry;
        this.fields = fields;
        this.fieldClasses = new HashMap<>();
        if (!fields.isEmpty()) {
            fields.forEach(field -> {
                if (isNotBlank(field.getType())) {
                    try {
                        this.fieldClasses.put(field.getName(),
                                ClassUtils.getClass(field.getType()));
                    } catch (final ClassNotFoundException e) {
                        throw DataXException.asDataXException(
                                FIELD_CLASS_NOT_FOUND_EXCEPTION,
                                String.format("field %s type %s not found",
                                        field.getName(), field.getType()),
                                e);
                    }
                } else {
                    this.fieldClasses.put(field.getName(), Void.class);
                }
            });
        } else {
            throw DataXException.asDataXException(EMPTY_FIELD_EXCEPTION,
                    "you should configure at least one field");
        }
    }
    
    @Override
    public Map<String, Object> convert(final Record record) {
        if (record.getColumnNumber() <= 0) {
            throw DataXException.asDataXException(EMPTY_RECORD_EXCEPTION,
                    "record is empty");
        }
        if (this.fields.size() > record.getColumnNumber()) {
            throw DataXException.asDataXException(
                    FIELD_MISMATCH_WITH_COLUMN_EXCEPTION,
                    "number of fields is less than number of columns of record");
        }
        final Map<String, Object> m = Maps.newHashMap();
        IntStream.range(0, this.fields.size()).forEach(num -> {
            final Column column = record.getColumn(num);
            final Class<?> clazz = this.fieldClasses
                    .get(this.fields.get(num).getName());
            final TypeHandler<?> typeHandler = this.registry
                    .getTypeHandler(column.getType(), clazz);
            if (isNull(typeHandler)) {
                throw DataXException.asDataXException(
                        TYPE_HANDLER_NOT_FOUND_EXCEPTION,
                        String.format(
                                "type handler not found for source type %s and target class %s",
                                column.getType().name(), clazz.getName()));
            }
            m.put(this.fields.get(num).getName(),
                    typeHandler.convert(column.getRawData()));
        });
        return m;
    }
}
