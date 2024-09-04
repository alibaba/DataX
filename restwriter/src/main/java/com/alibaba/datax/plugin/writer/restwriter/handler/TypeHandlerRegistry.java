package com.alibaba.datax.plugin.writer.restwriter.handler;

import java.time.LocalDateTime;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.writer.restwriter.handler.bool.BoolVoidTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.bytes.BytesVoidTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.date.DateLocalDateTimeTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.date.DateVoidTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.string.StringVoidTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.typedouble.DoubleVoidTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.typeint.IntVoidTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.typelong.LongVoidTypeHandler;
import com.alibaba.datax.plugin.writer.restwriter.handler.typenull.NullVoidTypeHandler;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:03
 **/
public class TypeHandlerRegistry {
    
    private final Table<Column.Type, Class<?>, TypeHandler<?>> handlers = HashBasedTable
            .create();
    
    public TypeHandlerRegistry() {
        registerDefault(Column.Type.INT, new IntVoidTypeHandler());
        registerDefault(Column.Type.LONG, new LongVoidTypeHandler());
        registerDefault(Column.Type.NULL, new NullVoidTypeHandler());
        registerDefault(Column.Type.DOUBLE, new DoubleVoidTypeHandler());
        registerDefault(Column.Type.STRING, new StringVoidTypeHandler());
        registerDefault(Column.Type.BOOL, new BoolVoidTypeHandler());
        registerDefault(Column.Type.DATE, new DateVoidTypeHandler());
        registerDefault(Column.Type.BYTES, new BytesVoidTypeHandler());
        register(Column.Type.DATE, LocalDateTime.class,
                new DateLocalDateTimeTypeHandler());
    }
    
    // BAD, NULL, INT, LONG, DOUBLE, STRING, BOOL, DATE, BYTES
    
    <T> void register(final Column.Type type, final Class<T> targetClass,
            final TypeHandler<T> typeHandler) {
        this.handlers.put(type, targetClass, typeHandler);
    }
    
    void registerDefault(final Column.Type type,
            final TypeHandler<Object> typeHandler) {
        this.handlers.put(type, Void.class, typeHandler);
    }
    
    <T> boolean hasTypeHandler(final Column.Type type,
            final Class<T> targetClass) {
        return this.handlers.contains(type, targetClass);
    }
    
    <T> TypeHandler<T> getTypeHandler(final Column.Type type,
            final Class<T> targetClass) {
        return (TypeHandler<T>) this.handlers.get(type, targetClass);
    }
    
}
