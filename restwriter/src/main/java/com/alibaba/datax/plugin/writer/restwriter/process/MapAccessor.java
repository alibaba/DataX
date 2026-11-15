package com.alibaba.datax.plugin.writer.restwriter.process;

import java.util.Map;

import org.springframework.asm.MethodVisitor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.CodeFlow;
import org.springframework.expression.spel.CompilablePropertyAccessor;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @version 1.0
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 * @date 2023/10/25 15:14
 * @description:
 **/
public class MapAccessor implements CompilablePropertyAccessor {
    
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class<?>[] { Map.class };
    }
    
    @Override
    public boolean canRead(@NonNull final EvaluationContext context,
            @Nullable final Object target, @NonNull final String name)
            throws AccessException {
        return (target instanceof Map
                && ((Map<?, ?>) target).containsKey(name));
    }
    
    @NonNull
    @Override
    public TypedValue read(@NonNull final EvaluationContext context,
            @Nullable final Object target, @NonNull final String name)
            throws AccessException {
        Assert.state(target instanceof Map, "Target must be of type Map");
        final Map<?, ?> map = (Map<?, ?>) target;
        final Object value = map.get(name);
        if (value == null && !map.containsKey(name)) {
            throw new MapAccessException(name);
        }
        return new TypedValue(value);
    }
    
    @Override
    public boolean canWrite(@NonNull final EvaluationContext context,
            @Nullable final Object target, @NonNull final String name)
            throws AccessException {
        return true;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void write(@NonNull final EvaluationContext context,
            @Nullable final Object target, @NonNull final String name,
            @Nullable final Object newValue) throws AccessException {
        
        Assert.state(target instanceof Map, "Target must be a Map");
        final Map<Object, Object> map = (Map<Object, Object>) target;
        map.put(name, newValue);
    }
    
    @Override
    public boolean isCompilable() {
        return true;
    }
    
    @NonNull
    @Override
    public Class<?> getPropertyType() {
        return Object.class;
    }
    
    @Override
    public void generateCode(@NonNull final String propertyName,
            @NonNull final MethodVisitor mv, final CodeFlow cf) {
        final String descriptor = cf.lastDescriptor();
        if (descriptor == null || !descriptor.equals("Ljava/util/Map")) {
            if (descriptor == null) {
                cf.loadTarget(mv);
            }
            CodeFlow.insertCheckCast(mv, "Ljava/util/Map");
        }
        mv.visitLdcInsn(propertyName);
        mv.visitMethodInsn(INVOKEINTERFACE, "java/util/Map", "get",
                "(Ljava/lang/Object;)Ljava/lang/Object;", true);
    }
    
    /**
     * Exception thrown from {@code read} in order to reset a cached
     * PropertyAccessor, allowing other accessors to have a try.
     */
    @SuppressWarnings("serial")
    private static class MapAccessException extends AccessException {
        
        private final String key;
        
        public MapAccessException(final String key) {
            super("");
            this.key = key;
        }
        
        @Override
        public String getMessage() {
            return "Map does not contain a value for key '" + this.key + "'";
        }
    }
    
}
