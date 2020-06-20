/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import java.util.LinkedList;
import java.util.List;

import com.alibaba.datax.plugin.writer.gdbwriter.Key.PropertyType;
import com.alibaba.datax.plugin.writer.gdbwriter.mapping.MapperConfig;

/**
 * @author jerrywang
 *
 */
public class GdbElement {
    private String id = null;
    private String label = null;
    private List<GdbProperty> properties = new LinkedList<>();

    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        final int maxIdLength = MapperConfig.getInstance().getMaxIdLength();
        if (id.length() > maxIdLength) {
            throw new IllegalArgumentException("id length over limit(" + maxIdLength + ")");
        }
        this.id = id;
    }

    public String getLabel() {
        return this.label;
    }

    public void setLabel(final String label) {
        final int maxLabelLength = MapperConfig.getInstance().getMaxLabelLength();
        if (label.length() > maxLabelLength) {
            throw new IllegalArgumentException("label length over limit(" + maxLabelLength + ")");
        }
        this.label = label;
    }

    public List<GdbProperty> getProperties() {
        return this.properties;
    }

    public void addProperty(final String propKey, final Object propValue, final PropertyType card) {
        if (propKey == null || propValue == null) {
            return;
        }

        final int maxPropKeyLength = MapperConfig.getInstance().getMaxPropKeyLength();
        if (propKey.length() > maxPropKeyLength) {
            throw new IllegalArgumentException("property key length over limit(" + maxPropKeyLength + ")");
        }
        if (propValue instanceof String) {
            final int maxPropValueLength = MapperConfig.getInstance().getMaxPropValueLength();
            if (((String)propValue).length() > maxPropKeyLength) {
                throw new IllegalArgumentException("property value length over limit(" + maxPropValueLength + ")");
            }
        }

        this.properties.add(new GdbProperty(propKey, propValue, card));
    }

    public void addProperty(final String propKey, final Object propValue) {
        addProperty(propKey, propValue, PropertyType.single);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer(this.id + "[" + this.label + "]{");
        this.properties.forEach(n -> {
            sb.append(n.cardinality.name());
            sb.append("[");
            sb.append(n.key);
            sb.append(" - ");
            sb.append(String.valueOf(n.value));
            sb.append("]");
        });
        return sb.toString();
    }

    public static class GdbProperty {
        private String key;
        private Object value;
        private PropertyType cardinality;

        private GdbProperty(final String key, final Object value, final PropertyType card) {
            this.key = key;
            this.value = value;
            this.cardinality = card;
        }

        public PropertyType getCardinality() {
            return this.cardinality;
        }

        public String getKey() {
            return this.key;
        }

        public Object getValue() {
            return this.value;
        }
    }
}
