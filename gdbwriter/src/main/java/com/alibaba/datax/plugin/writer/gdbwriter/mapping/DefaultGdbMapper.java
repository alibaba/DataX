/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import static com.alibaba.datax.plugin.writer.gdbwriter.Key.ImportType.VERTEX;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbEdge;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbElement;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbVertex;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import lombok.extern.slf4j.Slf4j;

/**
 * @author jerrywang
 *
 */
@Slf4j
public class DefaultGdbMapper implements GdbMapper {
    private static final Pattern STR_DOLLAR_PATTERN = Pattern.compile("\\$\\{(\\d+)}");
    private static final Pattern NORMAL_DOLLAR_PATTERN = Pattern.compile("^\\$\\{(\\d+)}$");

    private static final Pattern STR_NUM_PATTERN = Pattern.compile("#\\{(\\d+)}");
    private static final Pattern NORMAL_NUM_PATTERN = Pattern.compile("^#\\{(\\d+)}$");

    public DefaultGdbMapper() {}

    public DefaultGdbMapper(final Configuration config) {
        MapperConfig.getInstance().updateConfig(config);
    }

    private static BiConsumer<Record, GdbElement> forElement(final MappingRule rule) {
        final boolean numPattern = rule.isNumPattern();
        final List<BiConsumer<Record, GdbElement>> properties = new ArrayList<>();
        for (final MappingRule.PropertyMappingRule propRule : rule.getProperties()) {
            final Function<Record, String> keyFunc = forStrColumn(numPattern, propRule.getKey());

            if (propRule.getValueType() == ValueType.STRING) {
                final Function<Record, String> valueFunc = forStrColumn(numPattern, propRule.getValue());
                properties.add((r, e) -> {
                    e.addProperty(keyFunc.apply(r), valueFunc.apply(r), propRule.getPType());
                });
            } else {
                final Function<Record, Object> valueFunc =
                    forObjColumn(numPattern, propRule.getValue(), propRule.getValueType());
                properties.add((r, e) -> {
                    e.addProperty(keyFunc.apply(r), valueFunc.apply(r), propRule.getPType());
                });
            }
        }

        if (rule.getPropertiesJsonStr() != null) {
            final Function<Record, String> jsonFunc = forStrColumn(numPattern, rule.getPropertiesJsonStr());
            properties.add((r, e) -> {
                final String propertiesStr = jsonFunc.apply(r);
                final JSONObject root = (JSONObject)JSONObject.parse(propertiesStr);
                final JSONArray propertiesList = root.getJSONArray("properties");

                for (final Object object : propertiesList) {
                    final JSONObject jsonObject = (JSONObject)object;
                    final String key = jsonObject.getString("k");
                    final String name = jsonObject.getString("v");
                    final String type = jsonObject.getString("t");
                    final String card = jsonObject.getString("c");

                    if (key == null || name == null) {
                        continue;
                    }
                    addToProperties(e, key, name, type, card);
                }
            });
        }

        final BiConsumer<Record, GdbElement> ret = (r, e) -> {
            final String label = forStrColumn(numPattern, rule.getLabel()).apply(r);
            String id = forStrColumn(numPattern, rule.getId()).apply(r);

            if (rule.getImportType() == Key.ImportType.EDGE) {
                final String to = forStrColumn(numPattern, rule.getTo()).apply(r);
                final String from = forStrColumn(numPattern, rule.getFrom()).apply(r);
                if (to == null || from == null) {
                    log.error("invalid record to: {} , from: {}", to, from);
                    throw new IllegalArgumentException("to or from missed in edge");
                }
                ((GdbEdge)e).setTo(to);
                ((GdbEdge)e).setFrom(from);

                // generate UUID for edge
                if (id == null) {
                    id = UUID.randomUUID().toString();
                }
            }

            if (id == null || label == null) {
                log.error("invalid record id: {} , label: {}", id, label);
                throw new IllegalArgumentException("id or label missed");
            }

            e.setId(id);
            e.setLabel(label);

            properties.forEach(p -> p.accept(r, e));
        };
        return ret;
    }

    private static Function<Record, Object> forObjColumn(final boolean numPattern, final String rule, final ValueType type) {
        final Pattern pattern = numPattern ? NORMAL_NUM_PATTERN : NORMAL_DOLLAR_PATTERN;
        final Matcher m = pattern.matcher(rule);
        if (m.matches()) {
            final int index = Integer.valueOf(m.group(1));
            return r -> type.applyColumn(r.getColumn(index));
        } else {
            return r -> type.fromStrFunc(rule);
        }
    }

    private static Function<Record, String> forStrColumn(final boolean numPattern, final String rule) {
        final List<BiConsumer<StringBuilder, Record>> list = new ArrayList<>();
        final Pattern pattern = numPattern ? STR_NUM_PATTERN : STR_DOLLAR_PATTERN;
        final Matcher m = pattern.matcher(rule);
        int last = 0;
        while (m.find()) {
            final String index = m.group(1);
            // as simple integer index.
            final int i = Integer.parseInt(index);

            final int tmp = last;
            final int start = m.start();
            list.add((sb, record) -> {
                sb.append(rule.subSequence(tmp, start));
                if (record.getColumn(i) != null && record.getColumn(i).getByteSize() > 0) {
                    sb.append(record.getColumn(i).asString());
                }
            });

            last = m.end();
        }

        final int tmp = last;
        list.add((sb, record) -> {
            sb.append(rule.subSequence(tmp, rule.length()));
        });

        return r -> {
            final StringBuilder sb = new StringBuilder();
            list.forEach(c -> c.accept(sb, r));
            final String res = sb.toString();
            return res.isEmpty() ? null : res;
        };
    }

    private static boolean addToProperties(final GdbElement e, final String key, final String value, final String type, final String card) {
        final Object pValue;
        final ValueType valueType = ValueType.fromShortName(type);

        if (valueType == ValueType.STRING) {
            pValue = value;
        } else if (valueType == ValueType.INT || valueType == ValueType.INTEGER) {
            pValue = Integer.valueOf(value);
        } else if (valueType == ValueType.LONG) {
            pValue = Long.valueOf(value);
        } else if (valueType == ValueType.DOUBLE) {
            pValue = Double.valueOf(value);
        } else if (valueType == ValueType.FLOAT) {
            pValue = Float.valueOf(value);
        } else if (valueType == ValueType.BOOLEAN) {
            pValue = Boolean.valueOf(value);
        } else {
            log.error("invalid property key {}, value {}, type {}", key, value, type);
            return false;
        }

        // apply vertexSetProperty
        if (Key.PropertyType.set.name().equals(card) && (e instanceof GdbVertex)) {
            e.addProperty(key, pValue, Key.PropertyType.set);
        } else {
            e.addProperty(key, pValue);
        }
        return true;
    }

    @Override
    public Function<Record, GdbElement> getMapper(final MappingRule rule) {
        return r -> {
            final GdbElement e = (rule.getImportType() == VERTEX) ? new GdbVertex() : new GdbEdge();
            forElement(rule).accept(r, e);
            return e;
        };
    }
}
