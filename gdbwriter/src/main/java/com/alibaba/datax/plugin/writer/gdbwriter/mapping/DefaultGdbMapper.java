/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbEdge;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbElement;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbVertex;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.datax.plugin.writer.gdbwriter.Key.ImportType.VERTEX;

/**
 * @author jerrywang
 *
 */
@Slf4j
public class DefaultGdbMapper implements GdbMapper {
	private static final Pattern STR_PATTERN = Pattern.compile("\\$\\{(\\d+)}");
	private static final Pattern NORMAL_PATTERN = Pattern.compile("^\\$\\{(\\d+)}$");

	@Override
	public Function<Record, GdbElement> getMapper(MappingRule rule) {
	    return r -> {
	        GdbElement e = (rule.getImportType() == VERTEX) ? new GdbVertex() : new GdbEdge();
	        forElement(rule).accept(r, e);
	        return e;
        };
	}

	private static BiConsumer<Record, GdbElement> forElement(MappingRule rule) {
		List<BiConsumer<Record, GdbElement>> properties = new ArrayList<>();
		for (MappingRule.PropertyMappingRule propRule : rule.getProperties()) {
			Function<Record, String> keyFunc = forStrColumn(propRule.getKey());

			if (propRule.getValueType() == ValueType.STRING) {
				final Function<Record, String> valueFunc = forStrColumn(propRule.getValue());
				properties.add((r, e) -> {
					String k = keyFunc.apply(r);
					String v = valueFunc.apply(r);
					if (k != null && v != null) {
						e.getProperties().put(k, v);
					}
				});
			} else {
				final Function<Record, Object> valueFunc = forObjColumn(propRule.getValue(), propRule.getValueType());
				properties.add((r, e) -> {
					String k = keyFunc.apply(r);
					Object v = valueFunc.apply(r);
					if (k != null && v != null) {
						e.getProperties().put(k, v);
					}
				});
			}
		}

		if (rule.getPropertiesJsonStr() != null) {
			Function<Record, String> jsonFunc = forStrColumn(rule.getPropertiesJsonStr());
			properties.add((r, e) -> {
				String propertiesStr = jsonFunc.apply(r);
				JSONObject root = (JSONObject)JSONObject.parse(propertiesStr);
				JSONArray propertiesList = root.getJSONArray("properties");

				for (Object object : propertiesList) {
					JSONObject jsonObject = (JSONObject)object;
					String key = jsonObject.getString("k");
					String name = jsonObject.getString("v");
					String type = jsonObject.getString("t");

					if (key == null || name == null) {
						continue;
					}
					addToProperties(e, key, name, type);
				}
			});
		}

		BiConsumer<Record, GdbElement> ret = (r, e) -> {
			String label = forStrColumn(rule.getLabel()).apply(r);
			String id = forStrColumn(rule.getId()).apply(r);

			if (rule.getImportType() == Key.ImportType.EDGE) {
				String to = forStrColumn(rule.getTo()).apply(r);
				String from = forStrColumn(rule.getFrom()).apply(r);
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

	static Function<Record, Object> forObjColumn(String rule, ValueType type) {
		Matcher m = NORMAL_PATTERN.matcher(rule);
		if (m.matches()) {
			int index = Integer.valueOf(m.group(1));
			return r -> type.applyColumn(r.getColumn(index));
		} else {
			return r -> type.fromStrFunc(rule);
		}
	}

	static Function<Record, String> forStrColumn(String rule) {
		List<BiConsumer<StringBuilder, Record>> list = new ArrayList<>();
		Matcher m = STR_PATTERN.matcher(rule);
		int last = 0;
		while (m.find()) {
			String index = m.group(1);
			// as simple integer index.
			int i = Integer.parseInt(index);
			
			final int tmp = last;
			final int start = m.start();
			list.add((sb, record) -> {
				sb.append(rule.subSequence(tmp, start));
				if(record.getColumn(i) != null && record.getColumn(i).getByteSize() > 0) {
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
			StringBuilder sb = new StringBuilder();
			list.forEach(c -> c.accept(sb, r));
			String res = sb.toString();
			return res.isEmpty() ? null : res;
		};
	}

	static boolean addToProperties(GdbElement e, String key, String value, String type) {
		ValueType valueType = ValueType.fromShortName(type);

		if(valueType == ValueType.STRING) {
			e.getProperties().put(key, value);
		} else if (valueType == ValueType.INT) {
			e.getProperties().put(key, Integer.valueOf(value));
		} else if (valueType == ValueType.LONG) {
			e.getProperties().put(key, Long.valueOf(value));
		} else if (valueType == ValueType.DOUBLE) {
			e.getProperties().put(key, Double.valueOf(value));
		} else if (valueType == ValueType.FLOAT) {
			e.getProperties().put(key, Float.valueOf(value));
		} else if (valueType == ValueType.BOOLEAN) {
			e.getProperties().put(key, Boolean.valueOf(value));
		} else {
			log.error("invalid property key {}, value {}, type {}", key, value, type);
			return false;
		}

		return true;
	}
}
