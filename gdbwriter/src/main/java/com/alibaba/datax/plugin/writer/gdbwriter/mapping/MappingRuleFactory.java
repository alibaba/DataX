/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.GdbWriterErrorCode;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.Key.ImportType;
import com.alibaba.datax.plugin.writer.gdbwriter.Key.IdTransRule;
import com.alibaba.datax.plugin.writer.gdbwriter.Key.ColumnType;
import com.alibaba.datax.plugin.writer.gdbwriter.mapping.MappingRule.PropertyMappingRule;
import com.alibaba.datax.plugin.writer.gdbwriter.util.ConfigHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author jerrywang
 *
 */
@Slf4j
public class MappingRuleFactory {
	private static final MappingRuleFactory instance = new MappingRuleFactory();
	
	public static final MappingRuleFactory getInstance() {
		return instance;
	}

	@Deprecated
	public MappingRule create(Configuration config, ImportType type) {
		MappingRule rule = new MappingRule();
		rule.setId(config.getString(Key.ID));
		rule.setLabel(config.getString(Key.LABEL));
		if (type == ImportType.EDGE) {
			rule.setFrom(config.getString(Key.FROM));
			rule.setTo(config.getString(Key.TO));
		}
		
		rule.setImportType(type);

		List<Configuration> configurations = config.getListConfiguration(Key.PROPERTIES);
		if (configurations != null) {
			for (Configuration prop : config.getListConfiguration(Key.PROPERTIES)) {
				PropertyMappingRule propRule = new PropertyMappingRule();
				propRule.setKey(prop.getString(Key.PROP_KEY));
				propRule.setValue(prop.getString(Key.PROP_VALUE));
				propRule.setValueType(ValueType.fromShortName(prop.getString(Key.PROP_TYPE).toLowerCase()));
				rule.getProperties().add(propRule);
			}
		}

		String propertiesJsonStr = config.getString(Key.PROPERTIES_JSON_STR, null);
		if (propertiesJsonStr != null) {
			rule.setPropertiesJsonStr(propertiesJsonStr);
		}

		return rule;
	}

	public MappingRule createV2(Configuration config) {
		try {
			ImportType type = ImportType.valueOf(config.getString(Key.IMPORT_TYPE));
			return createV2(config, type);
		} catch (NullPointerException e) {
			throw DataXException.asDataXException(GdbWriterErrorCode.CONFIG_ITEM_MISS, Key.IMPORT_TYPE);
		} catch (IllegalArgumentException e) {
			throw DataXException.asDataXException(GdbWriterErrorCode.BAD_CONFIG_VALUE, Key.IMPORT_TYPE);
		}
    }

	public MappingRule createV2(Configuration config, ImportType type) {
        MappingRule rule = new MappingRule();

		ConfigHelper.assertHasContent(config, Key.LABEL);
		rule.setLabel(config.getString(Key.LABEL));
		rule.setImportType(type);

		IdTransRule srcTransRule = IdTransRule.none;
        IdTransRule dstTransRule = IdTransRule.none;
        if (type == ImportType.EDGE) {
	        ConfigHelper.assertHasContent(config, Key.SRC_ID_TRANS_RULE);
	        ConfigHelper.assertHasContent(config, Key.DST_ID_TRANS_RULE);

            srcTransRule = IdTransRule.valueOf(config.getString(Key.SRC_ID_TRANS_RULE));
            dstTransRule = IdTransRule.valueOf(config.getString(Key.DST_ID_TRANS_RULE));

            if (srcTransRule == IdTransRule.labelPrefix) {
                ConfigHelper.assertHasContent(config, Key.SRC_LABEL);
            }

            if (dstTransRule == IdTransRule.labelPrefix) {
                ConfigHelper.assertHasContent(config, Key.DST_LABEL);
            }
        }
		ConfigHelper.assertHasContent(config, Key.ID_TRANS_RULE);
        IdTransRule transRule = IdTransRule.valueOf(config.getString(Key.ID_TRANS_RULE));

		List<Configuration> configurationList = config.getListConfiguration(Key.COLUMN);
		ConfigHelper.assertConfig(Key.COLUMN, () -> (configurationList != null && !configurationList.isEmpty()));
		for (Configuration column : configurationList) {
			ConfigHelper.assertHasContent(column, Key.COLUMN_NAME);
			ConfigHelper.assertHasContent(column, Key.COLUMN_VALUE);
			ConfigHelper.assertHasContent(column, Key.COLUMN_TYPE);
			ConfigHelper.assertHasContent(column, Key.COLUMN_NODE_TYPE);

			String columnValue = column.getString(Key.COLUMN_VALUE);
			ColumnType columnType = ColumnType.valueOf(column.getString(Key.COLUMN_NODE_TYPE));
			if (columnValue == null || columnValue.isEmpty()) {
			    // only allow edge empty id
				ConfigHelper.assertConfig("empty column value",
					() -> (type == ImportType.EDGE && columnType == ColumnType.primaryKey));
			}

			if (columnType == ColumnType.primaryKey) {
				ValueType propType = ValueType.fromShortName(column.getString(Key.COLUMN_TYPE));
				ConfigHelper.assertConfig("only string is allowed in primary key", () -> (propType == ValueType.STRING));

				if (transRule == IdTransRule.labelPrefix) {
					rule.setId(config.getString(Key.LABEL) + columnValue);
				} else {
					rule.setId(columnValue);
				}
			} else if (columnType == ColumnType.edgeJsonProperty || columnType == ColumnType.vertexJsonProperty) {
				// only support one json property in column
				ConfigHelper.assertConfig("multi JsonProperty", () -> (rule.getPropertiesJsonStr() == null));

				rule.setPropertiesJsonStr(columnValue);
			} else if (columnType == ColumnType.vertexProperty || columnType == ColumnType.edgeProperty) {
				PropertyMappingRule propertyMappingRule = new PropertyMappingRule();

				propertyMappingRule.setKey(column.getString(Key.COLUMN_NAME));
				propertyMappingRule.setValue(columnValue);
				ValueType propType = ValueType.fromShortName(column.getString(Key.COLUMN_TYPE));
				ConfigHelper.assertConfig("unsupported property type", () -> propType != null);

				propertyMappingRule.setValueType(propType);
				rule.getProperties().add(propertyMappingRule);
			} else if (columnType == ColumnType.srcPrimaryKey) {
			    if (type != ImportType.EDGE) {
			        continue;
                }

				ValueType propType = ValueType.fromShortName(column.getString(Key.COLUMN_TYPE));
				ConfigHelper.assertConfig("only string is allowed in primary key", () -> (propType == ValueType.STRING));

			    if (srcTransRule == IdTransRule.labelPrefix) {
			        rule.setFrom(config.getString(Key.SRC_LABEL) + columnValue);
                } else {
                    rule.setFrom(columnValue);
                }
			} else if (columnType == ColumnType.dstPrimaryKey) {
                if (type != ImportType.EDGE) {
                    continue;
                }

				ValueType propType = ValueType.fromShortName(column.getString(Key.COLUMN_TYPE));
				ConfigHelper.assertConfig("only string is allowed in primary key", () -> (propType == ValueType.STRING));

                if (dstTransRule == IdTransRule.labelPrefix) {
                    rule.setTo(config.getString(Key.DST_LABEL) + columnValue);
                } else {
                    rule.setTo(columnValue);
                }
			}
		}

		if (rule.getImportType() == ImportType.EDGE) {
			if (rule.getId() == null) {
				rule.setId("");
				log.info("edge id is missed, uuid be default");
			}
			ConfigHelper.assertConfig("to needed in edge", () -> (rule.getTo() != null));
			ConfigHelper.assertConfig("from needed in edge", () -> (rule.getFrom() != null));
		}
		ConfigHelper.assertConfig("id needed", () -> (rule.getId() != null));

		return rule;
	}
}
