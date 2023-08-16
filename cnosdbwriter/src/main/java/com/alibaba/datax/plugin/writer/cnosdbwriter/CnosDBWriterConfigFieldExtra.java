package com.alibaba.datax.plugin.writer.cnosdbwriter;

import com.alibaba.datax.common.util.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CnosDBWriterConfigFieldExtra {
    private String table;
    private String field;
    /**
     * Config fieldsExtra[*].tagsExtra, will never be null.
     */
    private Map<String, String> tagsExtra;

    public CnosDBWriterConfigFieldExtra(Configuration config) {
        this.table = config.getString(CnosDBWriter.CFG_TABLE);
        this.field = config.getString(CnosDBWriter.CFG_FIELD);
        this.tagsExtra = config.getMap(CnosDBWriter.CFG_TAGS_EXTRA, String.class);
        if (this.tagsExtra == null) {
            this.tagsExtra = new HashMap<>();
        }
    }

    public CnosDBWriterConfigFieldExtra(String table, String field, Map<String, String> tagsExtra) {
        this.table = table;
        this.field = field;
        this.tagsExtra = tagsExtra;
        if (this.tagsExtra == null) {
            this.tagsExtra = new HashMap<>();
        }
    }

    public void check() throws Exception {
        if (this.table.isEmpty()) {
            throw new Exception("配置项 'table' 不能为空");
        }
        if (this.field.isEmpty()) {
            throw new Exception("配置项 'field' 不能为空");
        }
        if (this.tagsExtra != null) {
            for (Map.Entry<String, String> entry : this.tagsExtra.entrySet()) {
                if (entry.getKey().isEmpty()) {
                    throw new Exception("配置项 'tagsExtra[*].key' 不能为空");
                }
                if (entry.getValue().isEmpty()) {
                    throw new Exception("配置项 'tagsExtra[*].value' 不能为空");
                }
            }
        }
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Map<String, String> getTagsExtra() {
        return tagsExtra;
    }

    public void setTagsExtra(Map<String, String> tagsExtra) {
        this.tagsExtra = tagsExtra;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CnosDBWriterConfigFieldExtra that = (CnosDBWriterConfigFieldExtra) o;
        return Objects.equals(table, that.table)
                && Objects.equals(field, that.field)
                && Objects.equals(tagsExtra, that.tagsExtra);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, field, tagsExtra);
    }
}
