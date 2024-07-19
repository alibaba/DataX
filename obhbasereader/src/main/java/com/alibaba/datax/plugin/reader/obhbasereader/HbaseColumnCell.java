package com.alibaba.datax.plugin.reader.obhbasereader;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.ColumnType;
import com.alibaba.datax.plugin.reader.obhbasereader.util.ObHbaseReaderUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 描述 hbasereader 插件中，column 配置中的一个单元项实体
 */
public class HbaseColumnCell extends BaseObject {
    private ColumnType columnType;

    // columnName 格式为：列族:列名
    private String columnName;

    private byte[] cf;
    private byte[] qualifier;

    //对于常量类型，其常量值放到 columnValue 里
    private String columnValue;

    //当配置了 columnValue 时，isConstant=true（这个成员变量是用于方便使用本类的地方判断是否是常量类型字段）
    private boolean isConstant;

    // 只在类型是时间类型时，才会设置该值，无默认值。形式如：yyyy-MM-dd HH:mm:ss
    private String dateformat;

    private HbaseColumnCell(Builder builder) {
        this.columnType = builder.columnType;

        //columnName 和 columnValue 必须有一个为 null
        Validate.isTrue(builder.columnName == null || builder.columnValue == null, "In obhbasereader, column cannot configure both column name and column value. Choose one of them.");

        //columnName 和 columnValue 不能都为 null
        Validate.isTrue(builder.columnName != null || builder.columnValue != null, "In obhbasereader, column cannot configure both column name and column value. Choose one of them.");

        if (builder.columnName != null) {
            this.isConstant = false;
            this.columnName = builder.columnName;

            // 如果 columnName 不是 rowkey，则必须配置为：列族:列名 格式
            if (!ObHbaseReaderUtil.isRowkeyColumn(this.columnName)) {

                String promptInfo = "In obhbasereader, the column configuration format of column should be: 'family:column'. The column you configured is wrong:" + this.columnName;
                String[] cfAndQualifier = this.columnName.split(":");
                Validate.isTrue(cfAndQualifier.length == 2 && StringUtils.isNotBlank(cfAndQualifier[0]) && StringUtils.isNotBlank(cfAndQualifier[1]), promptInfo);

                this.cf = Bytes.toBytes(cfAndQualifier[0].trim());
                this.qualifier = Bytes.toBytes(cfAndQualifier[1].trim());
            }
        } else {
            this.isConstant = true;
            this.columnValue = builder.columnValue;
        }

        if (builder.dateformat != null) {
            this.dateformat = builder.dateformat;
        }
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public byte[] getCf() {
        return cf;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public String getDateformat() {
        return dateformat;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public boolean isConstant() {
        return isConstant;
    }

    // 内部 builder 类
    public static class Builder {
        private ColumnType columnType;
        private String columnName;
        private String columnValue;

        private String dateformat;

        public Builder(ColumnType columnType) {
            this.columnType = columnType;
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder columnValue(String columnValue) {
            this.columnValue = columnValue;
            return this;
        }

        public Builder dateformat(String dateformat) {
            this.dateformat = dateformat;
            return this;
        }

        public HbaseColumnCell build() {
            return new HbaseColumnCell(this);
        }
    }
}
