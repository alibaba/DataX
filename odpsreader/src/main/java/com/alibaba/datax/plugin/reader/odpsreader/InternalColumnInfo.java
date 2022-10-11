package com.alibaba.datax.plugin.reader.odpsreader;

public class InternalColumnInfo {

	private String columnName;

	private ColumnType columnType;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public ColumnType getColumnType() {
		return columnType;
	}

	public void setColumnType(ColumnType columnType) {
		this.columnType = columnType;
	}
}
