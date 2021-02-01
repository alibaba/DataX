package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.ClassSize;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-8-24.
 */

public class DefaultRecord implements Record {

	private static final int RECORD_AVERGAE_COLUMN_NUMBER = 16;

	private List<Column> columns;

	private int byteSize;

	// 首先是Record本身需要的内存
	private int memorySize = ClassSize.DefaultRecordHead;

	public DefaultRecord() {
		this.columns = new ArrayList<Column>(RECORD_AVERGAE_COLUMN_NUMBER);
	}

	@Override
	public void addColumn(Column column) {
		columns.add(column);
		incrByteSize(column);
	}

	@Override
	public Column getColumn(int i) {
		if (i < 0 || i >= columns.size()) {
			return null;
		}
		return columns.get(i);
	}

	@Override
	public void setColumn(int i, final Column column) {
		if (i < 0) {
			throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR,
					"不能给index小于0的column设置值");
		}

		if (i >= columns.size()) {
			expandCapacity(i + 1);
		}

		decrByteSize(getColumn(i));
		this.columns.set(i, column);
		incrByteSize(getColumn(i));
	}

	@Override
	public String toString() {
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("size", this.getColumnNumber());
		json.put("data", this.columns);
		return JSON.toJSONString(json);
	}

	@Override
	public int getColumnNumber() {
		return this.columns.size();
	}

	@Override
	public int getByteSize() {
		return byteSize;
	}

	public int getMemorySize(){
		return memorySize;
	}

	private void decrByteSize(final Column column) {
		if (null == column) {
			return;
		}

		byteSize -= column.getByteSize();

		//内存的占用是column对象的头 再加实际大小
		memorySize = memorySize -  ClassSize.ColumnHead - column.getByteSize();
	}

	private void incrByteSize(final Column column) {
		if (null == column) {
			return;
		}

		byteSize += column.getByteSize();

		//内存的占用是column对象的头 再加实际大小
		memorySize = memorySize + ClassSize.ColumnHead + column.getByteSize();
	}

	private void expandCapacity(int totalSize) {
		if (totalSize <= 0) {
			return;
		}

		int needToExpand = totalSize - columns.size();
		while (needToExpand-- > 0) {
			this.columns.add(null);
		}
	}

}
