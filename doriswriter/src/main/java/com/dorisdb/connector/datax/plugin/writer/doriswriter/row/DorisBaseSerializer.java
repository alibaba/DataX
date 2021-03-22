package com.dorisdb.connector.datax.plugin.writer.doriswriter.row;

import java.util.TimeZone;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Column.Type;
import com.alibaba.datax.common.util.Configuration;

import org.apache.commons.lang3.time.DateFormatUtils;

public class DorisBaseSerializer {

    static String datetimeFormat = "yyyy-MM-dd HH:mm:ss";

	static String dateFormat = "yyyy-MM-dd";

	static String timeFormat = "HH:mm:ss";

	static String timeZone = "GMT+8";

	static TimeZone timeZoner = TimeZone.getTimeZone(DorisBaseSerializer.timeZone);

	static void init(final Configuration configuration) {
		DorisBaseSerializer.datetimeFormat = configuration.getString("common.column.datetimeFormat", datetimeFormat);
        DorisBaseSerializer.timeFormat = configuration.getString("common.column.timeFormat", timeFormat);
        DorisBaseSerializer.dateFormat = configuration.getString("common.column.dateFormat", dateFormat);
        DorisBaseSerializer.timeZone = configuration.getString("common.column.timeZone", DorisBaseSerializer.timeZone);
        DorisBaseSerializer.timeZoner = TimeZone.getTimeZone(DorisBaseSerializer.timeZone);
	}

    protected String fieldConvertion(Column col) {
        if (null == col.getRawData()) {
            return null;
        }
        if (Type.DATE != col.getType()) {
            return col.asString();
        }
        DateColumn.DateType type = ((DateColumn)col).getSubType();
        if (type == DateColumn.DateType.DATE) {
            return DateFormatUtils.format(col.asDate(),  DorisBaseSerializer.dateFormat, DorisBaseSerializer.timeZoner);
        }
        if (type == DateColumn.DateType.TIME) {
            return DateFormatUtils.format(col.asDate(), DorisBaseSerializer.timeFormat, DorisBaseSerializer.timeZoner);
        }
        if (type == DateColumn.DateType.DATETIME) {
            return DateFormatUtils.format(col.asDate(), DorisBaseSerializer.datetimeFormat, DorisBaseSerializer.timeZoner);
        }
        return null;
    }
    
}
