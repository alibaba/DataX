package com.leehom.arch.datax.plugin.rdb2graph.transformer;

import java.util.List;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;

public class StringUtils {

	/** */
	public static final String FIELD_SEQ = ",";
	
	// 字段名排列
    public static String fieldNames2String(String prefix, List<FieldMetadata> fields) {
		if (fields == null || fields.size() == 0)
			return "";
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < fields.size(); i++) {
			FieldMetadata f = fields.get(i);
			sb.append(prefix+f.getName());
			if (i != fields.size() - 1)
				sb.append(FIELD_SEQ);
		}
		return sb.toString();
    }

}
