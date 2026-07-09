package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import java.util.List;

import com.leehom.arch.datax.plugin.rdb2graph.common.StringUtils;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;

public class SchemaUtils {

	/** 字段分割*/
	public static final String FIELD_SEQ = ",";
	
	// 
    public static String[] extractTableNames(String prefix, List<TableMetadata> tables) {
		if (tables == null || tables.size() == 0)
			return new String[0];
		String[] tns = new String[tables.size()];
		for (int i = 0; i < tables.size(); i++) {
			tns[i] = tables.get(i).getName();
		}
		return tns;
    }
    
    // 字段名排列
    public static String extractFieldNames(List<FieldMetadata> fields) {
    	return extractFieldNames(null, fields);
    }
    
    public static String extractFieldNames(String prefix, List<FieldMetadata> fields) {
    	return extractFieldNames(prefix, fields, FIELD_SEQ);
    }
    
    public static String extractFieldNames(String prefix, List<FieldMetadata> fields, String seq) {
		if (fields == null || fields.size() == 0)
			return "";
		StringBuffer sb = new StringBuffer(); 
		for (int i = 0; i < fields.size(); i++) {
			String tmp = StringUtils.isNotEmpty(prefix) ? prefix+"."+fields.get(i).getName() : fields.get(i).getName() + seq;
			sb.append(tmp);
		}
		// 去掉最后seq
		sb.delete(sb.length()-seq.length(), sb.length());
		return sb.toString();
    }
    
    // 
    public static String extractFieldWhereNotNull(List<FieldMetadata> fields, String seq) {
    	return extractFieldWhereNotNull("", fields, seq);
    	
    }
    public static String extractFieldWhereNotNull(String prefix, List<FieldMetadata> fields, String seq) {
		if (fields == null || fields.size() == 0)
			return "";
		StringBuffer sb = new StringBuffer(); 
		for (int i = 0; i < fields.size(); i++) {
			String fieldItem = fields.get(i).getName() + " is not null ";
			String tmp = StringUtils.isNotEmpty(prefix) ? prefix+"."+fieldItem : fieldItem + seq;
			sb.append(tmp);
		}
		// 去掉最后seq
		sb.delete(sb.length()-seq.length(), sb.length());
		return sb.toString();
    }

}
