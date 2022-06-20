package com.leehom.arch.datax.plugin.rdb2graph.writer.neo4jwriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.leehom.arch.datax.plugin.rdb2graph.common.StringUtils;

public class Utils {

	/** 字段分割*/
	public static final String FIELD_SEQ = " and ";
	    
    // 关系wehre
    public static String relWhere(List<String> fields) {
    	return relWhere(null, fields);
    }
    
    public static String relWhere(String prefix, List<String> fields) {
    	return relWhere(prefix, fields, FIELD_SEQ);
    }
    
    public static String relWhere(String prefix, List<String> fields, String seq) {
		if (fields == null || fields.size() == 0)
			return "";
		StringBuffer sb = new StringBuffer(); 
		for (int i = 0; i < fields.size(); i++) {
			String tmp = StringUtils.isNotEmpty(prefix) ? prefix+"."+fields.get(i) : fields.get(i) + seq;
			sb.append(tmp);
		}
		// 去掉最后seq
		sb.delete(sb.length()-seq.length(), sb.length());
		return sb.toString();
    }
    
    private static Map<String, String> ESCAPES = new HashMap<String, String>();
    
    static {
    	// '\' -> '\\'
    	// '"' -> '""'
    	ESCAPES.put("\\", "\\\\\\\\");
    	ESCAPES.put("\"", "\"\"");
    }
    
    // 字符字段转义
	public static String strFieldEscape(String fieldString) {
		if (StringUtils.isEmpty(fieldString))
			return fieldString;
		String ed = fieldString;
		for (String key : ESCAPES.keySet()) {
			if(ed.contains(key)) {
				if("\\".equals(key))
					ed = ed.replaceAll("\\\\", ESCAPES.get(key));
				else
					ed = ed.replaceAll(key, ESCAPES.get(key));
			}
		}
		return ed;
	}

}
