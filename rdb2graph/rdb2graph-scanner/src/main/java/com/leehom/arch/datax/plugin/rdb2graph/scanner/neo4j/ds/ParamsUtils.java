package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

import com.leehom.arch.datax.plugin.rdb2graph.common.DateTimeUtils;

/**
 * @类名: ParamsUtils
 * @说明: 参数工具类
 *
 * @author   leehom
 * @Date	 2022年5月5日 下午3:48:14
 * 修改记录：
 *
 * @see 	 
 */
public class ParamsUtils {
	
	// 日期类型转换
    public static LocalDateTime dateToLocalDateTime(Date date) {
    	if(date==null)
    		return null;
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        return instant.atZone(zoneId).toLocalDateTime();
    }
    
	/** */
	public static final String FIELD_SEQ = ",";
	/** */
	public static final String FIELD_QUOTA = "'";
	
	// 参数字符串
    public static String params2String(Map<String, Object> params) {
		if (params == null || params.size() == 0)
			return "";
		StringBuffer sb = new StringBuffer("{");
		for(Map.Entry<String, Object> entry : params.entrySet()){
			String name = entry.getKey();
			Object v = entry.getValue();
			if(v==null)
				continue;
			// 数字，int；long；double，不需要引号
			if(Number.class.isAssignableFrom(v.getClass())) {
				sb.append(name).append(":").append(v.toString());
			} else if(v instanceof Date) {
				String dataStr = DateTimeUtils.DateToString((Date)v, DateTimeUtils.ISO8601PattenNoZone);
				sb.append(name).append(":").append("'").append(dataStr).append("'");
			} else { // 
				sb.append(name).append(":").append("'").append(v.toString()).append("'");
			}
			sb.append(FIELD_SEQ);
		}
		sb.delete(sb.length()-1, sb.length());
		sb.append("}");
		return sb.toString();
    }

}
